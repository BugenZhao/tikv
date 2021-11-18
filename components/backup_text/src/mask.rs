// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::{TryFrom, TryInto};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tidb_query_datatype::codec::mysql::MAX_FSP;
use tidb_query_datatype::codec::{
    convert::ConvertTo,
    data_type::{DateTime, Decimal, Duration, Enum, Set},
    mysql::{last_day_of_month, TimeType},
    Datum,
};
use tidb_query_datatype::expr::EvalContext;
use tikv_util::buffer_vec::BufferVec;

use crate::eval_context;
use crate::hr_datum::*;

const DEFAULT_CONTEXT: &'static str = "tidb";

#[inline]
fn new_hasher() -> blake3::Hasher {
    blake3::Hasher::new_derive_key(DEFAULT_CONTEXT)
}

fn hash_bytes(bytes: &[u8], size: usize) -> Vec<u8> {
    let mut hasher = new_hasher();
    hasher.update(bytes);
    let mut reader = hasher.finalize_xof();

    let mut buf = vec![0; size];
    reader.fill(&mut buf);
    buf
}

const I24_MIN: i64 = -(1 << 24 - 1);
const I24_MAX: i64 = (1 << 24 - 1) - 1;
const U24_MIN: u64 = 0;
const U24_MAX: u64 = (1 << 24) - 1;

#[inline]
fn mask_i64(from: i64) -> i64 {
    let to = i64::from_le_bytes(hash_bytes(&mut from.to_le_bytes(), 8).try_into().unwrap());

    if let Ok(_) = i8::try_from(from) {
        to % (i8::MAX as i64 + 1)
    } else if let Ok(_) = i16::try_from(from) {
        to % (i16::MAX as i64 + 1)
    } else if let I24_MIN..=I24_MAX = from {
        to % (I24_MAX as i64 + 1)
    } else if let Ok(_) = i32::try_from(from) {
        to % (i32::MAX as i64 + 1)
    } else {
        to
    }
}

#[inline]
fn mask_u64(from: u64) -> u64 {
    let to = u64::from_le_bytes(hash_bytes(&mut from.to_le_bytes(), 8).try_into().unwrap());

    if let Ok(_) = u8::try_from(from) {
        to % (u8::MAX as u64 + 1)
    } else if let Ok(_) = u16::try_from(from) {
        to % (u16::MAX as u64 + 1)
    } else if let U24_MIN..=U24_MAX = from {
        to % (U24_MAX as u64 + 1)
    } else if let Ok(_) = u32::try_from(from) {
        to % (u32::MAX as u64 + 1)
    } else {
        to
    }
}

fn mask_string(bytes: &[u8]) -> Vec<u8> {
    let mut size = bytes.len();
    // bytes in hex format is always twice the length as the original
    if size < 2 {
        size = 2;
    }
    let sum = hash_bytes(bytes, size / 2);
    let mut hex = hex::encode(sum);
    hex.push_str(&"*".repeat(size - hex.len())); // 0 or 1 '*'
    hex.into_bytes()
}

fn mask_duration(dur: Duration) -> Result<Duration> {
    // hack: 3e15 is slightly smaller than the max duration (838:59:59) * 10^9 nanosecs
    let nanos = mask_i64(dur.to_nanos()) % 3_000_000_000_000_000;
    let dur = Duration::new(nanos, MAX_FSP as u8).round_frac(dur.fsp() as i8)?;
    Ok(dur)
}

fn mask_time(ctx: &mut EvalContext, time: DateTime) -> Result<DateTime> {
    let (fsp, time_type) = (time.fsp(), time.get_time_type());

    // The least significant 4 bits are used to store `fsp`,
    // we ignore them here to get the `core_time` which keeps consistent with TiDB
    let core_time = time.0 & (!0b1111);
    // Get a masked but unchecked time
    let unchecked_time = DateTime(mask_u64(core_time));

    // Adjust time to a valid range
    let year = match time_type {
        TimeType::Date | TimeType::DateTime => unchecked_time.year() % 10000, // 0..9999
        // Hack! the timestampe is the number of non-leap seconds since January 1, 1970 0:00:00 UTC (aka "UNIX timestamp").
        // the valid range is 0..(1 << 31) - 1, 2035 is an approximately upper bound
        TimeType::Timestamp => unchecked_time.year() % (2036 - 1970) + 1970, // 1970..2035
    };
    let month = (unchecked_time.month() % 12) + 1; // 1..12
    let day = (unchecked_time.day() % last_day_of_month(year, month)) + 1; // 1..28/29/30/31
    let hour = unchecked_time.hour() % 24; // 0..23
    let minute = unchecked_time.minute() % 60; // 0..59
    let second = unchecked_time.second() % 60; // 0..59
    let micro = unchecked_time.micro() % 1000000; // 0..999999

    let time = DateTime::from_slice(
        ctx,
        &[year, month, day, hour, minute, second, micro][..],
        time_type,
        fsp,
    )
    .ok_or_else(|| anyhow!("failed to mask time"))?;

    Ok(time)
}

fn format_float(mut f: f64, neg: bool, mut int_num: usize, frac: usize) -> String {
    f = f.abs();
    int_num = int_num.max(1);

    let mut s: String = format!("{:.10e}", f)
        .chars()
        .filter(|c| c.is_numeric())
        .collect();

    if s.len() < int_num {
        s.push_str(&"0".repeat(int_num - s.len()));
    }

    let left = &s[..int_num];
    let right = &s[int_num..s.len().min(int_num + frac)];

    let mut res = left.to_owned();
    if !right.is_empty() {
        res.push_str(".");
        res.push_str(right);
    }
    if neg {
        res = format!("-{}", res);
    }
    res
}

#[inline]
fn mask_f64_raw(f: f64) -> Result<f64> {
    let bytes = hash_bytes(&mut f.to_le_bytes(), 8).try_into().unwrap();
    let mut masked_f64 = f64::from_le_bytes(bytes);
    if masked_f64.is_infinite() || masked_f64.is_nan() {
        masked_f64 = u64::from_le_bytes(bytes) as f64;
    }
    Ok(masked_f64)
}

fn mask_f64(mut f: f64) -> Result<f64> {
    let neg = f.is_sign_negative();
    f = f.abs();

    let s = f.to_string();
    let tokens: Vec<_> = s.split('.').collect();
    let (interger_part, fract_part) = match tokens.len() {
        0 => unreachable!(),
        1 => (tokens[0].chars().count(), 0),
        _ => (tokens[0].chars().count(), tokens[1].chars().count()),
    };
    let masked_f64 = mask_f64_raw(f)?;
    let res = format_float(masked_f64, neg, interger_part, fract_part);
    let f = f64::from_str(&res)?;

    Ok(f)
}

fn mask_decimal(ctx: &mut EvalContext, d: Decimal) -> Result<Decimal> {
    let (prec, frac) = d.prec_and_frac();
    let neg = d.is_negative();
    let f: f64 = d.convert(ctx)?;
    let masked_f64 = mask_f64_raw(f.abs())?;
    let res = format_float(masked_f64, neg, (prec - frac) as usize, frac as usize);
    let d = Decimal::from_str(&res)?;

    Ok(d)
}

pub fn workload_sim_mask(mut datum: Datum) -> Datum {
    let ctx = &mut eval_context();
    match &mut datum {
        Datum::Null => {}
        Datum::Min => {}
        Datum::Max => {}
        Datum::I64(i) => *i = mask_i64(*i),
        Datum::U64(u) => *u = mask_u64(*u),
        Datum::F64(f) => *f = mask_f64(*f).unwrap_or(*f),
        Datum::Bytes(bytes) => *bytes = mask_string(bytes),
        Datum::Enum(e) => {
            let masked_name = mask_string(e.name());
            *e = Enum::new(masked_name, e.value())
        }
        Datum::Dur(dur) => *dur = mask_duration(*dur).unwrap_or(*dur),
        Datum::Time(time) => *time = mask_time(ctx, *time).unwrap_or(*time),
        Datum::Set(s) => {
            let (data, value) = (s.data(), s.value());
            let mut masked_data = BufferVec::with_capacity(data.capacity(), data.data_capacity());
            for e in data.iter() {
                masked_data.push(mask_string(e));
            }
            *s = Set::new(Arc::new(masked_data), value);
        }
        Datum::Dec(d) => *d = mask_decimal(ctx, *d).unwrap_or(*d),
        Datum::Json(_) => {
            // todo: not supported yet
        }
    }
    datum
}

pub fn mask_hr_bytes(b: &mut HrBytes) {
    match b {
        HrBytes::Utf8(s) => {
            // Safety depend on `mask_string` return a slice of bytes of valid utf8 string
            *s = unsafe { String::from_utf8_unchecked(mask_string(s.as_bytes())) };
        }
        HrBytes::Raw(r) => *r = mask_string(&r),
    }
}

pub fn mask_hr_datum(hr_datum: &mut HrDatum) {
    let ctx = &mut eval_context();
    match hr_datum {
        HrDatum::Null | HrDatum::Max | HrDatum::Min => {}
        HrDatum::I64(i) => *i = mask_i64(*i),
        HrDatum::U64(u) => *u = mask_u64(*u),
        HrDatum::F64(f) => *f = mask_f64(*f).unwrap_or(*f),
        HrDatum::Bytes(b) => mask_hr_bytes(b),
        HrDatum::Enum(e) => mask_hr_bytes(&mut e.name),
        HrDatum::Set(set) => {
            match set.data {
                HrBytes::Utf8(ref mut s) => {
                    let mut res = String::new();
                    for e in s.split(',') {
                        if !res.is_empty() {
                            res.push(',');
                        }
                        // Safety depend on `mask_string` return a slice of bytes of valid utf8 string
                        let masked_e =
                            unsafe { String::from_utf8_unchecked(mask_string(e.as_bytes())) };
                        res.push_str(masked_e.as_str());
                    }
                    *s = res;
                }
                _ => unreachable!(),
            }
        }
        // TODO: support directly mask `HrDatum::Dur`, `HrDatum::Time` and `HrDatum::Dec`
        HrDatum::Dur(d) => {
            *d = {
                let raw_d = d.to_duration(ctx);
                HrDuration::from(mask_duration(raw_d).unwrap_or(raw_d))
            }
        }
        HrDatum::Time(t) => {
            *t = {
                let raw_t = t.to_time();
                HrTime::from(mask_time(ctx, raw_t).unwrap_or(raw_t))
            }
        }
        HrDatum::Dec(d) => {
            *d = {
                let raw_d = d.to_decimal();
                HrDecimal::from(mask_decimal(ctx, raw_d).unwrap_or(raw_d))
            }
        }
        // TODO: not supported yet
        HrDatum::Json(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workload_sim_mask() {
        let ctx = &mut eval_context();
        let datum_pairs = [
            (Datum::I64(42), Datum::I64(-113)),                           // 8
            (Datum::I64(4200), Datum::I64(32405)),                        // 16
            (Datum::I64(420000), Datum::I64(-5612912)),                   // 24
            (Datum::I64(42000000), Datum::I64(-2040048546)),              // 32
            (Datum::I64(420000000000), Datum::I64(-1884590655846725089)), // 64
            (Datum::U64(42), Datum::U64(15)),                             // 8
            (Datum::U64(4200), Datum::U64(65173)),                        // 16
            (Datum::U64(420000), Datum::U64(2775696)),                    // 24
            (Datum::U64(42000000), Datum::U64(107435102)),                // 32
            (Datum::U64(420000000000), Datum::U64(16562153417862826527)), // 64
            (Datum::F64(42.42), Datum::F64(78.18)),
            (Datum::F64(-42.42), Datum::F64(-78.18)),
            (Datum::F64(4242.), Datum::F64(1188.)),
            (Datum::F64(0.4242), Datum::F64(1.0003)),
            (Datum::F64(4.2e10), Datum::F64(3.0508343005e+10)),
            (Datum::F64(4.2e20), Datum::F64(1.5036856769116e+20)),
            (Datum::F64(4.2e-10), Datum::F64(2.54857468602)),
            (Datum::F64(4.2e-20), Datum::F64(9.054866296551)),
            (
                Datum::Dec("42.42".parse().unwrap()),
                Datum::Dec("78.18".parse().unwrap()),
            ),
            (
                Datum::Dec("-42.42".parse().unwrap()),
                Datum::Dec("-78.18".parse().unwrap()),
            ),
            (
                Datum::Dec("4242".parse().unwrap()),
                Datum::Dec("1188".parse().unwrap()),
            ),
            (
                Datum::Dec("0.4242".parse().unwrap()),
                Datum::Dec("1.0003".parse().unwrap()),
            ),
            (
                Datum::Bytes("你好".as_bytes().to_vec()),
                Datum::Bytes(b"ba3468".to_vec()),
            ),
            (
                Datum::Bytes(b"\x01\x02".to_vec()),
                Datum::Bytes(b"0a".to_vec()),
            ),
            (
                Datum::Enum(Enum::new(b"male".to_vec(), 1)),
                Datum::Enum(Enum::new(b"d85e".to_vec(), 1)),
            ),
            (
                Datum::Dur(Duration::parse(ctx, "10:11:12.1314", 4).unwrap()),
                Datum::Dur(Duration::parse(ctx, "307:05:33.1856", 4).unwrap()),
            ),
            (
                Datum::Time(DateTime::parse_date(ctx, "2021-10-19").unwrap()),
                Datum::Time(DateTime::parse_date(ctx, "1706-05-07").unwrap()),
            ),
            (
                Datum::Time(
                    DateTime::parse_datetime(ctx, "2021-10-19 12:34:56.7890", 4, false).unwrap(),
                ),
                Datum::Time(
                    DateTime::parse_datetime(ctx, "0939-03-21 14:01:42.5772", 4, false).unwrap(),
                ),
            ),
            (
                Datum::Time(
                    DateTime::parse_timestamp(ctx, "2021-10-19 12:34:56.7890", 4, false).unwrap(),
                ),
                Datum::Time(
                    DateTime::parse_timestamp(ctx, "2019-03-21 14:01:42.5772", 4, false).unwrap(),
                ),
            ),
        ];

        for (from, expected) in datum_pairs {
            let to = workload_sim_mask(from);
            println!("{:?}", to);
            assert_eq!(expected.to_string().unwrap(), to.to_string().unwrap()); // fixme: cannot expect the bit representation to be exactly same now
        }
    }
}
