// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::{TryFrom, TryInto};

use tidb_query_datatype::codec::{
    data_type::{DateTime, Duration, Enum},
    Datum,
};
use tikv_util::warn;

use crate::eval_context;

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

#[inline]
fn mask_f64(f: f64) -> f64 {
    f64::from_le_bytes(hash_bytes(&mut f.to_le_bytes(), 8).try_into().unwrap())
}

fn mask_string(bytes: &[u8]) -> Vec<u8> {
    let size = bytes.len();
    // bytes in hex format is always twice the length as the original
    let sum = hash_bytes(bytes, size / 2);
    let mut hex = hex::encode(sum);
    hex.push_str(&"*".repeat(size - hex.len())); // 0 or 1 '*'
    hex.into_bytes()
}

fn mask_duration(dur: Duration) -> Duration {
    // todo: a trivial mask function
    let secs = dur.to_secs() / 3600 * 3600;
    let fsp = dur.fsp() as i8;
    Duration::from_secs(secs, fsp).unwrap()
}

fn mask_time(time: DateTime) -> DateTime {
    // todo: a trivial mask function
    let ctx = &mut eval_context();

    let year = time.year();
    let month = time.month();
    let day = time.day();
    let fsp = time.fsp() as i8;
    let time_type = time.get_time_type();

    DateTime::parse(
        ctx,
        &format!("{}-{}-{}", year, month, day),
        time_type,
        fsp,
        true,
    )
    .unwrap()
}

pub fn workload_sim_mask(mut datum: Datum) -> Datum {
    match &mut datum {
        Datum::Null => {}
        Datum::Min => {}
        Datum::Max => {}
        Datum::I64(i) => *i = mask_i64(*i),
        Datum::U64(u) => *u = mask_u64(*u),
        Datum::F64(f) => *f = mask_f64(*f),
        Datum::Bytes(bytes) => *bytes = mask_string(bytes).to_vec(),
        Datum::Enum(e) => {
            let masked_name = mask_string(e.name()).to_vec();
            *e = Enum::new(masked_name, e.value())
        }
        Datum::Dur(dur) => *dur = mask_duration(*dur),
        Datum::Time(time) => *time = mask_time(*time),

        Datum::Dec(_) | Datum::Json(_) | Datum::Set(_) => {
            // todo: not supported yet
            warn!("mask not supported yet"; "datum" => ?datum);
        }
    }

    datum
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
            (Datum::F64(42.42), Datum::F64(7818787403329284e135)),
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
                Datum::Dur(Duration::parse(ctx, "10:00:00.0000", 4).unwrap()),
            ),
            (
                Datum::Time(DateTime::parse_date(ctx, "2021-10-19").unwrap()),
                Datum::Time(DateTime::parse_date(ctx, "2021-10-19").unwrap()),
            ),
            (
                Datum::Time(
                    DateTime::parse_datetime(ctx, "2021-10-19 12:34:56.7890", 4, false).unwrap(),
                ),
                Datum::Time(
                    DateTime::parse_datetime(ctx, "2021-10-19 00:00:00.0000", 4, false).unwrap(),
                ),
            ),
            (
                Datum::Time(
                    DateTime::parse_timestamp(ctx, "2021-10-19 12:34:56.7890", 4, false).unwrap(),
                ),
                Datum::Time(
                    DateTime::parse_timestamp(ctx, "2021-10-19 00:00:00.0000", 4, false).unwrap(),
                ),
            ),
        ];

        for (from, expected) in datum_pairs {
            let to = workload_sim_mask(from);
            println!("{:?}", to);
            assert_eq!(to, expected);
        }
    }
}
