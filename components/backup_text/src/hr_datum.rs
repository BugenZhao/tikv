// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::eval_context;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tidb_query_datatype::codec::data_type::{Decimal, Duration, Enum, Json, Set};
use tidb_query_datatype::codec::mysql::Time;
use tidb_query_datatype::codec::Datum;
use tidb_query_datatype::{FieldTypeAccessor, FieldTypeTp};
use tikv_util::buffer_vec::BufferVec;
use tikv_util::escape;

pub fn write_bytes_to(tp: &dyn FieldTypeAccessor, buf: &mut Vec<u8>, d: &Datum) {
    let s = match d {
        Datum::I64(i) => format!("{}", i),
        Datum::U64(u) => format!("{}", u),
        Datum::F64(f) => format!("{}", f), // TODO: double to 1.xxx`e`xxx format
        Datum::Time(t) => format!("\"{}\"", t), // TODO: time zone
        Datum::Dur(ref d) => format!("\"{}\"", d),
        Datum::Dec(ref d) => format!("{}", d),
        Datum::Json(ref d) => format!("\"{}\"", escape(d.to_string().as_bytes())),
        Datum::Enum(ref e) => format!("\"{}\"", e.to_string()),
        Datum::Set(ref s) => format!("\"{}\"", s.to_string()),
        Datum::Null => "\\N".to_owned(),
        Datum::Max => "MAX".to_owned(),
        Datum::Min => "MIN".to_owned(),
        Datum::Bytes(ref bs) => match tp.tp() {
            FieldTypeTp::VarString | FieldTypeTp::String | FieldTypeTp::VarChar => {
                format!("\"{}\"", escape(bs))
            }
            _ => {
                buf.extend("\"".as_bytes());
                buf.extend(bs);
                buf.extend("\"".as_bytes());
                return;
            }
        },
    };
    buf.extend(s.as_bytes());
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrDuration {
    #[serde(rename = "v")]
    value: String,
    #[serde(rename = "f")]
    fsp: u8,
}

impl HrDuration {
    pub fn from(d: Duration) -> HrDuration {
        HrDuration {
            value: d.to_string(),
            fsp: d.fsp(),
        }
    }
    pub fn to_duration(&self) -> Duration {
        Duration::parse(&mut eval_context(), &self.value, self.fsp as i8).unwrap()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HrBytes {
    Utf8(String),
    Raw(Vec<u8>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrDecimal {
    #[serde(rename = "v")]
    value: String,
    #[serde(rename = "p")]
    prec: u8,
}

impl From<Decimal> for HrDecimal {
    fn from(d: Decimal) -> Self {
        let (prec, _) = d.preferred_prec_and_frac();
        let value = d.to_string();
        Self { value, prec }
    }
}

impl Into<Decimal> for HrDecimal {
    fn into(self) -> Decimal {
        let mut d = self.value.parse::<Decimal>().unwrap();
        d.set_preferred_prec(Some(self.prec));
        d
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrTime {
    #[serde(rename = "v")]
    value: String,
    #[serde(rename = "r")]
    raw: u64,
}

impl HrTime {
    pub fn from(t: Time) -> HrTime {
        HrTime {
            value: t.to_string(),
            raw: t.0,
        }
    }
    pub fn to_time(&self) -> Time {
        Time(self.raw)
    }
}

#[derive(Debug, Clone)]
pub struct HrJson(pub Json);

impl Serialize for HrJson {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.as_ref().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for HrJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // todo: `serde_json` has a recursion depth limit of 128 on deserializing and it's not safe to turn it off.
        // while tidb allows depth up to 10000, this might be problematic on deeply nested json documents.
        Json::deserialize(deserializer).map(HrJson)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrEnum {
    #[serde(rename = "n")]
    pub name: HrBytes,
    #[serde(rename = "v")]
    pub value: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrSet {
    #[serde(rename = "d")]
    data: HrBytes,
    #[serde(rename = "v")]
    value: u64,
}

impl From<Vec<u8>> for HrBytes {
    fn from(bytes: Vec<u8>) -> Self {
        String::from_utf8(bytes)
            .map(HrBytes::Utf8)
            .unwrap_or_else(|e| HrBytes::Raw(e.into_bytes()))
    }
}

impl Into<Vec<u8>> for HrBytes {
    fn into(self) -> Vec<u8> {
        match self {
            HrBytes::Utf8(s) => s.into_bytes(),
            HrBytes::Raw(b) => b,
        }
    }
}

/// A human-readable `Datum`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HrDatum {
    #[serde(rename = "n")]
    Null,
    #[serde(rename = "i")]
    I64(i64),
    #[serde(rename = "u")]
    U64(u64),
    #[serde(rename = "f")]
    F64(f64),
    #[serde(rename = "D")]
    Dur(HrDuration),
    #[serde(rename = "b")]
    Bytes(HrBytes),
    #[serde(rename = "d")]
    Dec(HrDecimal),
    #[serde(rename = "t")]
    Time(HrTime),
    #[serde(rename = "j")]
    Json(HrJson),
    #[serde(rename = "e")]
    Enum(HrEnum),
    #[serde(rename = "s")]
    Set(HrSet),
    #[serde(rename = "m")]
    Min,
    #[serde(rename = "M")]
    Max,
}

lazy_static::lazy_static! {
    static ref NULL_BUFFER_VEC: Arc<BufferVec> = Arc::new(BufferVec::new());
}

impl From<Datum> for HrDatum {
    fn from(d: Datum) -> Self {
        match d {
            Datum::Null => Self::Null,
            Datum::I64(v) => Self::I64(v),
            Datum::U64(v) => Self::U64(v),
            Datum::F64(v) => Self::F64(v),
            Datum::Dur(d) => Self::Dur(HrDuration::from(d)),
            Datum::Bytes(v) => Self::Bytes(HrBytes::from(v)),
            Datum::Dec(d) => Self::Dec(d.into()),
            Datum::Time(t) => Self::Time(HrTime::from(t)),
            Datum::Json(j) => Self::Json(HrJson(j)),
            Datum::Enum(e) => Self::Enum(HrEnum {
                name: HrBytes::from(e.name().to_vec()),
                value: e.value(),
            }),
            Datum::Set(s) => Self::Set(HrSet {
                data: HrBytes::Utf8(s.as_ref().to_string()),
                value: s.value(),
            }),
            Datum::Min => Self::Min,
            Datum::Max => Self::Max,
        }
    }
}

impl HrDatum {
    pub fn get_int_handle(self) -> i64 {
        match self {
            HrDatum::I64(i) => i,
            _ => panic!("not an i64"),
        }
    }
}

impl Into<Datum> for HrDatum {
    fn into(self) -> Datum {
        let ctx = &mut eval_context();

        match self {
            HrDatum::Null => Datum::Null,
            HrDatum::I64(v) => Datum::I64(v),
            HrDatum::U64(v) => Datum::U64(v),
            HrDatum::F64(v) => Datum::F64(v),
            HrDatum::Dur(d) => Datum::Dur(Duration::parse(ctx, &d.value, d.fsp as i8).unwrap()),
            HrDatum::Bytes(b) => Datum::Bytes(b.into()),
            HrDatum::Dec(d) => Datum::Dec(d.into()),
            HrDatum::Time(t) => Datum::Time(Time(t.raw)),
            HrDatum::Json(j) => Datum::Json(j.0),
            HrDatum::Enum(e) => Datum::Enum(Enum::new(e.name.into(), e.value)),
            HrDatum::Set(s) => Datum::Set(Set::new(
                // todo: this is currently ok since we always flatten it before encoding, where the `data` field is ignored
                NULL_BUFFER_VEC.clone(),
                s.value,
            )),
            HrDatum::Min => Datum::Min,
            HrDatum::Max => Datum::Max,
        }
    }
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::codec::table::flatten;

    use super::*;
    use crate::{from_text, to_text};

    #[test]
    fn test_decimal_different_prec_frac() {
        use tidb_query_datatype::codec::data_type::Decimal;
        use tidb_query_datatype::codec::datum::DECIMAL_FLAG;
        use tidb_query_datatype::codec::datum::{DatumDecoder, DatumEncoder};
        use tidb_query_datatype::codec::mysql::DecimalEncoder;

        let ctx = &mut eval_context();

        let dec = "1.7702".parse::<Decimal>().unwrap();
        let raws = (6..=13)
            .map(|prec| {
                let mut buf = vec![DECIMAL_FLAG];
                buf.write_decimal(&dec, prec, 4).unwrap();
                buf
            })
            .collect::<Vec<_>>();

        for raw in raws {
            let datum = raw.as_slice().read_datum().unwrap();
            match &datum {
                Datum::Dec(dec) => {
                    println!("{:?}, {}", dec, dec);
                }
                _ => unreachable!(),
            }

            let hr = HrDatum::from(datum);
            let dec_datum = hr.into();
            let mut enc = vec![];
            enc.write_datum(ctx, &[dec_datum], true).unwrap();
            assert_eq!(raw, enc);
        }
    }

    #[test]
    fn test_datum() {
        let ctx = &mut eval_context();

        let datums = [
            // bytes
            Datum::Bytes(b"hello".to_vec()),
            Datum::Bytes("你好".as_bytes().to_vec()),
            Datum::Bytes(b"\xf0\x28\x8c\x28".to_vec()),
            // decimal
            Datum::Dec("12345678987.654321".parse().unwrap()),
            Datum::Dec("123.000".parse().unwrap()),
            Datum::Dec("123.0".parse().unwrap()),
            Datum::Dec("123".parse().unwrap()),
            // duration
            Datum::Dur(Duration::parse(ctx, "1 23:34:45.6789", 4).unwrap()),
            Datum::Dur(Duration::parse(ctx, "1 23:34:45", 0).unwrap()),
            // enum
            Datum::Enum(Enum::new(b"Foo".to_vec(), 1)),
            // numbers
            Datum::F64(3.1415926),
            Datum::F64(9.8596765437597708567e-305),
            Datum::F64(1.0142320547350045095e304),
            Datum::I64(31415926535898),
            Datum::U64(31415926535898),
            // json
            Datum::Json(r#"{"name": "John"}"#.parse().unwrap()),
            Datum::Json(r#"["foo", "bar"]"#.parse().unwrap()),
            Datum::Json(r#""foo""#.parse().unwrap()),
            Datum::Json(r#"0.1010101010101010101010101010101010101010"#.parse().unwrap()),
            Datum::Json(r#"18446744073709551615"#.parse().unwrap()),
            Datum::Json(Json::from_u64(18446744073709551615u64).unwrap()),
            // primitive
            Datum::Max,
            Datum::Min,
            Datum::Null,
            // enum
            Datum::Enum(Enum::parse_value(2, &["bug".to_owned(), "gen".to_owned()])),
            // set
            Datum::Set(Set::parse_value(
                0b11,
                &["bug".to_owned(), "gen".to_owned()],
            )),
            // time
            Datum::Time(Time::parse_datetime(ctx, "2021-08-12 12:34:56.789", 3, false).unwrap()),
            Datum::Time(Time::parse_date(ctx, "2021-08-12").unwrap()),
        ];

        for datum in datums {
            let enc = to_text(HrDatum::from(datum.clone()));
            println!("{}", enc);
            let dec: Datum = from_text::<HrDatum>(&enc).into();

            match (&datum, &dec) {
                // only check u64 value for enum and set
                (Datum::Enum(e), Datum::Enum(dec_e)) => {
                    assert_eq!(e.value(), dec_e.value(), "encoded as {}", enc)
                }
                (Datum::Set(s), Datum::Set(dec_s)) => {
                    assert_eq!(s.value(), dec_s.value(), "encoded as {}", enc)
                }
                (datum, dec) => assert_eq!(datum, dec, "encoded as {}", enc),
            }

            use tidb_query_datatype::codec::datum::DatumEncoder;

            let mut v_datum = vec![];
            let flatten_datum = flatten(ctx, datum).unwrap();
            v_datum.write_datum(ctx, &[flatten_datum], true).unwrap();

            let mut v_dec = vec![];
            let flatten_dec = flatten(ctx, dec).unwrap();
            v_dec.write_datum(ctx, &[flatten_dec], true).unwrap();
            assert_eq!(v_datum, v_dec);
        }
    }
}
