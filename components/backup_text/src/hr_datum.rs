// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tidb_query_datatype::codec::data_type::{Duration, Enum, Json, Set};
use tidb_query_datatype::codec::mysql::Time;
use tidb_query_datatype::codec::Datum;

use serde::{Deserialize, Serialize};
use tidb_query_datatype::expr::EvalContext;
use tikv_util::buffer_vec::BufferVec;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrDuration {
    #[serde(rename = "v")]
    value: String,
    #[serde(rename = "f")]
    fsp: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HrBytes {
    Utf8(String),
    Raw(Vec<u8>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrTime {
    #[serde(rename = "v")]
    value: String,
    #[serde(rename = "r")]
    raw: u64,
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
        Json::deserialize(deserializer).map(HrJson)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrEnum {
    #[serde(rename = "n")]
    name: HrBytes,
    #[serde(rename = "v")]
    value: u64,
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
    Dec(String),
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
            Datum::Dur(d) => Self::Dur(HrDuration {
                value: d.to_string(),
                fsp: d.fsp(),
            }),
            Datum::Bytes(v) => Self::Bytes(HrBytes::from(v)),
            Datum::Dec(d) => Self::Dec(d.to_string()),
            Datum::Time(t) => Self::Time(HrTime {
                value: t.to_string(),
                raw: t.0,
            }),
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

impl Into<Datum> for HrDatum {
    fn into(self) -> Datum {
        let ctx = &mut EvalContext::default();

        match self {
            HrDatum::Null => Datum::Null,
            HrDatum::I64(v) => Datum::I64(v),
            HrDatum::U64(v) => Datum::U64(v),
            HrDatum::F64(v) => Datum::F64(v),
            HrDatum::Dur(d) => Datum::Dur(Duration::parse(ctx, &d.value, d.fsp as i8).unwrap()),
            HrDatum::Bytes(b) => Datum::Bytes(b.into()),
            HrDatum::Dec(d) => Datum::Dec(d.parse().unwrap()),
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
    use super::*;
    use crate::{from_text, to_text};

    #[test]
    fn test_it_works() {
        let ctx = &mut EvalContext::default();

        let datums = [
            // bytes
            Datum::Bytes(b"hello".to_vec()),
            Datum::Bytes("你好".as_bytes().to_vec()),
            Datum::Bytes(b"\xf0\x28\x8c\x28".to_vec()),
            // decimal
            Datum::Dec("12345678987.654321".parse().unwrap()),
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

            match (datum, dec) {
                // only check u64 value for enum and set
                (Datum::Enum(e), Datum::Enum(dec_e)) => {
                    assert_eq!(e.value(), dec_e.value(), "encoded as {}", enc)
                }
                (Datum::Set(s), Datum::Set(dec_s)) => {
                    assert_eq!(s.value(), dec_s.value(), "encoded as {}", enc)
                }
                (datum, dec) => assert_eq!(datum, dec, "encoded as {}", enc),
            }
        }
    }
}
