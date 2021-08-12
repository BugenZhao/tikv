// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_datatype::codec::data_type::{Duration, Enum};
use tidb_query_datatype::codec::mysql::Time;
use tidb_query_datatype::codec::Datum;

use serde::{Deserialize, Serialize};
use tidb_query_datatype::expr::EvalContext;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrDuration {
    value: String,
    fsp: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HrBytes {
    Utf8(String),
    Raw(Vec<u8>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrTime {
    value: String,
    raw: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrEnum {
    name: HrBytes,
    value: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrSet {
    data: Vec<HrBytes>,
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
    Null,
    I64(i64),
    U64(u64),
    F64(f64),
    Dur(HrDuration),
    Bytes(HrBytes),
    Dec(String),
    Time(HrTime),
    Json(String),
    Enum(HrEnum),
    Set(HrSet),
    Min,
    Max,
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
            Datum::Json(j) => Self::Json(j.to_string()),
            Datum::Enum(e) => Self::Enum(HrEnum {
                name: HrBytes::from(e.name().to_vec()),
                value: e.value(),
            }),
            Datum::Set(_s) => todo!(),
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
            HrDatum::Json(j) => Datum::Json(j.parse().unwrap()),
            HrDatum::Enum(e) => Datum::Enum(Enum::new(e.name.into(), e.value)),
            HrDatum::Set(_s) => todo!(),
            HrDatum::Min => Datum::Min,
            HrDatum::Max => Datum::Max,
        }
    }
}

impl HrDatum {
    pub fn to_text(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn from_text(text: impl AsRef<str>) -> Self {
        serde_json::from_str(text.as_ref()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            Datum::I64(31415926535898),
            Datum::U64(31415926535898),
            // json
            Datum::Json(r#"{"name": "John"}"#.parse().unwrap()),
            Datum::Json(r#"["foo", "bar"]"#.parse().unwrap()),
            // primitive
            Datum::Max,
            Datum::Min,
            Datum::Null,
            // set
            // Datum::Set(todo!()),
            // time
            Datum::Time(Time::parse_datetime(ctx, "2021-08-12 12:34:56.789", 3, false).unwrap()),
            Datum::Time(Time::parse_date(ctx, "2021-08-12").unwrap()),
        ];

        for datum in datums {
            let enc = HrDatum::from(datum.clone()).to_text();
            println!("{}", enc);
            let dec: Datum = HrDatum::from_text(enc).into();
            dbg!(&datum, &dec);
            assert_eq!(datum, dec);
        }
    }
}
