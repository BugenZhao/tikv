// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::hr_datum::HrBytes;
use serde::{Deserialize, Serialize};
use txn_types::{TimeStamp, WriteRef, WriteType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrKvWrite {
    #[serde(rename = "k")]
    pub key: HrBytes,
    #[serde(rename = "v")]
    pub value: HrWrite,
}

impl HrKvWrite {
    pub fn to_text(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn from_text(text: impl AsRef<str>) -> Self {
        serde_json::from_str(text.as_ref()).unwrap()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrWrite {
    #[serde(rename = "t")]
    pub write_type: HrWriteType,
    #[serde(rename = "s")]
    pub start_ts: u64,
    #[serde(rename = "o")]
    pub has_overlapped_rollback: bool,
    #[serde(rename = "f")]
    pub gc_fence: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HrWriteType {
    #[serde(rename = "p")]
    Put,
    #[serde(rename = "d")]
    Delete,
    #[serde(rename = "l")]
    Lock,
    #[serde(rename = "r")]
    Rollback,
}

impl From<WriteType> for HrWriteType {
    fn from(wt: WriteType) -> HrWriteType {
        match wt {
            WriteType::Put => HrWriteType::Put,
            WriteType::Delete => HrWriteType::Delete,
            WriteType::Lock => HrWriteType::Lock,
            WriteType::Rollback => HrWriteType::Rollback,
        }
    }
}

impl Into<WriteType> for HrWriteType {
    fn into(self) -> WriteType {
        match self {
            HrWriteType::Put => WriteType::Put,
            HrWriteType::Delete => WriteType::Delete,
            HrWriteType::Lock => WriteType::Lock,
            HrWriteType::Rollback => WriteType::Rollback,
        }
    }
}

impl<'a> From<WriteRef<'a>> for HrWrite {
    fn from(wr: WriteRef<'a>) -> HrWrite {
        HrWrite {
            write_type: HrWriteType::from(wr.write_type),
            start_ts: wr.start_ts.into_inner(),
            has_overlapped_rollback: wr.has_overlapped_rollback,
            gc_fence: wr.gc_fence.map(|t| t.into_inner()),
        }
    }
}

impl<'a> Into<WriteRef<'a>> for HrWrite {
    fn into(self) -> WriteRef<'a> {
        WriteRef {
            write_type: self.write_type.into(),
            start_ts: TimeStamp::from(self.start_ts),
            short_value: None,
            has_overlapped_rollback: self.has_overlapped_rollback,
            gc_fence: self.gc_fence.map(TimeStamp::from),
        }
    }
}
