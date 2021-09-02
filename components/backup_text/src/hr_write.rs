// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::hr_index::{HrIndex, HrIndexValue};
use crate::hr_key::HrDataKey;
use crate::{hr_index::HrIndexKey, hr_value::HrValue};
use collections::HashMap;
use serde::{Deserialize, Serialize};
use tidb_query_datatype::expr::EvalContext;
use tipb::ColumnInfo;
use txn_types::{TimeStamp, Write, WriteRef, WriteType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrKvWrite {
    #[serde(rename = "k")]
    pub key: HrDataKey,
    #[serde(rename = "v")]
    pub value: HrWrite,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrIndexKvWrite {
    #[serde(rename = "k")]
    pub key: HrIndexKey,
    #[serde(rename = "v")]
    pub value: HrWrite,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "DT")]
pub enum HrShortValue {
    #[serde(rename = "i")]
    Index(HrIndexValue),
    #[serde(rename = "r")]
    Record(HrValue),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrWrite {
    #[serde(rename = "t", with = "HrWriteType")]
    pub write_type: WriteType,
    #[serde(rename = "s")]
    pub start_ts: u64,
    #[serde(rename = "o")]
    pub has_overlapped_rollback: bool,
    #[serde(rename = "f", skip_serializing_if = "Option::is_none", default)]
    pub gc_fence: Option<u64>,
    #[serde(rename = "v", skip_serializing_if = "Option::is_none", default)]
    pub value: Option<HrShortValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(remote = "WriteType")]
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

impl HrWrite {
    #[inline]
    pub fn from_ref_index(
        ctx: &mut EvalContext,
        wr: WriteRef<'_>,
        columns: &HashMap<i64, ColumnInfo>,
    ) -> HrWrite {
        Self::from_ref(ctx, wr, columns, true)
    }

    #[inline]
    pub fn from_ref_data(
        ctx: &mut EvalContext,
        wr: WriteRef<'_>,
        columns: &HashMap<i64, ColumnInfo>,
    ) -> HrWrite {
        Self::from_ref(ctx, wr, columns, false)
    }

    fn from_ref(
        ctx: &mut EvalContext,
        wr: WriteRef<'_>,
        columns: &HashMap<i64, ColumnInfo>,
        is_index: bool,
    ) -> HrWrite {
        let value = wr.short_value.map(|val| {
            if is_index {
                let value = HrIndex::decode_index_value(val, ctx, columns).unwrap();
                HrShortValue::Index(value)
            } else {
                let value = HrValue::from_bytes(ctx, val, columns)
                    .map_err(|e| {
                        println!("{:?}", val);
                        e
                    })
                    .unwrap();
                HrShortValue::Record(value)
            }
        });
        HrWrite {
            write_type: wr.write_type,
            start_ts: wr.start_ts.into_inner(),
            has_overlapped_rollback: wr.has_overlapped_rollback,
            gc_fence: wr.gc_fence.map(|t| t.into_inner()),
            value,
        }
    }
}

impl HrWrite {
    pub fn into_bytes(self, ctx: &mut EvalContext) -> Vec<u8> {
        let short_value = self.value.map(|val| match val {
            HrShortValue::Index(val) => HrIndex::encode_index_value(ctx, val).unwrap(),
            HrShortValue::Record(val) => val.into_bytes(ctx).unwrap(),
        });
        let write = Write {
            write_type: self.write_type,
            start_ts: TimeStamp::from(self.start_ts),
            short_value,
            has_overlapped_rollback: self.has_overlapped_rollback,
            gc_fence: self.gc_fence.map(TimeStamp::from),
        };
        write.as_ref().to_bytes()
    }
}
