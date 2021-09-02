// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::hr_datum::HrDatum;
use collections::HashMap;
use serde::{Deserialize, Serialize};
use tidb_query_datatype::codec::Result as CodecResult;
use tidb_query_datatype::codec::{
    datum::DatumDecoder,
    row::v2::{encoder_for_test::RowEncoder, RowSlice, V1CompatibleEncoder},
    table::unflatten,
};
use tidb_query_datatype::expr::EvalContext;
use tipb::ColumnInfo;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RowV1 {
    #[serde(rename = "#")]
    pub ids: Vec<u32>,
    #[serde(rename = "d")]
    pub datums: Vec<HrDatum>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowV2 {
    #[serde(rename = "b")]
    pub is_big: bool,
    #[serde(rename = "#!")]
    pub non_null_ids: Vec<u32>,
    #[serde(rename = "#?")]
    pub null_ids: Vec<u32>,
    #[serde(rename = "d")]
    pub datums: Vec<HrDatum>,
}

impl RowV2 {
    pub fn from_bytes(
        ctx: &mut EvalContext,
        val: &[u8],
        column_id_info: &HashMap<i64, ColumnInfo>,
    ) -> CodecResult<RowV2> {
        let row = RowSlice::from_bytes(val)?;
        let mut non_null_ids = Vec::with_capacity(row.values_num());
        let mut hr_datums = Vec::with_capacity(column_id_info.len());
        for id in row.non_null_ids() {
            if let Some(ci) = column_id_info.get(&(id as i64)) {
                let (start, offset) = row.search_in_non_null_ids(id as i64)?.unwrap();
                let raw_datum = {
                    // encode with V1CompatibleEncoder and decode as v1 datum
                    let mut buf = vec![];
                    buf.write_v2_as_datum(&row.values()[start..offset], ci)?;
                    (&mut buf.as_slice()).read_datum()?
                };
                let datum = unflatten(ctx, raw_datum, ci)?;
                non_null_ids.push(id);
                hr_datums.push(HrDatum::from(datum));
            }
        }
        Ok(RowV2 {
            is_big: row.is_big(),
            non_null_ids,
            null_ids: row.null_ids(),
            datums: hr_datums,
        })
    }

    pub fn into_bytes(self, ctx: &mut EvalContext) -> CodecResult<Vec<u8>> {
        let RowV2 {
            is_big,
            non_null_ids,
            null_ids,
            datums,
        } = self;
        let datums = datums.into_iter().map(|d| d.into()).collect();
        let mut buf = vec![];
        buf.write_row_with_datum(ctx, is_big, non_null_ids, null_ids, datums)?;
        Ok(buf)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "V")]
pub enum HrValue {
    #[serde(rename = "1")]
    V1(RowV1),
    #[serde(rename = "2")]
    V2(RowV2),
}
