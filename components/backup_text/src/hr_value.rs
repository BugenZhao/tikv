// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::hr_datum::HrDatum;
use collections::HashMap;
use serde::{Deserialize, Serialize};
use tidb_query_datatype::codec::{table, Result as CodecResult};
use tidb_query_datatype::expr::EvalContext;
use tidb_query_datatype::{
    codec::{
        datum::{Datum, DatumDecoder},
        row::{
            self,
            v2::{encoder_for_test::RowEncoder, RowSlice, V1CompatibleEncoder},
        },
        table::unflatten,
    },
    FieldTypeAccessor,
};
use tipb::ColumnInfo;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RowV1 {
    #[serde(rename = "#")]
    pub ids: Vec<i64>,
    #[serde(rename = "d")]
    pub datums: Vec<HrDatum>,
}

impl RowV1 {
    pub fn from_bytes(
        ctx: &mut EvalContext,
        mut val: &[u8],
        columns: &HashMap<i64, ColumnInfo>,
    ) -> CodecResult<RowV1> {
        let (ids, datums) = table::decode_row_vec(&mut val, ctx, columns)?;
        let datums = datums
            .into_iter()
            .zip(ids.iter())
            .map(|(d, id)| {
                HrDatum::with_workload_sim_mask(
                    d,
                    columns.get(id).map(|ci| ci as &dyn FieldTypeAccessor),
                )
            })
            .collect();
        Ok(RowV1 { ids, datums })
    }

    pub fn into_bytes(self, ctx: &mut EvalContext) -> CodecResult<Vec<u8>> {
        let RowV1 { ids, datums } = self;
        let datums = datums.into_iter().map(|d| d.into()).collect();
        let value = table::encode_row(ctx, datums, &ids).unwrap();
        Ok(value)
    }

    pub fn to_datums(
        ctx: &mut EvalContext,
        columns: &HashMap<i64, ColumnInfo>,
        mut val: &[u8],
    ) -> CodecResult<HashMap<i64, Datum>> {
        Ok(table::decode_row(&mut val, ctx, columns)?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowV2 {
    #[serde(rename = "b", skip_serializing_if = "crate::is_false", default)]
    pub is_big: bool,
    #[serde(rename = "#!")]
    pub non_null_ids: Vec<u32>,
    #[serde(rename = "#?", skip_serializing_if = "Vec::is_empty", default)]
    pub null_ids: Vec<u32>,
    #[serde(rename = "d")]
    pub datums: Vec<HrDatum>,
}

impl RowV2 {
    pub fn from_bytes(
        ctx: &mut EvalContext,
        val: &[u8],
        columns: &HashMap<i64, ColumnInfo>,
    ) -> CodecResult<RowV2> {
        let row = RowSlice::from_bytes(val)?;
        let mut non_null_ids = Vec::with_capacity(row.values_num());
        let mut hr_datums = Vec::with_capacity(columns.len());
        for id in row.non_null_ids() {
            if let Some(ci) = columns.get(&(id as i64)) {
                let (start, offset) = row.search_in_non_null_ids(id as i64)?.unwrap();
                let raw_datum = {
                    // encode with V1CompatibleEncoder and decode as v1 datum
                    let mut buf = vec![];
                    buf.write_v2_as_datum(&row.values()[start..offset], ci)?;
                    (&mut buf.as_slice()).read_datum()?
                };
                let datum = unflatten(ctx, raw_datum, ci)?;
                non_null_ids.push(id);
                hr_datums.push(HrDatum::with_workload_sim_mask(datum, Some(ci)));
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

    pub fn to_datums(
        ctx: &mut EvalContext,
        columns: &HashMap<i64, ColumnInfo>,
        val: &[u8],
    ) -> CodecResult<HashMap<i64, Datum>> {
        let row = RowSlice::from_bytes(val)?;
        let mut datums_map = HashMap::default();
        let mut buf = vec![];
        for (id, ci) in columns {
            if let Some((start, offset)) = row.search_in_non_null_ids(*id)? {
                let raw_datum = {
                    // encode with V1CompatibleEncoder and decode as v1 datum
                    buf.write_v2_as_datum(&row.values()[start..offset], ci)?;
                    let raw_datum = DatumDecoder::read_datum(&mut buf.as_slice())?;
                    buf.clear();
                    raw_datum
                };
                let row_datum = unflatten(ctx, raw_datum, ci)?;
                datums_map.insert(*id, row_datum);
            } else if row.search_in_null_ids(*id) {
                datums_map.insert(*id, Datum::Null);
            }
        }
        Ok(datums_map)
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

impl HrValue {
    pub fn from_bytes(
        ctx: &mut EvalContext,
        val: &[u8],
        columns: &HashMap<i64, ColumnInfo>,
    ) -> CodecResult<HrValue> {
        let value = match val.get(0) {
            Some(&row::v2::CODEC_VERSION) => HrValue::V2(RowV2::from_bytes(ctx, val, &columns)?),
            Some(_ /* v1 */) => HrValue::V1(RowV1::from_bytes(ctx, val, &columns)?),
            None => HrValue::V1(Default::default()),
        };

        Ok(value)
    }

    pub fn into_bytes(self, ctx: &mut EvalContext) -> CodecResult<Vec<u8>> {
        match self {
            HrValue::V1(row_v1) => row_v1.into_bytes(ctx),
            HrValue::V2(row_v2) => row_v2.into_bytes(ctx),
        }
    }

    pub fn to_datums(
        ctx: &mut EvalContext,
        columns: &HashMap<i64, ColumnInfo>,
        val: &[u8],
    ) -> CodecResult<HashMap<i64, Datum>> {
        assert!(!val.is_empty());
        Ok(match val[0] {
            row::v2::CODEC_VERSION => RowV2::to_datums(ctx, columns, val)?,
            _ => RowV1::to_datums(ctx, columns, val)?,
        })
    }
}
