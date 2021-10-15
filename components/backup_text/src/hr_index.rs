// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::hr_datum::{HrBytes, HrDatum};
use crate::hr_value::RowV2;
use crate::Result;
use codec::number::NumberCodec;
use codec::prelude::{NumberDecoder, NumberEncoder};
use collections::HashMap;
use serde::{Deserialize, Serialize};
use tidb_query_datatype::codec::datum;
use tidb_query_datatype::codec::table::{
    self, decode_index_key_id, encode_index_seek_key, INDEX_VALUE_VERSION_FLAG,
    MAX_OLD_ENCODED_VALUE_LEN,
};
use tidb_query_datatype::expr::EvalContext;
use tipb::ColumnInfo;
use txn_types::{Key, TimeStamp};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrIndexKey {
    #[serde(rename = "#")]
    table_id: i64,
    #[serde(rename = "i")]
    index_id: i64,
    #[serde(rename = "h")]
    handles: Vec<HrDatum>,
    #[serde(rename = "t")]
    ts: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HrIndexValue {
    #[serde(rename = "t", skip_serializing_if = "Option::is_none", default)]
    tail_len: Option<u8>,
    #[serde(rename = "v", skip_serializing_if = "Option::is_none", default)]
    version_segment: Option<(u8, u8)>,
    #[serde(rename = "h", skip_serializing_if = "Option::is_none", default)]
    common_handle: Option<Vec<HrDatum>>,
    #[serde(rename = "p", skip_serializing_if = "Option::is_none", default)]
    partition_id: Option<i64>,
    #[serde(rename = "r", skip_serializing_if = "Option::is_none", default)]
    restore_data: Option<RowV2>,
    #[serde(rename = "T", skip_serializing_if = "Option::is_none", default)]
    tail: Option<HrIndexTail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum HrIndexTail {
    #[serde(rename = "i")]
    Int(i64),
    #[serde(rename = "p")]
    Padding(HrBytes),
}

impl HrIndexTail {
    fn from_bytes(mut data: &[u8]) -> HrIndexTail {
        if data.len() >= 8 {
            HrIndexTail::Int(data.read_u64().unwrap() as i64)
        } else {
            HrIndexTail::Padding(HrBytes::from(data.to_vec()))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrIndex {
    #[serde(rename = "k")]
    pub key: HrIndexKey,
    #[serde(rename = "v")]
    pub value: HrIndexValue,
}

impl HrIndex {
    pub fn decode_index_key(index_key: &[u8]) -> Result<HrIndexKey> {
        let origin_encoded_key = keys::origin_key(index_key);
        let (user_key, ts) = Key::split_on_ts_for(origin_encoded_key)?;
        let raw_key = Key::from_encoded_slice(user_key).into_raw()?;
        let (table_id, index_id) = decode_index_key_id(&raw_key)?;
        // TODO: unflatten
        let handles = datum::decode(&mut &raw_key[table::PREFIX_LEN + table::ID_LEN..])?
            .into_iter()
            .map(|d| HrDatum::with_workload_sim_mask(d, None))
            .collect();
        Ok(HrIndexKey {
            table_id,
            index_id,
            handles,
            ts: ts.into_inner(),
        })
    }

    pub fn encode_index_key(ctx: &mut EvalContext, index_key: HrIndexKey) -> Result<Vec<u8>> {
        let HrIndexKey {
            table_id,
            index_id,
            handles,
            ts,
        } = index_key;
        let handles: Vec<_> = handles.into_iter().map(HrDatum::into).collect();
        let raw_key = encode_index_seek_key(table_id, index_id, &datum::encode_key(ctx, &handles)?);
        let origin_encoded_key = {
            let mut key = Key::from_raw(&raw_key);
            key = key.append_ts(TimeStamp::new(ts));
            key.into_encoded()
        };
        Ok(keys::data_key(&origin_encoded_key))
    }

    pub fn decode_index_value(
        index_value: &[u8],
        ctx: &mut EvalContext,
        cols: &HashMap<i64, ColumnInfo>,
    ) -> Result<HrIndexValue> {
        // Process old collation kv
        if index_value.len() <= MAX_OLD_ENCODED_VALUE_LEN {
            return Ok(HrIndexValue {
                tail: Some(HrIndexTail::from_bytes(index_value)),
                ..Default::default()
            });
        }

        let index_version = HrIndex::get_index_version(index_value)?;
        let tail_len = index_value[0] as usize;
        if tail_len >= index_value.len() {
            return Err(format!("`tail_len`: {} is corrupted", tail_len).into());
        }
        let (remaining, tail) = if index_version == 1 {
            // Skip the version segment.
            index_value[3..].split_at(index_value.len() - 3 - tail_len)
        } else {
            index_value[1..].split_at(index_value.len() - 1 - tail_len)
        };
        let version_segment = if index_version == 1 {
            Some((index_value[1], index_value[2]))
        } else {
            None
        };
        let tail = if !tail.is_empty() {
            Some(HrIndexTail::from_bytes(tail))
        } else {
            None
        };
        let (mut common_handle_bytes, remaining) = HrIndex::split_common_handle(remaining)?;
        let (partition_id_bytes, remaining) = HrIndex::split_partition_id(remaining)?;
        let (restore_data_bytes, remaining) = HrIndex::split_restore_data(remaining)?;
        let (mut common_handle, mut partition_id, mut restore_data) = (None, None, None);
        if !remaining.is_empty() {
            return Err(format!("Unexpected corrupted extra bytes: {:?}", remaining).into());
        }
        if !common_handle_bytes.is_empty() {
            let handles = datum::decode(&mut common_handle_bytes)?
                .into_iter()
                .map(|d| HrDatum::with_workload_sim_mask(d, None))
                .collect();
            common_handle = Some(handles);
        }
        if !partition_id_bytes.is_empty() {
            partition_id = Some(NumberCodec::decode_i64(partition_id_bytes));
        }
        if !restore_data_bytes.is_empty() {
            restore_data = Some(RowV2::from_bytes(ctx, restore_data_bytes, cols)?);
        }
        Ok(HrIndexValue {
            tail_len: Some(tail_len as u8),
            version_segment,
            common_handle,
            partition_id,
            restore_data,
            tail,
        })
    }

    pub fn encode_index_value(ctx: &mut EvalContext, index_value: HrIndexValue) -> Result<Vec<u8>> {
        let mut buf = vec![];
        let HrIndexValue {
            tail_len,
            version_segment,
            common_handle,
            partition_id,
            restore_data,
            tail,
        } = index_value;
        if let Some(tail_len) = tail_len {
            buf.push(tail_len);
        }
        if let Some(version_segment) = version_segment {
            buf.push(version_segment.0);
            buf.push(version_segment.1);
        }
        if let Some(common_handle) = common_handle {
            let handles: Vec<_> = common_handle.into_iter().map(HrDatum::into).collect();
            let common_handle_bytes = datum::encode_key(ctx, &handles)?;
            buf.push(table::INDEX_VALUE_COMMON_HANDLE_FLAG);
            buf.write_u16(common_handle_bytes.len() as u16)?;
            buf.extend(&common_handle_bytes);
        }
        if let Some(partition_id) = partition_id {
            buf.push(table::INDEX_VALUE_PARTITION_ID_FLAG);
            NumberCodec::encode_i64(&mut buf, partition_id);
        }
        if let Some(restore_data) = restore_data {
            buf.push(table::INDEX_VALUE_RESTORED_DATA_FLAG);
            buf.extend(restore_data.into_bytes(ctx)?);
        }
        if let Some(tail) = tail {
            match tail {
                HrIndexTail::Int(int_handle) => buf.write_u64(int_handle as u64)?,
                HrIndexTail::Padding(padding) => buf.extend::<Vec<_>>(padding.into()),
            }
        }
        Ok(buf)
    }

    #[inline]
    fn split_common_handle(value: &[u8]) -> Result<(&[u8], &[u8])> {
        if value
            .get(0)
            .map_or(false, |c| *c == table::INDEX_VALUE_COMMON_HANDLE_FLAG)
        {
            let handle_len = (&value[1..]).read_u16()? as usize;
            let handle_end_offset = 3 + handle_len;
            if handle_end_offset > value.len() {
                return Err(format!("`handle_len` is corrupted: {}", handle_len).into());
            }
            Ok(value[3..].split_at(handle_len))
        } else {
            Ok(value.split_at(0))
        }
    }

    #[inline]
    fn split_partition_id(value: &[u8]) -> Result<(&[u8], &[u8])> {
        if value
            .get(0)
            .map_or(false, |c| *c == table::INDEX_VALUE_PARTITION_ID_FLAG)
        {
            if value.len() < 9 {
                return Err(format!(
                    "Remaining len {} is too short to decode partition ID",
                    value.len()
                )
                .into());
            }
            Ok(value[1..].split_at(8))
        } else {
            Ok(value.split_at(0))
        }
    }

    #[inline]
    fn split_restore_data(value: &[u8]) -> Result<(&[u8], &[u8])> {
        Ok(
            if value
                .get(0)
                .map_or(false, |c| *c == table::INDEX_VALUE_RESTORED_DATA_FLAG)
            {
                (value, &value[value.len()..])
            } else {
                (&value[..0], value)
            },
        )
    }

    // get_index_version is the same as getIndexVersion() in the TiDB repo.
    fn get_index_version(value: &[u8]) -> Result<i64> {
        if value.len() == 3 || value.len() == 4 {
            // For the unique index with null value or non-unique index, the length can be 3 or 4 if <= 9.
            return Ok(1);
        }
        if value.len() <= MAX_OLD_ENCODED_VALUE_LEN {
            return Ok(0);
        }
        let tail_len = value[0] as usize;
        if tail_len >= value.len() {
            return Err(format!("`tail_len`: {} is corrupted", tail_len).into());
        }
        if (tail_len == 0 || tail_len == 1) && value[1] == INDEX_VALUE_VERSION_FLAG {
            return Ok(value[2] as i64);
        }

        Ok(0)
    }
}
