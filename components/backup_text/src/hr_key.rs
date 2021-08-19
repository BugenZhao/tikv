// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use serde::{Deserialize, Serialize};
use tidb_query_datatype::codec::table::*;
use txn_types::{Key, TimeStamp};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HrHandle {
    Common(Vec<u8>), // todo: human readable common handle
    Int(i64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrDataKey {
    #[serde(rename = "#")]
    table_id: i64,
    #[serde(rename = "t")]
    ts: Option<u64>,
    #[serde(rename = "h")]
    handle: HrHandle,
}

impl HrDataKey {
    pub fn from_encoded(encoded_key: &[u8]) -> Self {
        let origin_encoded_key = keys::origin_key(encoded_key);
        let key = Key::from_encoded_slice(origin_encoded_key);
        let ts = key.decode_ts().map(|ts| ts.into_inner()).ok();
        let raw_key = if ts.is_some() {
            key.truncate_ts().unwrap()
        } else {
            key
        }
        .into_raw()
        .unwrap();

        let table_id = decode_table_id(&raw_key).unwrap();
        let handle = decode_int_handle(&raw_key)
            .map(HrHandle::Int)
            .or_else(|_| decode_common_handle(&raw_key).map(|h| HrHandle::Common(h.to_vec())))
            .unwrap();

        Self {
            table_id,
            ts,
            handle,
        }
    }

    pub fn into_encoded(self) -> Vec<u8> {
        let HrDataKey {
            table_id,
            ts,
            handle,
        } = self;
        let raw_key = match handle {
            HrHandle::Common(h) => encode_common_handle_for_test(table_id, &h),
            HrHandle::Int(h) => encode_row_key(table_id, h),
        };
        let mut key = Key::from_raw(&raw_key);
        if let Some(ts) = ts {
            key = key.append_ts(TimeStamp::new(ts));
        }
        let origin_encoded_key = key.into_encoded();

        keys::data_key(&origin_encoded_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_it_works() {
        let encoded_key = vec![
            122, // z
            116, // t
            128, 0, 0, 0, 0, 0, 0, 255, 57, 95, 114, // _r
            128, 0, 0, 0, 0, 255, 0, 0, 32, 0, 0, 0, 0, 0, 250, 250, 18, 176, 87, 104, 223, 255,
            253,
        ];

        let hr = HrDataKey::from_encoded(&encoded_key);
        println!("{}", serde_json::to_string(&hr).unwrap());
        let restored_encoded_key = hr.into_encoded();
        assert_eq!(encoded_key, restored_encoded_key);
    }
}
