// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use serde::{Deserialize, Serialize};
use tidb_query_datatype::codec::table::*;
use txn_types::{Key, TimeStamp};

use crate::{eval_context, hr_datum::HrDatum};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HrHandle {
    Common(Vec<HrDatum>),
    Int(i64),
}

impl HrHandle {
    pub fn from_encoded_common_handle(handle: &[u8]) -> Self {
        let datums = decode_common_handle_into_datums(handle)
            .unwrap()
            .into_iter()
            .map(HrDatum::from)
            .collect();
        Self::Common(datums)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrDataKey {
    #[serde(rename = "#")]
    table_id: i64,
    #[serde(rename = "t")]
    ts: Option<u64>,
    #[serde(rename = "h")]
    pub handle: HrHandle,
}

impl HrDataKey {
    pub fn from_encoded(encoded_key: &[u8]) -> Self {
        let (raw_key, ts) = decode_key(encoded_key);
        let table_id = decode_table_id(&raw_key).unwrap();
        let handle = if raw_key.len() == PREFIX_LEN + 8 {
            // Int handle
            decode_int_handle(&raw_key).map(HrHandle::Int).unwrap()
        } else {
            // Common handle
            decode_common_handle(&raw_key)
                .map(HrHandle::from_encoded_common_handle)
                .unwrap()
        };
        Self {
            table_id,
            ts: ts.map(|t| t.into_inner()),
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
            HrHandle::Common(datums) => {
                let datums = datums.into_iter().map(HrDatum::into).collect::<Vec<_>>();
                let handle =
                    encode_common_handle_from_datums(&mut eval_context(), &datums).unwrap();
                encode_row_key_with_common_handle(table_id, &handle)
            }
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

pub fn decode_key(data_key: &[u8]) -> (Vec<u8>, Option<TimeStamp>) {
    let origin_encoded_key = keys::origin_key(data_key);
    let (user_key, ts) = Key::split_on_ts_for(origin_encoded_key)
        .map_or_else(|_| (origin_encoded_key, None), |(k, t)| (k, Some(t)));
    let raw_key = Key::from_encoded_slice(user_key).into_raw().unwrap();
    (raw_key, ts)
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::codec::Datum;

    use super::*;

    #[test]
    fn test_default_cf() {
        let encoded_key = vec![
            122, 116, 128, 0, 0, 0, 0, 0, 0, 255, 59, 95, 114, 128, 0, 0, 0, 0, 255, 0, 0, 10, 0,
            0, 0, 0, 0, 250, 250, 18, 182, 126, 209, 95, 255, 253,
        ];

        let hr = HrDataKey::from_encoded(&encoded_key);
        println!("{}", serde_json::to_string(&hr).unwrap());
        let restored_encoded_key = hr.into_encoded();
        assert_eq!(encoded_key, restored_encoded_key);
    }

    #[test]
    fn test_write_cf() {
        let encoded_key = vec![
            122, 116, 128, 0, 0, 0, 0, 0, 0, 255, 59, 95, 114, 128, 0, 0, 0, 0, 255, 0, 0, 10, 0,
            0, 0, 0, 0, 250, 250, 18, 182, 126, 208, 151, 255, 254,
        ];

        let hr = HrDataKey::from_encoded(&encoded_key);
        println!("{}", serde_json::to_string(&hr).unwrap());
        let restored_encoded_key = hr.into_encoded();
        assert_eq!(encoded_key, restored_encoded_key);
    }

    #[test]
    fn test_common_handle() {
        let datums = vec![Datum::Dec(1.into()), Datum::Bytes(b"hello".to_vec())];
        let handle = encode_common_handle_from_datums(&mut eval_context(), &datums).unwrap();
        let hr_handle = HrHandle::from_encoded_common_handle(&handle);
        let decoded_datums: Vec<Datum> = match hr_handle {
            HrHandle::Common(datums) => datums.into_iter().map(HrDatum::into).collect(),
            HrHandle::Int(_) => unreachable!(),
        };
        assert_eq!(datums, decoded_datums);
    }

    #[test]
    fn test_key_with_common_handle() {
        // decimal 1: [6, 1, 0, 129]
        // todo: test with real key
        let encoded_key = vec![
            122, 116, 128, 0, 0, 0, 0, 0, 0, 255, 59, 95, 114, 6, 1, 0, 129, 0, 254, 250, 18, 182,
            126, 208, 151, 255, 254,
        ];

        let hr = HrDataKey::from_encoded(&encoded_key);
        println!("{}", serde_json::to_string(&hr).unwrap());
        let restored_encoded_key = hr.into_encoded();
        assert_eq!(encoded_key, restored_encoded_key);
    }
}
