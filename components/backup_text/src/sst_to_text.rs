// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{
    from_text, hr_datum,
    hr_index::HrIndex,
    hr_key::{HrDataKey, HrHandle},
    hr_kv::HrKv,
    hr_value::HrValue,
    hr_write::{HrIndexKvWrite, HrKvWrite, HrWrite},
    mask,
    rwer::Schema,
    to_text, Result,
};
use collections::HashMap;
use tidb_query_datatype::codec::datum::{Datum, DatumDecoder};
use tidb_query_datatype::codec::table::unflatten;
use tidb_query_datatype::{codec::Result as CodecResult, expr::EvalContext};
use tipb::ColumnInfo;
use txn_types::WriteRef;

pub fn kv_to_text(
    ctx: &mut EvalContext,
    key: &[u8],
    val: &[u8],
    columns: &HashMap<i64, ColumnInfo>,
) -> CodecResult<String> {
    let key = HrDataKey::from_encoded(key);
    let value = HrValue::from_bytes(ctx, val, columns)?;

    Ok(to_text(HrKv { key, value }))
}

pub fn kv_to_csv(
    ctx: &mut EvalContext,
    schema: &Schema,
    key: &[u8],
    val: &[u8],
) -> Result<Vec<u8>> {
    let mut datums_map = HrValue::to_datums(ctx, &schema.columns, val)?;
    if schema.handle_in_key() {
        let HrDataKey { handle, .. } = HrDataKey::from_encoded(key);
        match handle {
            HrHandle::Int(pk) => {
                if schema.primary_handle.is_none() {
                    return Err(format!("unexpect empty primary_handle").into());
                }
                datums_map.insert(schema.primary_handle.unwrap(), Datum::I64(pk));
            }
            HrHandle::Common(ch) => {
                if schema.common_handle.is_empty() {
                    return Err(format!("unexpect empty common_handle").into());
                }
                for (h, id) in ch.into_iter().zip(&schema.common_handle) {
                    datums_map.insert(*id, h.into());
                }
            }
        }
    }
    let mut res = Vec::new();
    for id in &schema.column_ids {
        let datum = match datums_map.remove(id) {
            Some(d) => d,
            None if !schema.columns[id].get_default_val().is_empty() => {
                // Set to the default value
                let ci = &schema.columns[id];
                let raw_datum = DatumDecoder::read_datum(&mut ci.get_default_val())?;
                unflatten(ctx, raw_datum, ci)?
            }
            None => Datum::Null,
        };
        let tp = &schema.columns[id];
        let masked_datum = mask::workload_sim_mask(datum, Some(tp)).unwrap_or_else(|d| d);
        hr_datum::write_bytes_to(&schema.columns[id], &mut res, &masked_datum);
        res.push(b',');
    }
    if !res.is_empty() {
        res.pop();
    }
    Ok(res)
}

pub fn index_kv_to_text(
    ctx: &mut EvalContext,
    key: &[u8],
    val: &[u8],
    columns: &HashMap<i64, ColumnInfo>,
) -> Result<String> {
    let key = HrIndex::decode_index_key(key)?;
    let value = HrIndex::decode_index_value(val, ctx, columns)?;
    Ok(to_text(HrIndex { key, value }))
}

pub fn text_to_kv(ctx: &mut EvalContext, line: &str) -> (Vec<u8>, Vec<u8>) {
    let HrKv { key, value } = from_text(line);

    let key = key.into_encoded();
    let value = value.into_bytes(ctx).unwrap();
    (key, value)
}

pub fn index_text_to_kv(ctx: &mut EvalContext, line: &str) -> (Vec<u8>, Vec<u8>) {
    let HrIndex { key, value } = from_text(line);

    let key = HrIndex::encode_index_key(ctx, key).unwrap();
    let value = HrIndex::encode_index_value(ctx, value).unwrap();
    (key, value)
}

pub fn kv_to_write(
    ctx: &mut EvalContext,
    key: &[u8],
    val: &[u8],
    columns: &HashMap<i64, ColumnInfo>,
) -> String {
    let hr_write = HrKvWrite {
        key: HrDataKey::from_encoded(&key),
        value: HrWrite::from_ref_data(ctx, WriteRef::parse(val).unwrap(), columns),
    };
    to_text(hr_write)
}

pub fn kv_to_csv_write(
    ctx: &mut EvalContext,
    schema: &Schema,
    key: &[u8],
    val: &[u8],
) -> Option<Result<Vec<u8>>> {
    let write_ref = WriteRef::parse(val).unwrap();
    write_ref
        .short_value
        .map(|val| kv_to_csv(ctx, schema, key, val))
}

pub fn write_to_kv(ctx: &mut EvalContext, line: &str) -> (Vec<u8>, Vec<u8>) {
    let HrKvWrite { key, value } = from_text(line);

    let key = key.into_encoded();
    let value = value.into_bytes(ctx);
    (key, value)
}

pub fn index_kv_to_write(
    ctx: &mut EvalContext,
    key: &[u8],
    val: &[u8],
    columns: &HashMap<i64, ColumnInfo>,
) -> String {
    let hr_write = HrIndexKvWrite {
        key: HrIndex::decode_index_key(key).unwrap(),
        value: HrWrite::from_ref_index(ctx, WriteRef::parse(val).unwrap(), columns),
    };
    to_text(hr_write)
}

pub fn index_write_to_kv(ctx: &mut EvalContext, line: &str) -> (Vec<u8>, Vec<u8>) {
    let HrIndexKvWrite { key, value } = from_text(line);

    let key = HrIndex::encode_index_key(ctx, key).unwrap();
    let value = value.into_bytes(ctx);
    (key, value)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use tidb_query_datatype::{
        codec::{
            data_type::{DateTime, Decimal, Duration, Enum, Json, Set},
            row::v2::{self, encoder_for_test::Column as V2Column},
            table::{decode_row, encode_row, encode_row_key},
            Datum,
        },
        FieldTypeAccessor, FieldTypeFlag, FieldTypeTp,
    };

    use tikv_util::map;
    use tipb::TableInfo;
    use txn_types::Key;

    use crate::eval_context;

    use super::*;
    use collections::HashMap;

    fn table() -> (
        Vec<u8>,
        HashMap<i64, ColumnInfo>,
        HashMap<i64, Datum>,
        Vec<V2Column>,
        TableInfo,
    ) {
        let key = keys::data_key(
            &Key::from_raw(&encode_row_key(233, 666))
                .append_ts(123456789.into())
                .into_encoded(),
        );

        let duration_col = {
            let mut col = ColumnInfo::default();
            col.as_mut_accessor()
                .set_tp(FieldTypeTp::Duration)
                .set_decimal(2);
            col
        };

        let small_unsigned_col = {
            let mut col = ColumnInfo::default();
            col.as_mut_accessor()
                .set_tp(FieldTypeTp::Short)
                .set_flag(FieldTypeFlag::UNSIGNED);
            col
        };

        let enum_elems = vec!["bug".to_owned(), "gen".to_owned()];
        let enum_col = {
            let mut col = ColumnInfo::default();
            col.as_mut_accessor()
                .set_tp(FieldTypeTp::Enum)
                .set_elems(&enum_elems);
            col
        };

        let set_elems = vec!["bug".to_owned(), "gen".to_owned()];
        let set_col = {
            let mut col = ColumnInfo::default();
            col.as_mut_accessor()
                .set_tp(FieldTypeTp::Set)
                .set_elems(&set_elems);
            col
        };

        let bit_col = {
            let mut col = ColumnInfo::default();
            col.as_mut_accessor().set_tp(FieldTypeTp::Bit);
            col
        };

        let cols_v1 = map![
            1 => FieldTypeTp::LongLong.into(),
            2 => FieldTypeTp::VarChar.into(),
            3 => FieldTypeTp::NewDecimal.into(),
            5 => FieldTypeTp::JSON.into(),
            6 => duration_col,
            7 => small_unsigned_col,
            8 => FieldTypeTp::DateTime.into(),
            9 => enum_col,
            11 => set_col,
            12 => bit_col
        ];

        let small_int = || 42;
        let int = || 123_456_789_233_666;
        let bytes = || b"abc".to_vec();
        let dec = || Decimal::from_str("233.345678").unwrap();
        let json = || Json::from_str(r#"{"name": "John"}"#).unwrap();
        let dur = || Duration::parse(&mut eval_context(), "23:23:23.666", 2).unwrap();
        let time = || {
            DateTime::parse_datetime(&mut eval_context(), "2021-08-12 12:34:56.789", 3, false)
                .unwrap()
        };
        let enum_ = || Enum::parse_value(2, &enum_elems);
        let set = || Set::parse_value(0b11, &set_elems);
        let bit = || 0b11;

        let row = map![
            1 => Datum::I64(int()),
            2 => Datum::Bytes(bytes()),
            3 => Datum::Dec(dec()),
            5 => Datum::Json(json()),
            6 => Datum::Dur(dur()),
            7 => Datum::U64(small_int()),
            8 => Datum::Time(time()),
            9 => Datum::Enum(enum_()),
            11 => Datum::Set(set()),
            12 => Datum::U64(bit())
        ];

        let cols_v2 = vec![
            V2Column::new(1, int()),
            V2Column::new(2, bytes()),
            V2Column::new(3, dec()),
            V2Column::new(5, json()),
            V2Column::new(6, dur()),
            V2Column::new(7, small_int() as i64),
            V2Column::new(8, time()),
            V2Column::new(9, enum_()),
            V2Column::new(11, set()),
            V2Column::new(12, bit() as i64),
        ];

        let table_info = {
            let mut info = TableInfo::new();
            info.set_table_id(233);
            info.set_columns(
                cols_v1
                    .clone()
                    .into_iter()
                    .map(|(id, mut ci)| {
                        ci.set_column_id(id);
                        ci
                    })
                    .collect(),
            );
            info
        };

        (key, cols_v1, row, cols_v2, table_info)
    }

    #[test]
    fn test_v1() {
        let ctx = &mut eval_context();

        let (key, cols_v1, row, _, _table_info) = table();
        let col_ids = row.iter().map(|p| *p.0).collect::<Vec<_>>();
        let col_values = row.iter().map(|p| p.1.clone()).collect::<Vec<_>>();

        let val = encode_row(ctx, col_values.clone(), &col_ids).unwrap();

        let text = kv_to_text(ctx, &key, &val, &cols_v1).unwrap();
        println!("{}", text);

        let (restored_key, restored_val) = text_to_kv(ctx, &text);
        let restored_row = decode_row(&mut restored_val.as_slice(), ctx, &cols_v1).unwrap();
        println!("{:?}", restored_row);

        assert_eq!(restored_key, key);
        assert_eq!(restored_val, val);
        assert_eq!(restored_row, row);
    }

    #[test]
    fn test_v2() {
        use v2::encoder_for_test::RowEncoder;
        let ctx = &mut eval_context();

        let (key, cols_v1, _, cols_v2, _table_info) = table();

        let val = {
            let mut buf = vec![];
            buf.write_row(ctx, cols_v2).unwrap();
            buf
        };

        let text = kv_to_text(ctx, &key, &val, &cols_v1).unwrap();
        println!("{}", text);

        let (restored_key, restored_val) = text_to_kv(ctx, &text);

        assert_eq!(restored_key, key);
        assert_eq!(restored_val, val);
    }

    #[test]
    fn test_json_depth() {
        let ctx = &mut eval_context();

        let json_depth = 123;
        let text = {
            let mut json_text = String::new();
            for _ in 0..json_depth {
                json_text.push('[');
            }
            for _ in 0..json_depth {
                json_text.push(']');
            }
            let text = r##"{"k":{"#":75,"t":427239193528500230,"h":1764},"v":{"V":"2","b":false,"#!":[2],"#?":[],"d":[{"j":<json>}]}}"##.to_owned();
            text.replace("<json>", &json_text)
        };

        let columns = {
            let col_1 = {
                let mut ci: ColumnInfo = FieldTypeTp::Long.into();
                ci.set_column_id(1);
                ci
            };
            let col_2 = {
                let mut ci: ColumnInfo = FieldTypeTp::JSON.into();
                ci.set_column_id(2);
                ci
            };
            map! {
                1 => col_1,
                2 => col_2
            }
        };
        let (..) = text_to_kv(ctx, &text);
    }
}
