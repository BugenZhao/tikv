// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{
    from_text,
    hr_datum::HrDatum,
    hr_key::HrDataKey,
    hr_kv::HrKv,
    hr_value::{HrValue, RowV1, RowV2},
    hr_write::{HrKvWrite, HrWrite},
    to_text,
};
use datum::DatumDecoder;
use std::collections::HashMap;
use tidb_query_datatype::codec::{
    row::v2::{encoder_for_test::RowEncoder, RowSlice, V1CompatibleEncoder},
    table::unflatten,
};
use tidb_query_datatype::{
    codec::{datum, row, table, Result as CodecResult},
    expr::EvalContext,
};
use tipb::{ColumnInfo, TableInfo};
use txn_types::WriteRef;

pub fn kv_to_text(key: &[u8], val: &[u8], table: &TableInfo) -> CodecResult<String> {
    let ctx = &mut EvalContext::default();
    let columns_info = table.get_columns();
    let column_id_info: HashMap<i64, &'_ ColumnInfo, _> = columns_info
        .iter()
        .map(|ci| (ci.get_column_id(), ci))
        .collect();

    let value = match val.get(0) {
        Some(&row::v2::CODEC_VERSION) => {
            let row = RowSlice::from_bytes(val)?;
            let mut non_null_ids = Vec::with_capacity(row.values_num());
            let mut hr_datums = Vec::with_capacity(columns_info.len());
            for id in row.non_null_ids() {
                if let Some(ci) = column_id_info.get(&(id as i64)) {
                    let (start, offset) = row.search_in_non_null_ids(id as i64)?.unwrap();
                    let raw_datum = {
                        // encode with V1CompatibleEncoder and decode as v1 datum
                        let mut buf = vec![];
                        buf.write_v2_as_datum(&row.values()[start..offset], *ci)?;
                        (&mut buf.as_slice()).read_datum()?
                    };
                    let datum = unflatten(ctx, raw_datum, *ci)?;
                    non_null_ids.push(id);
                    hr_datums.push(HrDatum::from(datum));
                }
            }
            let row_v2 = RowV2 {
                is_big: row.is_big(),
                non_null_ids,
                null_ids: row.null_ids(),
                datums: hr_datums,
            };
            HrValue::V2(row_v2)
        }
        Some(_ /* v1 */) => {
            let mut data = val;
            let (ids, datums) = table::decode_row_vec(&mut data, ctx, &column_id_info)?;
            let datums = datums.into_iter().map(HrDatum::from).collect();
            let row_v1 = RowV1 { ids, datums };
            HrValue::V1(row_v1)
        }
        None => HrValue::V1(Default::default()),
    };

    let key = HrDataKey::from_encoded(key);

    Ok(to_text(HrKv { key, value }))
}

pub fn text_to_kv(line: &str, _table: &TableInfo) -> (Vec<u8>, Vec<u8>) {
    let ctx = &mut EvalContext::default();
    let HrKv { key, value } = from_text(line);

    match value {
        HrValue::V1(row) => {
            let RowV1 { ids, datums } = row;
            let ids: Vec<_> = ids.into_iter().map(|i| i as i64).collect();
            let datums = datums.into_iter().map(|d| d.into()).collect();
            let value = table::encode_row(ctx, datums, &ids).unwrap();
            (key.into_encoded(), value)
        }
        HrValue::V2(row) => {
            let RowV2 {
                is_big,
                non_null_ids,
                null_ids,
                datums,
            } = row;
            let datums = datums.into_iter().map(|d| d.into()).collect();
            let mut buf = vec![];
            buf.write_row_with_datum(ctx, is_big, non_null_ids, null_ids, datums)
                .unwrap();
            (key.into_encoded(), buf)
        }
    }
}

pub fn kv_to_write(key: &[u8], val: &[u8]) -> String {
    let hr_write = HrKvWrite {
        key: HrDataKey::from_encoded(&key),
        value: HrWrite::from(WriteRef::parse(val).unwrap()),
    };
    to_text(hr_write)
}

pub fn write_to_kv(line: &str) -> (Vec<u8>, Vec<u8>) {
    let HrKvWrite { key, value } = from_text(line);
    let value: WriteRef<'_> = value.into();
    (key.into_encoded(), value.to_bytes())
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
    use txn_types::Key;

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

        let cols_v1 = map![
            1 => FieldTypeTp::LongLong.into(),
            2 => FieldTypeTp::VarChar.into(),
            3 => FieldTypeTp::NewDecimal.into(),
            5 => FieldTypeTp::JSON.into(),
            6 => duration_col,
            7 => small_unsigned_col,
            8 => FieldTypeTp::DateTime.into(),
            9 => enum_col,
            11 => set_col
        ];

        let small_int = || 42;
        let int = || 123_456_789_233_666;
        let bytes = || b"abc".to_vec();
        let dec = || Decimal::from_str("233.345678").unwrap();
        let json = || Json::from_str(r#"{"name": "John"}"#).unwrap();
        let dur = || Duration::parse(&mut EvalContext::default(), "23:23:23.666", 2).unwrap();
        let time = || {
            DateTime::parse_datetime(
                &mut EvalContext::default(),
                "2021-08-12 12:34:56.789",
                3,
                false,
            )
            .unwrap()
        };
        let enum_ = || Enum::parse_value(2, &enum_elems);
        let set = || Set::parse_value(0b11, &set_elems);

        let row = map![
            1 => Datum::I64(int()),
            2 => Datum::Bytes(bytes()),
            3 => Datum::Dec(dec()),
            5 => Datum::Json(json()),
            6 => Datum::Dur(dur()),
            7 => Datum::U64(small_int()),
            8 => Datum::Time(time()),
            9 => Datum::Enum(enum_()),
            11 => Datum::Set(set())
        ];

        let cols_v2 = vec![
            V2Column::new(1, int()),
            V2Column::new(2, bytes()),
            V2Column::new(3, dec()),
            V2Column::new(5, json()),
            V2Column::new(6, dur())
                .with_tp(FieldTypeTp::Duration)
                .with_decimal(2),
            V2Column::new(7, small_int() as i64).with_unsigned(),
            V2Column::new(8, time()).with_tp(FieldTypeTp::DateTime),
            V2Column::new(9, enum_()),
            V2Column::new(11, set()),
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
        let (key, cols_v1, row, _, table_info) = table();
        let col_ids = row.iter().map(|p| *p.0).collect::<Vec<_>>();
        let col_values = row.iter().map(|p| p.1.clone()).collect::<Vec<_>>();

        let val = encode_row(&mut EvalContext::default(), col_values.clone(), &col_ids).unwrap();

        let text = kv_to_text(&key, &val, &table_info).unwrap();
        println!("{}", text);

        let (restored_key, restored_val) = text_to_kv(&text, &table_info);
        let restored_row = decode_row(
            &mut restored_val.as_slice(),
            &mut EvalContext::default(),
            &cols_v1,
        )
        .unwrap();
        println!("{:?}", restored_row);

        assert_eq!(restored_key, key);
        assert_eq!(restored_val, val);
        assert_eq!(restored_row, row);
    }

    #[test]
    fn test_v2() {
        use v2::encoder_for_test::RowEncoder;

        let (key, _, _, cols_v2, table_info) = table();

        let val = {
            let mut buf = vec![];
            buf.write_row(&mut EvalContext::default(), cols_v2).unwrap();
            buf
        };

        let text = kv_to_text(&key, &val, &table_info).unwrap();
        println!("{}", text);

        let (restored_key, restored_val) = text_to_kv(&text, &table_info);

        assert_eq!(restored_key, key);
        assert_eq!(restored_val, val);
    }
}
