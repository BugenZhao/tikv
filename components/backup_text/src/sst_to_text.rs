// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{
    from_text,
    hr_datum::{HrBytes, HrDatum},
    hr_kv::{CodecVersion, HrKv},
    hr_write::{HrKvWrite, HrWrite},
    to_text,
};
use datum::DatumDecoder;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tidb_query_datatype::codec::row::v2::{
    encoder_for_test::RowEncoder, RowSlice, V1CompatibleEncoder,
};
use tidb_query_datatype::{
    codec::{datum, row, table, Result as CodecResult},
    expr::EvalContext,
};
use tipb::{ColumnInfo, TableInfo};
use txn_types::WriteRef;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RowV1 {
    ids: Vec<u32>,
    datums: Vec<HrDatum>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RowV2 {
    is_big: bool,
    non_null_ids: Vec<u32>,
    null_ids: Vec<u32>,
    datums: Vec<HrDatum>,
}

pub fn kv_to_text(key: &[u8], val: &[u8], table: &TableInfo) -> CodecResult<String> {
    let columns_info = table.get_columns();
    let column_id_info: HashMap<i64, &'_ ColumnInfo, _> = columns_info
        .iter()
        .map(|ci| (ci.get_column_id(), ci))
        .collect();

    let (value, version) = match val.get(0) {
        Some(&row::v2::CODEC_VERSION) => {
            let row = RowSlice::from_bytes(val)?;
            let mut non_null_ids = Vec::with_capacity(row.values_num());
            let mut hr_datums = Vec::with_capacity(columns_info.len());
            for id in row.non_null_ids() {
                if let Some(ci) = column_id_info.get(&(id as i64)) {
                    let (start, offset) = row.search_in_non_null_ids(id as i64)?.unwrap();
                    // encode with V1CompatibleEncoder and decode as v1 datum
                    let mut buf = vec![];
                    buf.write_v2_as_datum(&row.values()[start..offset], *ci)?;
                    let datum = (&mut buf.as_slice()).read_datum()?;
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
            (to_text(row_v2), CodecVersion::V2)
        }
        Some(_ /* v1 */) => {
            let mut data = val;
            let (ids, datums) =
                table::decode_row_vec(&mut data, &mut EvalContext::default(), &column_id_info)?;
            let datums = datums.into_iter().map(HrDatum::from).collect();
            let row_v1 = RowV1 { ids, datums };
            (to_text(row_v1), CodecVersion::V1)
        }
        None => (Default::default(), CodecVersion::V1),
    };

    Ok(to_text(HrKv {
        version,
        key: HrBytes::from(key.to_vec()),
        value,
    }))
}

pub fn text_to_kv(line: &str, _table: &TableInfo) -> (Vec<u8>, Vec<u8>) {
    let HrKv {
        version,
        key,
        value,
    } = from_text(line);
    match version {
        CodecVersion::V1 => {
            let RowV1 { ids, datums } = from_text(&value);
            let ids: Vec<_> = ids.into_iter().map(|i| i as i64).collect();
            let datums = datums.into_iter().map(|d| d.into()).collect();
            let value = table::encode_row(&mut EvalContext::default(), datums, &ids).unwrap();
            (key.into(), value)
        }
        CodecVersion::V2 => {
            let RowV2 {
                is_big,
                non_null_ids,
                null_ids,
                datums,
            } = from_text(&value);
            let datums = datums.into_iter().map(|d| d.into()).collect();
            let mut buf = vec![];
            buf.write_row_with_datum(is_big, non_null_ids, null_ids, datums)
                .unwrap();
            (key.into(), buf)
        }
    }
}

pub fn kv_to_write(key: &[u8], val: &[u8]) -> String {
    let hr_write = HrKvWrite {
        key: HrBytes::from(key.to_vec()),
        value: HrWrite::from(WriteRef::parse(val).unwrap()),
    };
    to_text(hr_write)
}

pub fn write_to_kv(line: &str) -> (Vec<u8>, Vec<u8>) {
    let HrKvWrite { key, value } = from_text(line);
    let value: WriteRef<'_> = value.into();
    (key.into(), value.to_bytes())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use tidb_query_datatype::{
        codec::{
            data_type::{DateTime, Decimal, Duration, Json},
            row::v2::{self, encoder_for_test::Column as V2Column},
            table::{decode_row, encode_row},
            Datum,
        },
        FieldTypeAccessor, FieldTypeFlag, FieldTypeTp,
    };

    use tikv_util::map;

    use super::*;
    use collections::HashMap;

    fn table() -> (
        HashMap<i64, ColumnInfo>,
        HashMap<i64, Datum>,
        Vec<V2Column>,
        TableInfo,
    ) {
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

        let cols = map![
            1 => FieldTypeTp::LongLong.into(),
            2 => FieldTypeTp::VarChar.into(),
            3 => FieldTypeTp::NewDecimal.into(),
            5 => FieldTypeTp::JSON.into(),
            6 => duration_col,
            7 => small_unsigned_col,
            8 => FieldTypeTp::DateTime.into()
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

        let row = map![
            1 => Datum::I64(int()),
            2 => Datum::Bytes(bytes()),
            3 => Datum::Dec(dec()),
            5 => Datum::Json(json()),
            6 => Datum::Dur(dur()),
            7 => Datum::U64(small_int()),
            8 => Datum::Time(time())
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
        ];

        let table_info = {
            let mut info = TableInfo::new();
            info.set_table_id(233);
            info.set_columns(
                cols.clone()
                    .into_iter()
                    .map(|(id, mut ci)| {
                        ci.set_column_id(id);
                        ci
                    })
                    .collect(),
            );
            info
        };

        (cols, row, cols_v2, table_info)
    }

    #[test]
    fn test_v1() {
        let (cols, row, _, table_info) = table();
        let col_ids = row.iter().map(|p| *p.0).collect::<Vec<_>>();
        let col_values = row.iter().map(|p| p.1.clone()).collect::<Vec<_>>();

        let key = b"dummy_key";
        let val = encode_row(&mut EvalContext::default(), col_values.clone(), &col_ids).unwrap();

        let text = kv_to_text(key, &val, &table_info).unwrap();
        println!("{}", text);

        let (dec_key, dec_val) = text_to_kv(&text, &table_info);
        let dec_row =
            decode_row(&mut dec_val.as_slice(), &mut EvalContext::default(), &cols).unwrap();
        println!("{:?}", dec_row);

        assert_eq!(dec_key, key);
        assert_eq!(dec_row, row);
    }

    #[test]
    fn test_v2() {
        use v2::encoder_for_test::RowEncoder;

        let (cols_v1, row, cols_v2, table_info) = table();

        let key = b"dummy_key";
        let val = {
            let mut buf = vec![];
            buf.write_row(&mut EvalContext::default(), cols_v2).unwrap();
            buf
        };

        let text = kv_to_text(key, &val, &table_info).unwrap();
        println!("{}", text);

        let (dec_key, dec_val) = text_to_kv(&text, &table_info);
        let dec_row = decode_row(
            &mut dec_val.as_slice(),
            &mut EvalContext::default(),
            &cols_v1,
        )
        .unwrap();
        println!("{:?}", dec_row);

        assert_eq!(dec_key, key);
        assert_eq!(dec_row, row);
    }
}
