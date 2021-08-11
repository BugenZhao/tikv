// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use tidb_query_datatype::{
    codec::{datum, row, table, Datum, Result as CodecResult},
    expr::EvalContext,
};
use tipb::{ColumnInfo, TableInfo};

use crate::{
    hr_datum::{HrBytes, HrDatum},
    hr_kv::HrKv,
};

pub fn kv_to_text(key: &[u8], val: &[u8], table: &TableInfo) -> CodecResult<String> {
    let columns_info = table.get_columns();
    let column_id_info: HashMap<i64, ColumnInfo, _> = columns_info
        .iter()
        .map(|ci| (ci.get_column_id(), ci.clone()))
        .collect();

    let hr_datums = match val.get(0) {
        Some(&row::v2::CODEC_VERSION) => {
            use datum::DatumDecoder;
            use row::v2::{RowSlice, V1CompatibleEncoder};

            let row = RowSlice::from_bytes(val)?;
            let mut hr_datums = HashMap::with_capacity(columns_info.len());

            for (col_id, ci) in column_id_info {
                let datum = if let Some((start, offset)) = row.search_in_non_null_ids(col_id)? {
                    let data = &row.values()[start..offset];
                    let mut buf = vec![];
                    buf.write_v2_as_datum(data, &ci).unwrap();
                    (&mut buf.as_slice()).read_datum()?
                } else if row.search_in_null_ids(col_id) {
                    Datum::Null
                } else {
                    // This column is missing (or with default value)?
                    Datum::Null
                };
                hr_datums.insert(col_id, HrDatum::from(datum));
            }

            hr_datums
        }
        Some(_ /* v1 */) => {
            let mut data = val;
            let datums =
                table::decode_row(&mut data, &mut EvalContext::default(), &column_id_info)?;
            datums
                .into_iter()
                .map(|(k, v)| (k, HrDatum::from(v)))
                .collect()
        }
        None => Default::default(),
    };

    Ok(HrKv {
        key: HrBytes::from(key.to_vec()),
        value: hr_datums,
    }
    .to_text())
}

pub fn text_to_kv(line: &str, _table: &TableInfo) -> (Vec<u8>, Vec<u8>) {
    let kv = HrKv::from_text(line);

    let mut col_ids = Vec::with_capacity(kv.value.len());
    let mut row = Vec::with_capacity(kv.value.len());

    for (col_id, datum) in kv.value.into_iter() {
        col_ids.push(col_id);
        row.push(datum.into());
    }

    // always encode in v1 format
    let value = table::encode_row(&mut EvalContext::default(), row, &col_ids).unwrap();
    let key = kv.key.into();

    (key, value)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use tidb_query_datatype::{
        codec::{
            data_type::{Decimal, Duration, Json},
            row::v2::{self, encoder_for_test::Column as V2Column},
            table::{decode_row, encode_row},
        },
        FieldTypeAccessor, FieldTypeTp,
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
        let mut duration_col = ColumnInfo::default();
        duration_col
            .as_mut_accessor()
            .set_tp(FieldTypeTp::Duration)
            .set_decimal(2);

        let cols = map![
            1 => FieldTypeTp::LongLong.into(),
            2 => FieldTypeTp::VarChar.into(),
            3 => FieldTypeTp::NewDecimal.into(),
            5 => FieldTypeTp::JSON.into(),
            6 => duration_col
        ];

        let int = || 123_456_789_233_666;
        let bytes = || b"abc".to_vec();
        let dec = || Decimal::from_str("233.345678").unwrap();
        let json = || Json::from_str(r#"{"name": "John"}"#).unwrap();
        let dur = || Duration::parse(&mut EvalContext::default(), "23:23:23.666", 2).unwrap();

        let row = map![
            1 => Datum::I64(int()),
            2 => Datum::Bytes(bytes()),
            3 => Datum::Dec(dec()),
            5 => Datum::Json(json()),
            6 => Datum::Dur(dur())
        ];

        let cols_v2 = vec![
            V2Column::new(1, int()),
            V2Column::new(2, bytes()),
            V2Column::new(3, dec()),
            V2Column::new(5, json()),
            V2Column::new(6, dur())
                .with_tp(FieldTypeTp::Duration)
                .with_decimal(2),
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

        assert_eq!(dec_key, key);
        assert_eq!(dec_row, row);
    }
}
