// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use tipb::{ColumnInfo, FieldType, TableInfo};

/// Extracts `FieldType` from `ColumnInfo`.
/// Migrate from `tidb_query_executors::util::scan_executor`.
// TODO: Embed FieldType in ColumnInfo directly in Cop DAG v2 to remove this function.
pub fn field_type_from_column_info(ci: &ColumnInfo) -> FieldType {
    let mut field_type = FieldType::default();
    field_type.set_tp(ci.get_tp());
    field_type.set_flag(ci.get_flag() as u32); // FIXME: This `as u32` is really awful.
    field_type.set_flen(ci.get_column_len());
    field_type.set_decimal(ci.get_decimal());
    field_type.set_collate(ci.get_collation());
    field_type.set_elems(ci.get_elems().into());
    // Note: Charset is not provided in column info.
    field_type
}

pub fn kv_to_text(key: &[u8], val: &[u8], table: &TableInfo) -> String {
    let columns_info = table.get_columns();
    let schema = columns_info.iter().map(field_type_from_column_info);

    todo!()
}

pub fn text_to_kv(line: &str, table: &TableInfo) -> (Vec<u8>, Vec<u8>) {
    unimplemented!()
}
