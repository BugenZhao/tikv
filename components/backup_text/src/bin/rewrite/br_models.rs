// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use serde::{Deserialize, Serialize};
use tipb::{ColumnInfo, TableInfo};

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct BrCiString {
    #[serde(rename = "O")]
    pub original: String,
    #[serde(rename = "L")]
    pub lower: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct BrDbInfo {
    pub id: i64,
    #[serde(rename = "db_name")]
    pub name: BrCiString,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct BrTableInfo {
    pub id: i64,
    pub name: BrCiString,
    #[serde(rename = "cols")]
    pub columns: Vec<BrColumnInfo>,
    pub pk_is_handle: bool,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct BrColumnInfo {
    pub id: i64,
    #[serde(rename = "type")]
    pub field_type: BrFieldType,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct BrFieldType {
    #[serde(rename = "Tp")]
    pub tp: u8, // `FieldTypeTp`
    #[serde(rename = "Flag")]
    pub flag: u32,
    #[serde(rename = "Flen")]
    pub flen: isize,
    #[serde(rename = "Decimal")]
    pub decimal: i32,
    #[serde(rename = "Elems")]
    pub elems: Option<Vec<String>>,
}

impl BrTableInfo {
    pub fn into_table_info_lossy(self) -> TableInfo {
        let BrTableInfo {
            id,
            columns,
            pk_is_handle,
            ..
        } = self;

        let mut ti = TableInfo::default();
        ti.set_table_id(id);
        ti.set_columns(
            columns
                .into_iter()
                .map(|c| {
                    use tidb_query_datatype::{FieldTypeAccessor, FieldTypeFlag};
                    const EXTRA_HANDLE_ID: i64 = -1;

                    let mut ci = c.into_column_info_lossy();
                    let pk_handle = (pk_is_handle && ci.flag().contains(FieldTypeFlag::PRI_KEY))
                        || ci.get_column_id() == EXTRA_HANDLE_ID;
                    ci.set_pk_handle(pk_handle);
                    ci
                })
                .collect(),
        );
        ti
    }
}

impl BrColumnInfo {
    pub fn into_column_info_lossy(self) -> ColumnInfo {
        let mut ci = ColumnInfo::default();
        ci.set_column_id(self.id);
        ci.set_tp(self.field_type.tp as i32);
        ci.set_flag(self.field_type.flag as i32);
        ci.set_column_len(self.field_type.flen as i32);
        ci.set_decimal(self.field_type.decimal);
        ci.set_elems(self.field_type.elems.unwrap_or_default().into());
        // todo: collation
        ci
    }
}

#[derive(Debug, Default, Clone)]
pub struct RewriteInfo {
    pub table_info: TableInfo,
    pub table_name: String,
    pub db_name: String,
}

impl From<kvproto::brpb::Schema> for RewriteInfo {
    fn from(schema: kvproto::brpb::Schema) -> Self {
        let br_table_info = {
            let str = String::from_utf8(schema.table).unwrap();
            serde_json::from_str::<BrTableInfo>(&str)
                .map_err(|e| format!("{}\n{}", e, str))
                .unwrap()
        };

        let br_db_info = {
            let str = String::from_utf8(schema.db).unwrap();
            serde_json::from_str::<BrDbInfo>(&str)
                .map_err(|e| format!("{}\n{}", e, str))
                .unwrap()
        };

        let table_name = br_table_info.name.original.clone();
        let db_name = br_db_info.name.original;
        let table_info = br_table_info.into_table_info_lossy();

        Self {
            table_info,
            table_name,
            db_name,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_info() {
        let str = r#"{"id":41,"name":{"O":"expr_pushdown_blacklist","L":"expr_pushdown_blacklist"},"charset":"utf8mb4","collate":"utf8mb4_bin",
        "cols":[{"id":1,"name":{"O":"name","L":"name"},"offset":0,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,
        "default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":254,"Flag":4097,"Flen":100,
        "Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null},"state":5,"comment":"","hidden":false,"change_state_info":null,
        "version":2},{"id":2,"name":{"O":"store_type","L":"store_type"},"offset":1,"origin_default":null,"origin_default_bit":null,"default":
        "tikv,tiflash,tidb","default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":
        {"Tp":254,"Flag":1,"Flen":100,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null},"state":5,"comment":"","hidden":false,
        "change_state_info":null,"version":2},{"id":3,"name":{"O":"reason","L":"reason"},"offset":2,"origin_default":null,"origin_default_bit":null,
        "default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":
        {"Tp":15,"Flag":0,"Flen":200,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null},"state":5,"comment":"","hidden":false,
        "change_state_info":null,"version":2}],"index_info":null,"constraint_info":null,"fk_info":null,"state":5,"pk_is_handle":false,
        "is_common_handle":false,"common_handle_version":0,"comment":"","auto_inc_id":561031,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":3,
        "max_idx_id":0,"max_cst_id":0,"update_timestamp":427397368802967566,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,
        "pre_split_regions":0,"partition":null,"compression":"","view":null,"sequence":null,"Lock":null,"version":4,"tiflash_replica":null,
        "is_columnar":false,"temp_table_type":0}"#;
        let br_table_info = serde_json::from_str::<BrTableInfo>(&str).unwrap();
        let table_info = br_table_info.into_table_info_lossy();
        println!("{:?}", table_info);
    }

    #[test]
    fn test_db_info() {
        let str = r#"{"id":3,"db_name":{"O":"__TiDB_BR_Temporary_mysql","L":"__tidb_br_temporary_mysql"},
        "charset":"utf8mb4","collate":"utf8mb4_bin","state":5}"#;
        let br_db_info = serde_json::from_str::<BrDbInfo>(str).unwrap();
        println!("{:?}", br_db_info);
    }
}
