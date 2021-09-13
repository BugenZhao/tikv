// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::brpb::Schema as BrSchema;
use serde::{Deserialize, Serialize};
use slog_global::warn;
use tipb::{ColumnInfo, TableInfo};

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct BrCiString {
    #[serde(rename = "O")]
    pub original: String,
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
    pub fn into_table_info_lossy(self, db_name: &str) -> TableInfo {
        let BrTableInfo {
            id,
            columns,
            pk_is_handle,
            ..
        } = self;

        let mut ti = TableInfo::default();
        ti.set_table_id(id);
        ti.set_name(format!("{}.{}", db_name, self.name.original));
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

pub fn schema_to_table_info(schema: BrSchema) -> TableInfo {
    let BrSchema {
        db,
        table,
        table_info,
        ..
    } = schema;

    if table_info.is_empty() {
        let br_table_info = {
            let str = String::from_utf8(table).unwrap();
            serde_json::from_str::<BrTableInfo>(&str)
                .map_err(|e| format!("{}\n{}", e, str))
                .unwrap()
        };
        let db_name = {
            let str = String::from_utf8(db).unwrap();
            let info = serde_json::from_str::<BrDbInfo>(&str)
                .map_err(|e| format!("{}\n{}", e, str))
                .unwrap();
            info.name.original
        };

        let table_info = br_table_info.into_table_info_lossy(&db_name);
        warn!(
            "no tipb::TableInfo found, convert from br models";
            "table" => table_info.get_name(),
        );
        table_info
    } else {
        protobuf::parse_from_bytes::<TableInfo>(&table_info).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use kvproto::brpb::Schema;
    use protobuf::Message;

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
        let table_info = br_table_info.into_table_info_lossy("foo");
        println!("{:?}", table_info);
        assert_eq!(table_info.get_name(), "foo.expr_pushdown_blacklist");
    }

    #[test]
    fn test_db_info() {
        let str = r#"{"id":3,"db_name":{"O":"__TiDB_BR_Temporary_mysql","L":"__tidb_br_temporary_mysql"},
        "charset":"utf8mb4","collate":"utf8mb4_bin","state":5}"#;
        let br_db_info = serde_json::from_str::<BrDbInfo>(str).unwrap();
        println!("{:?}", br_db_info);
    }

    #[test]
    fn test_patched_table_info() {
        let br = || {
            let ti = BrTableInfo {
                id: 6,
                name: BrCiString {
                    original: "br_name".to_owned(),
                },
                ..Default::default()
            };
            serde_json::to_vec(&ti).unwrap()
        };
        let pb = || {
            let mut ti = TableInfo::new();
            ti.set_table_id(2);
            ti.set_name("test.pb_name".to_owned());
            ti.write_to_bytes().unwrap()
        };
        let db = || {
            r#"{"id":42,"db_name":{"O":"test","L":"test"},
        "charset":"utf8mb4","collate":"utf8mb4_bin","state":5}"#
                .to_string()
                .into_bytes()
        };

        {
            let old = {
                let mut s = Schema::new();
                s.set_db(db());
                s.set_table(br());
                s
            };
            let ti = schema_to_table_info(old);
            assert_eq!(ti.get_name(), "test.br_name");
            assert_eq!(ti.get_table_id(), 6);
        }
        {
            let patched = {
                let mut s = Schema::new();
                s.set_db(db());
                s.set_table(br());
                s.set_table_info(pb());
                s
            };
            let ti = schema_to_table_info(patched);
            assert_eq!(ti.get_name(), "test.pb_name");
            assert_eq!(ti.get_table_id(), 2);
        }
    }
}
