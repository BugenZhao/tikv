use kvproto::brpb::Schema;
use serde::{Deserialize, Serialize};
use tipb::{ColumnInfo, TableInfo};

#[derive(Serialize, Deserialize, Debug, Default)]
struct BrTableInfo {
    pub id: i64,
    #[serde(rename = "cols")]
    pub columns: Vec<BrColumnInfo>,
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
        let mut ti = TableInfo::default();
        ti.set_table_id(self.id);
        ti.set_columns(
            self.columns
                .into_iter()
                .map(|c| c.into_column_info_lossy())
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
        // ci.set_column_len(self.field_type.flen as i32);  // todo: flen
        ci.set_decimal(self.field_type.decimal);
        if let Some(elems) = self.field_type.elems {
            ci.set_elems(elems.into());
        }
        ci
    }
}

pub fn schema_to_table_info(mut schema: Schema) -> TableInfo {
    let str = String::from_utf8(schema.take_table()).unwrap();
    let br_table_info = serde_json::from_str::<BrTableInfo>(&str)
        .map_err(|e| format!("{}\n{}", e, str))
        .unwrap();
    br_table_info.into_table_info_lossy()
}
