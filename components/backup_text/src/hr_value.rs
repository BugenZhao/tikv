// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use serde::{Deserialize, Serialize};

use crate::hr_datum::HrDatum;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RowV1 {
    #[serde(rename = "#")]
    pub ids: Vec<u32>,
    #[serde(rename = "d")]
    pub datums: Vec<HrDatum>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowV2 {
    #[serde(rename = "b")]
    pub is_big: bool,
    #[serde(rename = "#!")]
    pub non_null_ids: Vec<u32>,
    #[serde(rename = "#?")]
    pub null_ids: Vec<u32>,
    #[serde(rename = "d")]
    pub datums: Vec<HrDatum>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "V")]
pub enum HrValue {
    #[serde(rename = "1")]
    V1(RowV1),
    #[serde(rename = "2")]
    V2(RowV2),
}
