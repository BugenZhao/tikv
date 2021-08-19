// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::hr_datum::HrBytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrKv {
    #[serde(rename = "ver")]
    pub version: CodecVersion,
    #[serde(rename = "k")]
    pub key: HrBytes,
    #[serde(rename = "v")]
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CodecVersion {
    V1,
    V2,
}
