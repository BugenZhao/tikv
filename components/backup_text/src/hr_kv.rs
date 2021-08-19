// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::hr_key::HrDataKey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrKv {
    #[serde(rename = "V")]
    pub version: CodecVersion,
    #[serde(rename = "k")]
    pub key: HrDataKey,
    #[serde(rename = "v")]
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CodecVersion {
    V1,
    V2,
}
