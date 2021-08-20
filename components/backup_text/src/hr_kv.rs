// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{hr_key::HrDataKey, hr_value::HrValue};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrKv {
    #[serde(rename = "k")]
    pub key: HrDataKey,
    #[serde(rename = "v")]
    pub value: HrValue,
}
