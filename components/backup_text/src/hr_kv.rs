// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use crate::{hr_datum::HrDatum, hr_key::HrDataKey};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrKv {
    #[serde(rename = "k")]
    pub key: HrDataKey,
    #[serde(rename = "v")]
    pub value: HashMap<i64, HrDatum>,
}

impl HrKv {
    pub fn to_text(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn from_text(text: impl AsRef<str>) -> Self {
        serde_json::from_str(text.as_ref()).unwrap()
    }
}
