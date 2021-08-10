use crate::hr_datum::{HrBytes, HrDatum};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrKv {
    key: HrBytes,
    value: Vec<HrDatum>,
}

impl HrKv {
    pub fn to_text(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn from_text(text: impl AsRef<str>) -> Self {
        serde_json::from_str(text.as_ref()).unwrap()
    }
}
