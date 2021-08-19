// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod hr_datum;
mod hr_key;
mod hr_kv;
mod hr_write;
pub mod rwer;
pub mod sst_to_text;
use serde::{Deserialize, Serialize};

pub fn to_text<T: Serialize>(src: T) -> String {
    serde_json::to_string(&src).unwrap()
}

pub fn from_text<'a, T: Deserialize<'a>>(src: &'a str) -> T {
    serde_json::from_str(src).unwrap()
}
