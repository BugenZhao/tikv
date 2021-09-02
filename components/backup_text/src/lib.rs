// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod hr_datum;
mod hr_index;
mod hr_key;
mod hr_kv;
mod hr_value;
mod hr_write;
pub mod rwer;
pub mod sst_to_text;

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tidb_query_datatype::expr::{EncodingFlag, EvalConfig, EvalContext};

pub use hr_key::decode_key;

pub type Error = Box<dyn std::error::Error>;
pub type Result<T> = std::result::Result<T, Error>;

pub fn to_text<T: Serialize>(src: T) -> String {
    serde_json::to_string(&src).unwrap()
}

pub fn from_text<'a, T: Deserialize<'a>>(src: &'a str) -> T {
    serde_json::from_str(src).unwrap()
}

pub fn eval_context() -> EvalContext {
    let mut cfg = EvalConfig::default();
    cfg.encoding_flag
        .set(EncodingFlag::DECIMAL_PREFERRED_PREC_FRAC, true);
    EvalContext::new(Arc::new(cfg))
}

#[inline]
pub(crate) const fn is_false(v: &bool) -> bool {
    !(*v)
}
