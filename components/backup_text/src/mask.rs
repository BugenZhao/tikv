// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryInto;

use tidb_query_datatype::{codec::Datum, FieldTypeAccessor, FieldTypeTp};

pub type MaskResult = Result<Datum, Datum>;

const DEFAULT_CONTEXT: &'static str = "tidb";

fn new_hasher() -> blake3::Hasher {
    blake3::Hasher::new_derive_key(DEFAULT_CONTEXT)
}

fn resize_i64(i: i64, field_type: Option<&dyn FieldTypeAccessor>) -> i64 {
    let tp = field_type.map_or(FieldTypeTp::Unspecified, |ft| ft.tp());
    match tp {
        FieldTypeTp::Tiny => i % (i8::MAX as i64 + 1),
        FieldTypeTp::Short => i % (i16::MAX as i64 + 1),
        FieldTypeTp::Int24 => i % (1 << 23),
        FieldTypeTp::Long => i % (i32::MAX as i64 + 1),
        FieldTypeTp::LongLong => i,
        _ => i,
    }
}

fn resize_u64(u: u64, field_type: Option<&dyn FieldTypeAccessor>) -> u64 {
    let tp = field_type.map_or(FieldTypeTp::Unspecified, |ft| ft.tp());
    match tp {
        FieldTypeTp::Tiny => u % (u8::MAX as u64 + 1),
        FieldTypeTp::Short => u % (u16::MAX as u64 + 1),
        FieldTypeTp::Int24 => u % (1 << 24),
        FieldTypeTp::Long => u % (u32::MAX as u64 + 1),
        FieldTypeTp::LongLong => u,
        _ => u,
    }
}

fn hash_bytes(bytes: &mut [u8]) -> &[u8] {
    let mut hasher = new_hasher();
    hasher.update(bytes);
    let mut reader = hasher.finalize_xof();
    reader.fill(bytes);
    bytes
}

#[inline]
fn hash_i64(i: i64) -> i64 {
    i64::from_le_bytes(hash_bytes(&mut i.to_le_bytes()).try_into().unwrap())
}

#[inline]
fn hash_f64(f: f64) -> f64 {
    f64::from_le_bytes(hash_bytes(&mut f.to_le_bytes()).try_into().unwrap())
}

fn hash_string(bytes: &mut [u8]) -> Vec<u8> {
    let size = bytes.len();
    let sum = hash_bytes(bytes);
    let mut hex = hex::encode(sum);
    hex.push_str(&"*".repeat(size - hex.len()));
    hex.into_bytes()
}

pub fn workload_sim_mask(
    mut datum: Datum,
    field_type: Option<&dyn FieldTypeAccessor>,
) -> MaskResult {
    match &mut datum {
        Datum::Null => {}
        Datum::Min => {}
        Datum::Max => {}
        Datum::I64(i) => *i = resize_i64(hash_i64(*i), field_type),
        Datum::U64(u) => *u = resize_u64(hash_i64(*u as i64) as u64, field_type),
        Datum::F64(f) => *f = hash_f64(*f),
        Datum::Bytes(bytes) => *bytes = hash_string(bytes).to_vec(),
        Datum::Dur(_)
        | Datum::Dec(_)
        | Datum::Time(_)
        | Datum::Json(_)
        | Datum::Enum(_)
        | Datum::Set(_) => return Err(datum),
    }
    Ok(datum)
}
