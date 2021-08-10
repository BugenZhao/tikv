// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use tipb::TableInfo;

pub fn kv_to_text(key: &[u8], val: &[u8], table: &TableInfo) -> String {
    unimplemented!()
}

pub fn text_to_kv(line: &str, table: &TableInfo) -> (Vec<u8>, Vec<u8>) {
    unimplemented!()
}
