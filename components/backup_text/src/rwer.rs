// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::sst_to_text::{kv_to_text, text_to_kv};
use engine_traits::{CfName, SeekKey, CF_DEFAULT, CF_WRITE};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Lines, Read, Write};
use tipb::TableInfo;

pub struct TextWriter {
    file: File,
    file_writer: BufWriter<File>,
    table_info: TableInfo,
    file_size: usize,
    cf: CfName,
}

impl TextWriter {
    pub fn new(table_info: TableInfo, cf: CfName) -> TextWriter {
        unimplemented!()
    }

    pub fn get_size(&self) -> u64 {
        self.file_size as u64
    }

    pub fn put_line(&mut self, key: &[u8], val: &[u8]) -> io::Result<()> {
        let mut s = match self.cf {
            CF_DEFAULT => kv_to_text(key, val, &self.table_info),
            CF_WRITE => unimplemented!(),
            _ => unreachable!(),
        };
        s.push('\n');
        self.file_size += self.file_writer.write(s.as_bytes())?;
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<File> {
        self.file_writer.flush()?;
        Ok(self.file)
    }
}

pub struct TextReader {
    file: File,
    lines: Lines<BufReader<File>>,
    table_info: TableInfo,
    cf: CfName,
}

impl TextReader {
    pub fn new(table_info: TableInfo, cf: &str) -> TextReader {
        unimplemented!()
    }

    pub fn pop_kv(&mut self) -> io::Result<Option<(Vec<u8>, Vec<u8>)>> {
        if let Some(l) = self.lines.next() {
            let l = l?;
            match self.cf {
                CF_DEFAULT => return Ok(Some(text_to_kv(l.as_str(), &self.table_info))),
                CF_WRITE => unimplemented!(),
                _ => unreachable!(),
            }
        }
        Ok(None)
    }

    pub fn verify_checksum(&self) -> bool {
        unimplemented!()
    }

    pub fn seek(&mut self, seek_key: SeekKey) {
        unimplemented!()
    }
}
