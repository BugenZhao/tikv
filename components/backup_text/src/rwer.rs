// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::sst_to_text::{kv_to_text, text_to_kv};
use engine_traits::{CfName, SeekKey, CF_DEFAULT, CF_WRITE};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Lines, Write};
use tipb::TableInfo;

pub struct TextWriter {
    file_writer: BufWriter<File>,
    table_info: TableInfo,
    file_size: usize,
    name: String,
    cf: CfName,
}

impl TextWriter {
    pub fn new(table_info: TableInfo, cf: CfName, name: &str) -> io::Result<TextWriter> {
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .truncate(true)
            .open(name)?;
        let file_writer = BufWriter::new(file);
        Ok(TextWriter {
            file_writer,
            table_info,
            file_size: 0,
            name: name.to_owned(),
            cf,
        })
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
        Ok(OpenOptions::new().read(true).open(&self.name)?)
    }
}

pub struct TextReader {
    lines_reader: Lines<BufReader<File>>,
    table_info: TableInfo,
    cf: String,
}

impl TextReader {
    pub fn new(path: &str, table_info: TableInfo, cf: &str) -> io::Result<TextReader> {
        let lines_reader = BufReader::new(OpenOptions::new().read(true).open(path)?).lines();
        Ok(TextReader {
            lines_reader,
            table_info,
            cf: cf.to_owned(),
        })
    }

    pub fn pop_kv(&mut self) -> io::Result<Option<(Vec<u8>, Vec<u8>)>> {
        if let Some(l) = self.lines_reader.next() {
            let l = l?;
            match self.cf.as_str() {
                CF_DEFAULT => return Ok(Some(text_to_kv(l.as_str(), &self.table_info))),
                CF_WRITE => unimplemented!(),
                _ => unreachable!(),
            }
        }
        Ok(None)
    }

    pub fn seek(&mut self, seek_key: SeekKey) {
        unimplemented!()
    }
}
