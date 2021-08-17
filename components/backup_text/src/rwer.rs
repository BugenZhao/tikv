// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::sst_to_text::{kv_to_text, kv_to_write, text_to_kv, write_to_kv};
use engine_traits::{CfName, SeekKey, CF_DEFAULT, CF_WRITE};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Lines, Write};
use tikv_util::error;
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
        let name = format!("{}_{}", name, cf);
        let file = match OpenOptions::new()
            .write(true)
            .truncate(true)
            .create_new(true)
            .open(&name)
        {
            Ok(f) => f,
            Err(e) => {
                error!("failed to open file"; "err" => ?e, "path" => name);
                return Err(e);
            }
        };
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
            CF_DEFAULT => kv_to_text(key, val, &self.table_info).unwrap(),
            CF_WRITE => kv_to_write(key, val),
            _ => unreachable!(),
        };
        s.push('\n');
        self.file_size += self.file_writer.write(s.as_bytes())?;
        Ok(())
    }

    pub fn finish_read(&mut self) -> io::Result<BufReader<File>> {
        self.file_writer.flush()?;
        Ok(BufReader::new(
            OpenOptions::new().read(true).open(&self.name)?,
        ))
    }

    pub fn cleanup(self) -> io::Result<()> {
        Ok(fs::remove_file(&self.name)?)
    }
}

pub struct TextReader {
    lines_reader: Lines<BufReader<File>>,
    next_kv: Option<(Vec<u8>, Vec<u8>)>,
    table_info: TableInfo,
    cf: String,
}

impl TextReader {
    pub fn new(path: &str, table_info: TableInfo, cf: &str) -> io::Result<TextReader> {
        let lines_reader = match OpenOptions::new().read(true).open(path) {
            Ok(f) => BufReader::new(f).lines(),
            Err(e) => {
                error!("failed to open file"; "err" => ?e, "path" => path);
                return Err(e);
            }
        };
        Ok(TextReader {
            lines_reader,
            next_kv: None,
            table_info,
            cf: cf.to_owned(),
        })
    }

    pub fn new_start_at(
        path: &str,
        table_info: TableInfo,
        cf: &str,
        seek_key: SeekKey,
    ) -> io::Result<TextReader> {
        let mut text_reader = TextReader::new(path, table_info, cf)?;
        match seek_key {
            SeekKey::Start => return Ok(text_reader),
            SeekKey::Key(sk) => {
                while let Some((k, v)) = text_reader.pop_kv()? {
                    if k.as_slice() >= sk {
                        text_reader.next_kv = Some((k, v));
                        break;
                    }
                }
                if text_reader.next_kv.is_none() {
                    // reach end
                }
                return Ok(text_reader);
            }
            SeekKey::End => unreachable!(),
        }
    }

    pub fn pop_kv(&mut self) -> io::Result<Option<(Vec<u8>, Vec<u8>)>> {
        if let Some(kv) = self.next_kv.take() {
            return Ok(Some(kv));
        }
        if let Some(l) = self.lines_reader.next() {
            let l = match l {
                Err(e) => {
                    error!("TextReader pop_kv met error"; "err" => ?e);
                    return Err(e);
                }
                Ok(l) => l,
            };
            let res = match self.cf.as_str() {
                CF_DEFAULT => text_to_kv(l.as_str(), &self.table_info),
                CF_WRITE => write_to_kv(l.as_str()),
                _ => unreachable!(),
            };
            return Ok(Some(res));
        }
        Ok(None)
    }
}
