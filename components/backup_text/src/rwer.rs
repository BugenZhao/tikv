// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::sst_to_text::{
    index_kv_to_text, index_kv_to_write, index_text_to_kv, index_write_to_kv, kv_to_text,
    kv_to_write, text_to_kv, write_to_kv,
};
use crate::Result;
use engine_traits::{CfName, SeekKey, CF_DEFAULT, CF_WRITE};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Lines, Write};
use tidb_query_datatype::codec::table::{check_index_key, check_record_key};
use tikv_util::error;
use tipb::TableInfo;
use txn_types::Key;

pub struct TextWriter {
    file_writer: BufWriter<File>,
    table_info: TableInfo,
    file_size: usize,
    name: String,
    cf: CfName,
    data_type: Option<DataType>,
}

enum DataType {
    Record,
    Index,
}

impl DataType {
    fn new(key: &[u8]) -> Result<DataType> {
        if check_record_key(key).is_ok() {
            Ok(DataType::Record)
        } else if check_index_key(key).is_ok() {
            Ok(DataType::Index)
        } else {
            Err(format!("unknown data type, key: {:?}", key).into())
        }
    }
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
            data_type: None,
        })
    }

    pub fn get_size(&self) -> u64 {
        self.file_size as u64
    }

    pub fn put_line(&mut self, key: &[u8], val: &[u8]) -> io::Result<()> {
        if self.data_type.is_none() {
            let origin_encoded_key = keys::origin_key(key);
            let (user_key, _) = Key::split_on_ts_for(origin_encoded_key).unwrap();
            let raw_key = Key::from_encoded_slice(user_key).into_raw().unwrap();
            self.data_type = Some(DataType::new(&raw_key).unwrap());
        }
        let mut s = match (self.cf, self.data_type.as_ref().unwrap()) {
            (CF_DEFAULT, DataType::Record) => kv_to_text(key, val, &self.table_info).unwrap(),
            (CF_DEFAULT, DataType::Index) => index_kv_to_text(key, val, &self.table_info).unwrap(),
            (CF_WRITE, DataType::Record) => kv_to_write(key, val),
            (CF_WRITE, DataType::Index) => index_kv_to_write(key, val),
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
    data_type: DataType,
}

impl TextReader {
    pub fn new(
        path: &str,
        table_info: TableInfo,
        cf: &str,
        key_prefix: &[u8],
    ) -> io::Result<TextReader> {
        let data_type = DataType::new(key_prefix).unwrap();
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
            data_type,
        })
    }

    pub fn new_start_at(
        path: &str,
        table_info: TableInfo,
        cf: &str,
        seek_key: SeekKey,
        key_prefix: &[u8],
    ) -> io::Result<TextReader> {
        let mut text_reader = TextReader::new(path, table_info, cf, key_prefix)?;
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
            let res = match (self.cf.as_str(), &self.data_type) {
                (CF_DEFAULT, DataType::Record) => text_to_kv(l.as_str(), &self.table_info),
                (CF_DEFAULT, DataType::Index) => index_text_to_kv(l.as_str(), &self.table_info),
                (CF_WRITE, DataType::Record) => write_to_kv(l.as_str()),
                (CF_WRITE, DataType::Index) => index_write_to_kv(l.as_str()),
                _ => unreachable!(),
            };
            return Ok(Some(res));
        }
        Ok(None)
    }
}