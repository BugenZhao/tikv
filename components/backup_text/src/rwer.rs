// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::eval_context;
use crate::sst_to_text::{
    index_kv_to_text, index_kv_to_write, index_text_to_kv, index_write_to_kv, kv_to_text,
    kv_to_write, text_to_kv, write_to_kv,
};
use crate::{Error, Result};
use collections::HashMap;
use engine_traits::{CfName, SeekKey, CF_DEFAULT, CF_WRITE};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Lines, Write};
use std::str::FromStr;
use std::string::ToString;
use tidb_query_datatype::codec::table::{check_index_key, check_record_key};
use tidb_query_datatype::expr::EvalContext;
use tikv_util::error;
use tipb::{ColumnInfo, TableInfo};
use txn_types::Key;

pub struct TextWriter {
    ctx: EvalContext,
    data_type: Option<DataType>,
    file_writer: BufWriter<File>,
    #[allow(dead_code)]
    table_info: TableInfo,
    columns: HashMap<i64, ColumnInfo>,
    file_size: usize,
    name: String,
    cf: CfName,
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

impl ToString for DataType {
    fn to_string(&self) -> String {
        match self {
            DataType::Record => "DataType::Record".to_owned(),
            DataType::Index => "DataType::Index".to_owned(),
        }
    }
}

impl FromStr for DataType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        if s.contains("DataType::Record") {
            Ok(DataType::Record)
        } else if s.contains("DataType::Index") {
            Ok(DataType::Index)
        } else {
            Err(format!("unknown data type, s: {:?}", s).into())
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
        let columns = table_to_columns(&table_info);
        Ok(TextWriter {
            ctx: eval_context(),
            data_type: None,
            file_writer,
            table_info,
            columns,
            file_size: 0,
            name: name.to_owned(),
            cf,
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
            let dt = DataType::new(&raw_key).unwrap();
            self.file_size += self
                .file_writer
                .write(format!("{}\n", dt.to_string()).as_bytes())?;
            self.data_type = Some(dt);
        }
        let mut s = match (self.cf, self.data_type.as_ref().unwrap()) {
            (CF_DEFAULT, DataType::Record) => {
                kv_to_text(&mut self.ctx, key, val, &self.columns).unwrap()
            }
            (CF_DEFAULT, DataType::Index) => {
                index_kv_to_text(&mut self.ctx, key, val, &self.columns).unwrap()
            }
            (CF_WRITE, DataType::Record) => kv_to_write(&mut self.ctx, key, val, &self.columns),
            (CF_WRITE, DataType::Index) => {
                index_kv_to_write(&mut self.ctx, key, val, &self.columns)
            }
            _ => unreachable!(),
        };
        s.push('\n');
        self.file_size += self.file_writer.write(s.as_bytes())?;
        Ok(())
    }

    pub fn finish(&mut self) -> io::Result<()> {
        self.file_writer.flush()
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

    /// Get a reference to the text writer's name.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

pub struct TextReader {
    ctx: EvalContext,
    data_type: DataType,
    lines_reader: Lines<BufReader<File>>,
    next_kv: Option<(Vec<u8>, Vec<u8>)>,
    #[allow(dead_code)]
    table_info: TableInfo,
    columns: HashMap<i64, ColumnInfo>,
    cf: String,
}

impl TextReader {
    pub fn new(path: &str, table_info: TableInfo, cf: &str) -> io::Result<TextReader> {
        let mut lines_reader = match OpenOptions::new().read(true).open(path) {
            Ok(f) => BufReader::new(f).lines(),
            Err(e) => {
                error!("failed to open file"; "err" => ?e, "path" => path);
                return Err(e);
            }
        };
        let data_type = match lines_reader.next() {
            Some(l) => DataType::from_str((l?).as_str()).expect("backup text file corrupted"),
            None => panic!("backup text file corrupted"),
        };
        let columns = table_to_columns(&table_info);
        Ok(TextReader {
            ctx: eval_context(),
            data_type,
            lines_reader,
            next_kv: None,
            table_info,
            columns,
            cf: cf.to_owned(),
        })
    }

    pub fn new_start_at(
        path: &str,
        _table_info: TableInfo,
        cf: &str,
        seek_key: SeekKey,
    ) -> io::Result<TextReader> {
        let mut text_reader = TextReader::new(path, _table_info, cf)?;
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
                (CF_DEFAULT, DataType::Record) => {
                    text_to_kv(&mut self.ctx, l.as_str(), &self.columns)
                }
                (CF_DEFAULT, DataType::Index) => {
                    index_text_to_kv(&mut self.ctx, l.as_str(), &self.columns)
                }
                (CF_WRITE, DataType::Record) => write_to_kv(&mut self.ctx, l.as_str()),
                (CF_WRITE, DataType::Index) => index_write_to_kv(&mut self.ctx, l.as_str()),
                _ => unreachable!(),
            };
            return Ok(Some(res));
        }
        Ok(None)
    }
}

fn table_to_columns(table: &TableInfo) -> HashMap<i64, ColumnInfo> {
    let columns_info = table.get_columns();
    columns_info
        .iter()
        .map(|ci| (ci.get_column_id(), ci.clone()))
        .collect()
}
