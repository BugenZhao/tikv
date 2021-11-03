// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::eval_context;
use crate::sst_to_text::{
    index_kv_to_text, index_kv_to_write, index_text_to_kv, index_write_to_kv, kv_to_csv,
    kv_to_csv_write, kv_to_text, kv_to_write, text_to_kv, write_to_kv,
};
use crate::{Error, Result};
use collections::HashMap;
use engine_traits::{CfName, SeekKey, CF_DEFAULT, CF_WRITE};
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use kvproto::brpb::FileFormat;
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
    format: FileFormat,
    file_writer: Option<Box<dyn Write>>,
    schema: Schema,
    name: String,
    cf: CfName,
}

pub struct Schema {
    pub name: String,
    pub columns: HashMap<i64, ColumnInfo>,
    // `column_ids` preserve the order between each column
    pub column_ids: Vec<i64>,
    pub primary_handle: Option<i64>,
    pub common_handle: Vec<i64>,
}

impl Schema {
    fn new(mut table_info: TableInfo) -> Schema {
        let mut columns = HashMap::default();
        let mut column_ids = Vec::with_capacity(table_info.get_columns().len());
        let mut primary_handle = None;
        for ci in table_info.take_columns().into_iter() {
            let id = ci.get_column_id();
            if ci.get_pk_handle() {
                assert!(primary_handle.replace(id).is_none());
            }
            column_ids.push(id);
            columns.insert(id, ci);
        }
        Schema {
            name: table_info.take_name(),
            columns,
            column_ids,
            primary_handle,
            common_handle: table_info.take_common_handles(),
        }
    }

    pub fn handle_in_key(&self) -> bool {
        self.primary_handle.is_some() || !self.common_handle.is_empty()
    }
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
    pub fn new(
        table_info: TableInfo,
        cf: CfName,
        format: FileFormat,
        name: &str,
        compression_level: Option<u32>,
    ) -> io::Result<TextWriter> {
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
        let buf_writer = BufWriter::new(file);
        let file_writer: Box<dyn Write> = if let Some(level) = compression_level {
            Box::new(ZlibEncoder::new(buf_writer, Compression::new(level)))
        } else {
            Box::new(buf_writer)
        };
        Ok(TextWriter {
            ctx: eval_context(),
            data_type: None,
            format,
            file_writer: Some(file_writer),
            schema: Schema::new(table_info),
            name: name.to_owned(),
            cf,
        })
    }

    /// The value may be inaccurate unless `finish` is called
    pub fn get_size(&self) -> u64 {
        fs::metadata(&self.name)
            .map(|m| m.len())
            .unwrap_or_default()
    }

    pub fn put_line(&mut self, key: &[u8], val: &[u8]) -> io::Result<()> {
        let data_type = {
            if self.data_type.is_none() {
                self.init_data_type(key)?;
            }
            self.data_type.as_ref().unwrap()
        };
        // TODO: make `mask` as an option
        let mask = true;
        let mut v = match self.format {
            FileFormat::Text => match (self.cf, data_type) {
                (CF_DEFAULT, DataType::Record) => {
                    kv_to_text(&mut self.ctx, key, val, &self.schema.columns, mask)
                        .unwrap()
                        .into_bytes()
                }
                (CF_DEFAULT, DataType::Index) => {
                    index_kv_to_text(&mut self.ctx, key, val, &self.schema.columns, mask)
                        .unwrap()
                        .into_bytes()
                }
                (CF_WRITE, DataType::Record) => {
                    kv_to_write(&mut self.ctx, key, val, &self.schema.columns, mask).into_bytes()
                }
                (CF_WRITE, DataType::Index) => {
                    index_kv_to_write(&mut self.ctx, key, val, &self.schema.columns, mask)
                        .into_bytes()
                }
                _ => unreachable!(),
            },
            FileFormat::Csv => match (self.cf, data_type) {
                (CF_DEFAULT, DataType::Record) => {
                    kv_to_csv(&mut self.ctx, &self.schema, key, val, mask).unwrap()
                }
                (CF_WRITE, DataType::Record) => {
                    if let Some(result) =
                        kv_to_csv_write(&mut self.ctx, &self.schema, key, val, mask)
                    {
                        result.unwrap()
                    } else {
                        return Ok(());
                    }
                }
                (CF_DEFAULT | CF_WRITE, &DataType::Index) => {
                    // No need to write `write_cf` and index data to csv file
                    return Ok(());
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };

        v.push(b'\n');
        self.writer().write_all(&v)?;

        Ok(())
    }

    pub fn init_data_type(&mut self, key: &[u8]) -> io::Result<()> {
        let origin_encoded_key = keys::origin_key(key);
        let (user_key, _) = Key::split_on_ts_for(origin_encoded_key).unwrap();
        let raw_key = Key::from_encoded_slice(user_key).into_raw().unwrap();
        let dt = DataType::new(&raw_key).unwrap();
        if self.format != FileFormat::Csv {
            // Write the data type header for text file
            self.writer()
                .write_fmt(format_args!("{}\n", dt.to_string()))?;
        }
        self.data_type = Some(dt);
        Ok(())
    }

    pub fn finish(&mut self) -> io::Result<u64> {
        self.close_writer()?;
        Ok(self.get_size())
    }

    pub fn finish_read(&mut self) -> io::Result<(u64, BufReader<File>)> {
        self.close_writer()?;
        Ok((
            self.get_size(),
            BufReader::new(OpenOptions::new().read(true).open(&self.name)?),
        ))
    }

    pub fn cleanup(self) -> io::Result<()> {
        Ok(fs::remove_file(&self.name)?)
    }

    /// Get a reference to the text writer's name.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn name_prefix(&self) -> Option<String> {
        match self.format {
            FileFormat::Text => None,
            FileFormat::Csv => Some(self.schema.name.clone()),
            _ => unreachable!(),
        }
    }

    /// Get a mutable reference to the text writer's file writer.
    #[inline]
    fn writer(&mut self) -> &mut Box<dyn Write> {
        self.file_writer.as_mut().expect("writer has been closed")
    }

    fn close_writer(&mut self) -> io::Result<()> {
        let mut w = self.file_writer.take().expect("writer has been closed");
        w.flush()
        // writer is then dropped here
    }
}

pub struct TextReader {
    ctx: EvalContext,
    data_type: DataType,
    lines_reader: Lines<Box<dyn BufRead>>,
    next_kv: Option<(Vec<u8>, Vec<u8>)>,
    cf: String,
}

impl TextReader {
    pub fn new(
        path: &str,
        _table_info: TableInfo,
        cf: &str,
        compressed: bool,
    ) -> io::Result<TextReader> {
        let reader: Box<dyn BufRead> = match OpenOptions::new().read(true).open(path) {
            Ok(f) => {
                if compressed {
                    Box::new(BufReader::new(ZlibDecoder::new(f)))
                } else {
                    Box::new(BufReader::new(f))
                }
            }
            Err(e) => {
                error!("failed to open file"; "err" => ?e, "path" => path);
                return Err(e);
            }
        };
        let mut lines_reader = reader.lines();
        let data_type = match lines_reader.next() {
            Some(l) => DataType::from_str((l?).as_str()).expect("backup text file corrupted"),
            None => panic!("backup text file corrupted"),
        };
        Ok(TextReader {
            ctx: eval_context(),
            data_type,
            lines_reader,
            next_kv: None,
            cf: cf.to_owned(),
        })
    }

    pub fn new_start_at(
        path: &str,
        _table_info: TableInfo,
        cf: &str,
        seek_key: SeekKey,
        compressed: bool,
    ) -> io::Result<TextReader> {
        let mut text_reader = TextReader::new(path, _table_info, cf, compressed)?;
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
                (CF_DEFAULT, DataType::Record) => text_to_kv(&mut self.ctx, l.as_str()),
                (CF_DEFAULT, DataType::Index) => index_text_to_kv(&mut self.ctx, l.as_str()),
                (CF_WRITE, DataType::Record) => write_to_kv(&mut self.ctx, l.as_str()),
                (CF_WRITE, DataType::Index) => index_write_to_kv(&mut self.ctx, l.as_str()),
                _ => unreachable!(),
            };
            return Ok(Some(res));
        }
        Ok(None)
    }
}
