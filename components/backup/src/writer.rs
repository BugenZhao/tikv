// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::File as StdFile;
use std::io::{BufReader, Read};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use backup_text::rwer::TextWriter;
use engine_rocks::raw::DB;
use engine_rocks::{RocksEngine, RocksSstWriter, RocksSstWriterBuilder};
use engine_traits::{CfName, CF_DEFAULT, CF_WRITE};
use engine_traits::{ExternalSstFileInfo, SstCompressionType, SstWriter, SstWriterBuilder};
use external_storage_export::ExternalStorage;
use file_system::Sha256Reader;
use futures_util::io::AllowStdIo;
use kvproto::brpb::{File, FileFormat};
use kvproto::metapb::Region;
use protobuf::Message;
use tikv::coprocessor::checksum_crc64_xor;
use tikv::storage::txn::TxnEntry;
use tikv_util::{
    self, box_err, error, info,
    time::{Instant, Limiter},
};
use tipb::TableInfo;
use txn_types::{KvPair, WriteRef};

use crate::metrics::*;
use crate::{backup_file_name, Error, Result};

static FILE_NAME_ID_GEN: AtomicU64 = AtomicU64::new(1);

pub trait LocalWriter {
    type LocalReader: Read + Send + 'static;
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()>;
    fn finish_read(&mut self) -> Result<(u64, Self::LocalReader)>;
    fn file_size(&self) -> Option<u64>;
    fn name_prefix(&self) -> Option<String>;
    fn cleanup(self) -> Result<()>;
}

impl LocalWriter for RocksSstWriter {
    type LocalReader = <RocksSstWriter as SstWriter>::ExternalSstFileReader;

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        <RocksSstWriter as SstWriter>::put(self, key, val)?;
        Ok(())
    }
    fn finish_read(&mut self) -> Result<(u64, Self::LocalReader)> {
        let (sst_info, r) = <RocksSstWriter as SstWriter>::finish_read(self)?;
        Ok((sst_info.file_size(), r))
    }
    fn file_size(&self) -> Option<u64> {
        None
    }
    fn name_prefix(&self) -> Option<String> {
        None
    }
    fn cleanup(self) -> Result<()> {
        Ok(())
    }
}

impl LocalWriter for TextWriter {
    type LocalReader = BufReader<StdFile>;

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        match self.put_line(key, val) {
            Err(e) => {
                error!("TextWriter write failed"; "err" => ?e);
                Err(e.into())
            }
            Ok(()) => Ok(()),
        }
    }

    fn finish_read(&mut self) -> Result<(u64, Self::LocalReader)> {
        let size = self.get_size();
        match self.finish_read() {
            Err(e) => {
                error!("TextWriter get reader failed"; "err" => ?e);
                Err(e.into())
            }
            Ok(r) => Ok((size, r)),
        }
    }

    fn file_size(&self) -> Option<u64> {
        Some(self.get_size())
    }

    fn name_prefix(&self) -> Option<String> {
        self.name_prefix()
    }

    fn cleanup(self) -> Result<()> {
        Ok(self.cleanup()?)
    }
}

struct Writer<LW: LocalWriter> {
    writer: LW,
    total_kvs: u64,
    total_bytes: u64,
    checksum: u64,
    digest: crc64fast::Digest,
}

impl<LW: LocalWriter> Writer<LW> {
    fn new(writer: LW) -> Self {
        Writer {
            writer,
            total_kvs: 0,
            total_bytes: 0,
            checksum: 0,
            digest: crc64fast::Digest::new(),
        }
    }

    fn write(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // HACK: The actual key stored in TiKV is called
        // data_key and always prefix a `z`. But iterator strips
        // it, we need to add the prefix manually.
        let data_key_write = keys::data_key(key);
        self.writer.put(&data_key_write, value)?;
        Ok(())
    }

    fn update_with(&mut self, entry: TxnEntry, need_checksum: bool) -> Result<()> {
        self.total_kvs += 1;
        if need_checksum {
            let (k, v) = entry
                .into_kvpair()
                .map_err(|err| Error::Other(box_err!("Decode error: {:?}", err)))?;
            self.total_bytes += (k.len() + v.len()) as u64;
            self.checksum = checksum_crc64_xor(self.checksum, self.digest.clone(), &k, &v);
        }
        Ok(())
    }

    fn update_raw_with(&mut self, key: &[u8], value: &[u8], need_checksum: bool) -> Result<()> {
        self.total_kvs += 1;
        self.total_bytes += (key.len() + value.len()) as u64;
        if need_checksum {
            self.checksum = checksum_crc64_xor(self.checksum, self.digest.clone(), key, value);
        }
        Ok(())
    }

    fn save_and_build_file(
        mut self,
        file_name: &str,
        cf: &'static str,
        limiter: Limiter,
        storage: &dyn ExternalStorage,
    ) -> Result<File> {
        let (file_size, sst_reader) = self.writer.finish_read()?;
        BACKUP_RANGE_SIZE_HISTOGRAM_VEC
            .with_label_values(&[cf])
            .observe(file_size as f64);

        let (reader, hasher) = Sha256Reader::new(sst_reader)
            .map_err(|e| Error::Other(box_err!("Sha256 error: {:?}", e)))?;
        storage.write(
            &file_name,
            Box::new(limiter.limit(AllowStdIo::new(reader))),
            file_size,
        )?;
        self.writer.cleanup()?;
        let sha256 = hasher
            .lock()
            .unwrap()
            .finish()
            .map(|digest| digest.to_vec())
            .map_err(|e| Error::Other(box_err!("Sha256 error: {:?}", e)))?;

        let mut file = File::default();
        file.set_name(file_name.to_owned());
        file.set_sha256(sha256);
        file.set_crc64xor(self.checksum);
        file.set_total_kvs(self.total_kvs);
        file.set_total_bytes(self.total_bytes);
        file.set_cf(cf.to_owned());
        file.set_size(file_size);
        Ok(file)
    }

    fn is_empty(&self) -> bool {
        match self.writer.file_size() {
            Some(s) => s == 0,
            None => self.total_kvs == 0,
        }
    }
}

pub trait BackupWriterBuilder {
    type LW: LocalWriter;
    fn build(&self, start_key: Vec<u8>) -> Result<BackupWriter<Self::LW>>;
}

pub struct BackupSstWriterBuilder {
    store_id: u64,
    limiter: Limiter,
    region: Region,
    db: Arc<DB>,
    compression_type: Option<SstCompressionType>,
    compression_level: i32,
    sst_max_size: u64,
}

impl BackupSstWriterBuilder {
    pub fn new(
        store_id: u64,
        limiter: Limiter,
        region: Region,
        db: Arc<DB>,
        compression_type: Option<SstCompressionType>,
        compression_level: i32,
        sst_max_size: u64,
    ) -> BackupSstWriterBuilder {
        Self {
            store_id,
            limiter,
            region,
            db,
            compression_type,
            compression_level,
            sst_max_size,
        }
    }
}

impl BackupWriterBuilder for BackupSstWriterBuilder {
    type LW = RocksSstWriter;
    fn build(&self, start_key: Vec<u8>) -> Result<BackupWriter<Self::LW>> {
        let key = file_system::sha256(&start_key).ok().map(hex::encode);
        let store_id = self.store_id;
        let name = backup_file_name(store_id, &self.region, key);
        let default = RocksSstWriterBuilder::new()
            .set_in_memory(true)
            .set_cf(CF_DEFAULT)
            .set_db(RocksEngine::from_ref(&self.db))
            .set_compression_type(self.compression_type)
            .set_compression_level(self.compression_level)
            .build(&name)?;
        let write = RocksSstWriterBuilder::new()
            .set_in_memory(true)
            .set_cf(CF_WRITE)
            .set_db(RocksEngine::from_ref(&self.db))
            .set_compression_type(self.compression_type)
            .set_compression_level(self.compression_level)
            .build(&name)?;
        BackupWriter::<RocksSstWriter>::new(
            &name,
            self.limiter.clone(),
            default,
            write,
            self.sst_max_size,
            FileFormat::Sst,
        )
    }
}

pub struct BackupTextWriterBuilder {
    store_id: u64,
    limiter: Limiter,
    region: Region,
    sst_max_size: u64,
    table_info: TableInfo,
    format: FileFormat,
}

impl BackupTextWriterBuilder {
    pub fn new(
        store_id: u64,
        limiter: Limiter,
        region: Region,
        sst_max_size: u64,
        table_info_data: &[u8],
        format: FileFormat,
    ) -> BackupTextWriterBuilder {
        let mut table_info = TableInfo::default();
        table_info.merge_from_bytes(&table_info_data).unwrap();
        Self {
            store_id,
            limiter,
            region,
            sst_max_size,
            table_info,
            format,
        }
    }
}

impl BackupWriterBuilder for BackupTextWriterBuilder {
    type LW = TextWriter;
    fn build(&self, start_key: Vec<u8>) -> Result<BackupWriter<Self::LW>> {
        let key = file_system::sha256(&start_key).ok().map(hex::encode);
        let store_id = self.store_id;
        let name = if self.format == FileFormat::Csv {
            use std::time::{SystemTime, UNIX_EPOCH};
            let peer = self
                .region
                .get_peers()
                .iter()
                .find(|&p| p.get_store_id() == store_id)
                .unwrap();
            let id = FILE_NAME_ID_GEN.fetch_add(1, Ordering::SeqCst);
            let start = SystemTime::now();
            let since_the_epoch = start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            format!("{}{}{}", id, since_the_epoch.as_millis(), peer.get_id())
        } else {
            backup_file_name(store_id, &self.region, key)
        };
        let (default, write) = (
            TextWriter::new(
                self.table_info.clone(),
                CF_DEFAULT,
                self.format.clone(),
                &name,
                false, // todo: option for compression
            )?,
            TextWriter::new(
                self.table_info.clone(),
                CF_WRITE,
                self.format.clone(),
                &name,
                false, // todo: option for compression
            )?,
        );
        BackupWriter::<TextWriter>::new(
            &name,
            self.limiter.clone(),
            default,
            write,
            self.sst_max_size,
            self.format,
        )
    }
}

fn file_suffix(f: &FileFormat) -> &'static str {
    match f {
        FileFormat::Sst => "sst",
        FileFormat::Text => "txt",
        FileFormat::Csv => "csv",
    }
}

/// A writer writes txn entries into SST files.
pub struct BackupWriter<LW: LocalWriter> {
    name: String,
    default: Writer<LW>,
    write: Writer<LW>,
    limiter: Limiter,
    sst_max_size: u64,
    format: FileFormat,
}

impl<LW: LocalWriter> BackupWriter<LW> {
    /// Create a new BackupWriter.
    pub fn new(
        name: &str,
        limiter: Limiter,
        default: LW,
        write: LW,
        sst_max_size: u64,
        format: FileFormat,
    ) -> Result<BackupWriter<LW>> {
        let name = name.to_owned();
        Ok(BackupWriter {
            name,
            default: Writer::new(default),
            write: Writer::new(write),
            limiter,
            sst_max_size,
            format,
        })
    }

    /// Write entries to buffered files.
    pub fn write<I>(&mut self, entries: I, need_checksum: bool) -> Result<()>
    where
        I: Iterator<Item = TxnEntry>,
    {
        match self.format {
            FileFormat::Sst | FileFormat::Text => self.write_all(entries, need_checksum),
            FileFormat::Csv => self.write_value(entries, need_checksum),
        }
    }

    /// Write entries to buffered SST or Text files.
    pub fn write_all<I>(&mut self, entries: I, need_checksum: bool) -> Result<()>
    where
        I: Iterator<Item = TxnEntry>,
    {
        for e in entries {
            let mut value_in_default = false;
            match &e {
                TxnEntry::Commit { default, write, .. } => {
                    // Default may be empty if value is small.
                    if !default.0.is_empty() {
                        self.default.write(&default.0, &default.1)?;
                        value_in_default = true;
                    }
                    assert!(!write.0.is_empty());
                    self.write.write(&write.0, &write.1)?;
                }
                TxnEntry::Prewrite { .. } => {
                    return Err(Error::Other("prewrite is not supported".into()));
                }
            }
            if value_in_default {
                self.default.update_with(e, need_checksum)?;
            } else {
                self.write.update_with(e, need_checksum)?;
            }
        }
        Ok(())
    }

    /// Write entries to buffered csv files, the csv file only output the actual value
    /// so we can igorn other content
    pub fn write_value<I>(&mut self, entries: I, need_checksum: bool) -> Result<()>
    where
        I: Iterator<Item = TxnEntry>,
    {
        for mut e in entries {
            match &mut e {
                TxnEntry::Commit {
                    ref mut default,
                    ref mut write,
                    ..
                } => {
                    // Write the value to the `default` writer
                    if default.0.is_empty() {
                        // The `ts` in the `write_cf` key is different from the `default_cf` key
                        // but it is okay since the csv writer will igorn it
                        let mut write_ref = WriteRef::parse(&write.1).unwrap();
                        self.default
                            .write(&write.0, write_ref.short_value.take().unwrap())?;
                    } else {
                        self.default.write(&default.0, &default.1)?;
                    }
                }
                TxnEntry::Prewrite { .. } => {
                    return Err(Error::Other("prewrite is not supported".into()));
                }
            }
            self.default.update_with(e, need_checksum)?;
        }
        Ok(())
    }

    /// Save buffered SST files to the given external storage.
    pub fn save(self, storage: &dyn ExternalStorage) -> Result<Vec<File>> {
        let start = Instant::now();
        let mut files = Vec::with_capacity(2);
        let write_written = if self.format != FileFormat::Csv {
            !self.write.is_empty() || !self.default.is_empty()
        } else {
            // We do not write `write_cf` content to csv file
            false
        };
        if !self.default.is_empty() {
            // Save default cf contents.
            let file_name = match self.default.writer.name_prefix() {
                Some(prefix) => format!("{}.{}.{}", prefix, self.name, file_suffix(&self.format)),
                None => format!("{}_{}.{}", self.name, CF_DEFAULT, file_suffix(&self.format)),
            };
            let default = self.default.save_and_build_file(
                &file_name,
                CF_DEFAULT,
                self.limiter.clone(),
                storage,
            )?;
            files.push(default);
        }
        if write_written {
            // Save write cf contents.
            let file_name = format!("{}_{}.{}", self.name, CF_WRITE, file_suffix(&self.format));
            let write = self.write.save_and_build_file(
                &file_name,
                CF_WRITE,
                self.limiter.clone(),
                storage,
            )?;
            files.push(write);
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["save"])
            .observe(start.saturating_elapsed().as_secs_f64());
        Ok(files)
    }

    pub fn need_split_keys(&self) -> bool {
        self.default.total_bytes + self.write.total_bytes >= self.sst_max_size
    }

    pub fn need_flush_keys(&self) -> bool {
        self.default.total_bytes + self.write.total_bytes > 0
    }
}

/// A writer writes Raw kv into SST files.
pub struct BackupRawKVWriter {
    name: String,
    cf: CfName,
    writer: Writer<RocksSstWriter>,
    limiter: Limiter,
}

impl BackupRawKVWriter {
    /// Create a new BackupRawKVWriter.
    pub fn new(
        db: Arc<DB>,
        name: &str,
        cf: CfName,
        limiter: Limiter,
        compression_type: Option<SstCompressionType>,
        compression_level: i32,
    ) -> Result<BackupRawKVWriter> {
        let writer = RocksSstWriterBuilder::new()
            .set_in_memory(true)
            .set_cf(cf)
            .set_db(RocksEngine::from_ref(&db))
            .set_compression_type(compression_type)
            .set_compression_level(compression_level)
            .build(name)?;
        Ok(BackupRawKVWriter {
            name: name.to_owned(),
            cf,
            writer: Writer::new(writer),
            limiter,
        })
    }

    /// Write Kv_pair to buffered SST files.
    pub fn write<I>(&mut self, kv_pairs: I, need_checksum: bool) -> Result<()>
    where
        I: Iterator<Item = Result<KvPair>>,
    {
        for kv_pair in kv_pairs {
            let (k, v) = match kv_pair {
                Ok(s) => s,
                Err(e) => {
                    error!("write raw kv"; "error" => ?e);
                    return Err(Error::Other("occur an error when written raw kv".into()));
                }
            };

            assert!(!k.is_empty());
            self.writer.write(&k, &v)?;
            self.writer.update_raw_with(&k, &v, need_checksum)?;
        }
        Ok(())
    }

    /// Save buffered SST files to the given external storage.
    pub fn save(self, storage: &dyn ExternalStorage) -> Result<Vec<File>> {
        let start = Instant::now();
        let mut files = Vec::with_capacity(1);
        if !self.writer.is_empty() {
            let file_name = format!("{}_{}.sst", self.name, self.cf);
            let file = self.writer.save_and_build_file(
                &file_name,
                self.cf,
                self.limiter.clone(),
                storage,
            )?;
            files.push(file);
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["save_raw"])
            .observe(start.saturating_elapsed().as_secs_f64());
        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_traits::Iterable;
    use raftstore::store::util::new_peer;
    use std::collections::BTreeMap;
    use std::f64::INFINITY;
    use std::path::Path;
    use tempfile::TempDir;
    use tikv::storage::TestEngineBuilder;

    type CfKvs<'a> = (engine_traits::CfName, &'a [(&'a [u8], &'a [u8])]);

    fn check_sst(ssts: &[(engine_traits::CfName, &Path)], kvs: &[CfKvs]) {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .cfs(&[engine_traits::CF_DEFAULT, engine_traits::CF_WRITE])
            .build()
            .unwrap();
        let db = rocks.get_rocksdb();

        let opt = engine_rocks::raw::IngestExternalFileOptions::new();
        for (cf, sst) in ssts {
            let handle = db.as_inner().cf_handle(cf).unwrap();
            db.as_inner()
                .ingest_external_file_cf(handle, &opt, &[sst.to_str().unwrap()])
                .unwrap();
        }
        for (cf, kv) in kvs {
            let mut map = BTreeMap::new();
            db.scan_cf(
                cf,
                keys::DATA_MIN_KEY,
                keys::DATA_MAX_KEY,
                false,
                |key, value| {
                    map.insert(key.to_owned(), value.to_owned());
                    Ok(true)
                },
            )
            .unwrap();
            assert_eq!(map.len(), kv.len(), "{} {:?} {:?}", cf, map, kv);
            for (k, v) in *kv {
                assert_eq!(&v.to_vec(), map.get(&k.to_vec()).unwrap());
            }
        }
    }

    #[test]
    fn test_writer() {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .cfs(&[
                engine_traits::CF_DEFAULT,
                engine_traits::CF_LOCK,
                engine_traits::CF_WRITE,
            ])
            .build()
            .unwrap();
        let db = rocks.get_rocksdb();
        let backend = external_storage_export::make_local_backend(temp.path());
        let storage = external_storage_export::create_storage(&backend).unwrap();

        // Test empty file.
        let mut r = kvproto::metapb::Region::default();
        r.set_id(1);
        r.mut_peers().push(new_peer(1, 1));

        let writer_builder = BackupSstWriterBuilder::new(
            1,
            Limiter::new(INFINITY),
            r,
            db.get_sync_db(),
            None,
            0,
            144 * 1024 * 1024,
        );
        let mut writer = writer_builder.build(b"foo".to_vec()).unwrap();
        writer.write(vec![].into_iter(), false).unwrap();
        assert!(writer.save(&storage).unwrap().is_empty());

        // Test write only txn.
        let mut writer = writer_builder.build(b"foo1".to_vec()).unwrap();
        writer
            .write(
                vec![TxnEntry::Commit {
                    default: (vec![], vec![]),
                    write: (vec![b'a'], vec![b'a']),
                    old_value: None,
                }]
                .into_iter(),
                false,
            )
            .unwrap();
        let files = writer.save(&storage).unwrap();
        assert_eq!(files.len(), 1);
        check_sst(
            &[(
                engine_traits::CF_WRITE,
                &temp.path().join(files[0].get_name()),
            )],
            &[(
                engine_traits::CF_WRITE,
                &[(&keys::data_key(&[b'a']), &[b'a'])],
            )],
        );

        // Test write and default.
        let mut writer = writer_builder.build(b"foo2".to_vec()).unwrap();
        writer
            .write(
                vec![
                    TxnEntry::Commit {
                        default: (vec![b'a'], vec![b'a']),
                        write: (vec![b'a'], vec![b'a']),
                        old_value: None,
                    },
                    TxnEntry::Commit {
                        default: (vec![], vec![]),
                        write: (vec![b'b'], vec![]),
                        old_value: None,
                    },
                ]
                .into_iter(),
                false,
            )
            .unwrap();
        let files = writer.save(&storage).unwrap();
        assert_eq!(files.len(), 2);
        check_sst(
            &[
                (
                    engine_traits::CF_DEFAULT,
                    &temp.path().join(files[0].get_name()),
                ),
                (
                    engine_traits::CF_WRITE,
                    &temp.path().join(files[1].get_name()),
                ),
            ],
            &[
                (
                    engine_traits::CF_DEFAULT,
                    &[(&keys::data_key(&[b'a']), &[b'a'])],
                ),
                (
                    engine_traits::CF_WRITE,
                    &[
                        (&keys::data_key(&[b'a']), &[b'a']),
                        (&keys::data_key(&[b'b']), &[]),
                    ],
                ),
            ],
        );
    }
}
