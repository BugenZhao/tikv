mod br_models;
mod metafile;

use std::{
    borrow::Cow,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};
use backup_text::{decode_key, rwer::TextWriter};
use collections::HashMap;
use engine_rocks::RocksSstReader;
use engine_traits::{name_to_cf, Iterator, SeekKey, SstReader, CF_DEFAULT, CF_WRITE};
use external_storage_export::{create_storage, make_local_backend};
use futures::{future::join_all, AsyncReadExt};
use keys::data_key;
use kvproto::brpb::{BackupMeta, File};
use protobuf::Message;
use structopt::StructOpt;
use tidb_query_datatype::codec::table::decode_table_id;
use tipb::TableInfo;
use tokio::runtime::Runtime;
use txn_types::WriteRef;

use crate::{
    br_models::schema_to_table_info,
    metafile::{read_data_files, read_schemas},
};

const META_FILE: &'static str = "backupmeta";

#[derive(StructOpt)]
struct Opt {
    #[structopt(parse(from_os_str))]
    path: PathBuf,
}

fn rewrite(
    _table_id: i64,
    dir: impl AsRef<Path>,
    file: &File,
    table_info: TableInfo,
) -> Result<()> {
    let start_key = file.get_start_key();
    let end_key = file.get_end_key();

    let path = {
        let mut path = PathBuf::from(dir.as_ref());
        path.push(file.get_name());
        path
    };
    let reader = RocksSstReader::open(path.to_str().unwrap())?;
    reader.verify_checksum()?;

    let mut iter = reader.iter();
    iter.seek(SeekKey::Key(&data_key(start_key)))?;
    // assert_eq!(start_key, decode_key(iter.key()).0); // todo: why not eq?

    let cf = name_to_cf(file.get_cf()).ok_or_else(|| anyhow!("bad cf name"))?;
    let name = format!("{}_rewrite", path.to_str().unwrap());
    let mut writer = TextWriter::new(table_info, cf, &name)?;
    let temp_name = writer.name().to_owned();

    let mut count = 0;
    while iter.valid()? {
        let key = iter.key();
        if decode_key(key).0.as_slice() >= end_key { // todo: really needed?
            break;
        }
        let value = match cf {
            CF_WRITE => {
                let write = WriteRef::parse(iter.value())?;
                Cow::Owned(write.to_bytes())
            }
            CF_DEFAULT => Cow::Borrowed(iter.value()),
            _ => unreachable!(),
        };
        writer.put_line(key, &value)?;

        count += 1;
        iter.next()?;
    }

    if count != file.get_total_kvs() {
        println!(
            "wrong count on {}: count={}, total_kvs={}",
            path.to_str().unwrap(),
            count,
            file.get_total_kvs()
        );
    }

    let reader = writer.finish_read().unwrap();
    drop(reader);

    let file_name = path.to_str().unwrap().replace(".sst", ".rewrite.txt");
    fs::rename(&temp_name, &file_name)?;

    Ok(())
}

async fn worker() {
    let Opt { path } = Opt::from_args();
    let backend = make_local_backend(&path);
    let storage = create_storage(&backend).unwrap();

    let meta = {
        let mut meta_reader = storage.read(META_FILE);
        let mut buf = vec![];
        meta_reader.read_to_end(&mut buf).await.unwrap();
        let mut meta = BackupMeta::new();
        meta.merge_from_bytes(&buf).unwrap();
        meta
    };

    let file_map = {
        let mut map: HashMap<i64, Vec<File>> = Default::default();
        for file in read_data_files(&storage, &meta).await {
            let table_id = decode_table_id(file.get_start_key()).unwrap();
            map.entry(table_id).or_default().push(file);
        }
        map
    };

    let schemas = read_schemas(&storage, &meta).await;

    let table_info_map = schemas
        .into_iter()
        .map(|s| schema_to_table_info(s))
        .map(|ti| (ti.get_table_id(), ti))
        .collect::<HashMap<_, _>>();

    let mut handles = vec![];
    for (table_id, files) in file_map {
        let table_info = table_info_map.get(&table_id).unwrap();
        for file in files {
            let table_info = table_info.clone();
            let dir = path.clone();
            let handle = tokio::task::spawn_blocking(move || {
                rewrite(table_id, dir, &file, table_info)
                    .map_err(|e| anyhow!("failed to rewrite file {}: {}", file.get_name(), e))
                    .unwrap();
            });
            handles.push(handle);
        }
    }
    join_all(handles).await;
}

fn main() {
    let runtime = Runtime::new().unwrap();
    runtime.block_on(worker())
}
