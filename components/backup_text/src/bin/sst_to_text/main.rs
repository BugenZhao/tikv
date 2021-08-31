mod br_models;
mod metafile;

use std::{
    borrow::Cow,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};
use backup_text::rwer::TextWriter;
use collections::HashMap;
use engine_rocks::RocksSstReader;
use engine_traits::{name_to_cf, Iterator, SeekKey, SstReader, CF_DEFAULT, CF_WRITE};
use external_storage_export::{create_storage, make_local_backend};
use futures::{future::join_all, AsyncReadExt, FutureExt};
use kvproto::brpb::{BackupMeta, File};
use protobuf::Message;
use slog::Drain;
use slog_global::{info, warn};
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
    let path = {
        let mut path = PathBuf::from(dir.as_ref());
        path.push(file.get_name());
        path
    };
    let path_str = path.to_str().unwrap();
    let reader = RocksSstReader::open(path_str)?;
    reader.verify_checksum()?;

    let mut iter = reader.iter();
    iter.seek(SeekKey::Start)?; // ignore start_key in file

    let cf = name_to_cf(file.get_cf()).ok_or_else(|| anyhow!("bad cf name"))?;
    let name = format!("{}_rewrite", path_str);
    let mut writer = TextWriter::new(table_info, cf, &name)?;
    let temp_name = writer.name().to_owned();

    let mut count = 0;
    while iter.valid()? {
        let key = iter.key();
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
        warn!(
            "kv pairs count mismatched";
            "file" => path_str,
            "count" => count,
            "expected" => file.get_total_kvs()
        );
    }

    let reader = writer.finish_read()?;
    drop(reader);

    let file_name = path_str.replace(".sst", ".rewrite.txt");
    fs::rename(&temp_name, &file_name)?;

    Ok(())
}

async fn worker() -> Result<()> {
    let Opt { path } = Opt::from_args();
    let backend = make_local_backend(&path);
    let storage = create_storage(&backend)?;

    let meta = {
        let mut meta_reader = storage.read(META_FILE);
        let mut buf = vec![];
        meta_reader.read_to_end(&mut buf).await?;
        let mut meta = BackupMeta::new();
        meta.merge_from_bytes(&buf)?;
        meta
    };

    let file_map = {
        let mut map: HashMap<i64, Vec<File>> = Default::default();
        for file in read_data_files(&storage, &meta).await {
            let table_id = decode_table_id(file.get_start_key())?;
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
                info!("rewrite done"; "file" => file.get_name());
            });
            handles.push(handle);
        }
    }

    join_all(handles).await;
    info!("all done");

    Ok(())
}

fn main() {
    let runtime = Runtime::new().unwrap();
    let logger = {
        let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let drain = slog_term::CompactFormat::new(decorator).build();
        let drain = std::sync::Mutex::new(drain).fuse();
        slog::Logger::root(drain, slog::o!())
    };
    slog_global::set_global(logger);
    runtime.block_on(worker().map(Result::unwrap))
}
