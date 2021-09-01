// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod br_models;
mod metafile;
mod utils;

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
use futures::future::try_join_all;
use kvproto::brpb::{BackupMeta, File};
use slog::Drain;
use slog_global::{error, info, warn};
use structopt::StructOpt;
use tidb_query_datatype::codec::table::decode_table_id;
use tipb::TableInfo;
use tokio::runtime;
use txn_types::WriteRef;

use crate::{
    br_models::schema_to_table_info,
    metafile::{mutate_data_files, read_data_files, read_schemas},
    utils::{read_message, write_message},
};

const META_FILE: &'static str = "backupmeta";

#[derive(StructOpt)]
struct Opt {
    #[structopt(parse(from_os_str))]
    path: PathBuf,
    #[structopt(short, long, default_value = "8")]
    threads: usize,
}

fn rewrite(
    _table_id: i64,
    dir: impl AsRef<Path>,
    new_dir: impl AsRef<Path>,
    file: File,
    table_info: TableInfo,
) -> Result<File> {
    let start_key = file.get_start_key();
    let end_key = file.get_end_key();

    let get_path = |dir: &Path| {
        let mut path = PathBuf::from(dir);
        path.push(file.get_name());
        path
    };

    let path = get_path(dir.as_ref());
    let path_str = path.to_str().unwrap();
    let new_path = get_path(new_dir.as_ref());
    let new_path_str = new_path.to_str().unwrap();

    let reader = RocksSstReader::open(path_str)?;
    reader.verify_checksum()?;

    let mut iter = reader.iter();
    iter.seek(SeekKey::Key(start_key))?; // ignore start_key in file

    let cf = name_to_cf(file.get_cf()).ok_or_else(|| anyhow!("bad cf name"))?;
    let mut writer = TextWriter::new(table_info, cf, &format!("{}.rewrite_tmp", new_path_str))?;
    let temp_path = writer.name().to_owned();

    let mut count = 0;
    while iter.valid()? {
        let key = iter.key();
        if decode_key(key).0.as_slice() >= end_key {
            // todo: really needed?
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
        warn!(
            "kv pairs count mismatched";
            "file" => path_str,
            "count" => count,
            "expected" => file.get_total_kvs()
        );
    }

    let reader = writer.finish_read()?;
    drop(reader);

    let final_path = new_path_str.replace(".sst", ".rewrite.txt");
    fs::rename(&temp_path, &final_path)?;

    let mutated_file = {
        let name = file.get_name().replace(".sst", ".rewrite.txt");
        let size = fs::metadata(&final_path).unwrap().len();
        let mut file = file;
        file.clear_sha256();
        file.clear_crc64xor();
        file.set_name(name);
        file.set_size(size);
        file
    };

    Ok(mutated_file)
}

async fn worker(opt: Opt) -> Result<()> {
    let Opt { path, .. } = opt;
    let storage = {
        let backend = make_local_backend(&path);
        create_storage(&backend)?
    };

    let new_path = {
        let mut new_path = path.clone();
        new_path.push("rewrite");
        new_path
    };
    let new_storage = {
        fs::create_dir_all(&new_path)?;
        let backend = make_local_backend(&new_path);
        create_storage(&backend)?
    };

    let meta: BackupMeta = read_message(&storage, META_FILE).await?;

    let file_map = {
        let mut map: HashMap<i64, Vec<File>> = Default::default();
        for file in read_data_files(&storage, &meta).await {
            let table_id = decode_table_id(file.get_start_key())?;
            map.entry(table_id).or_default().push(file);
        }
        map
    };

    // println!("{:?}", file_map);

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
            let (dir, new_dir) = (path.clone(), new_path.clone());
            let handle = tokio::task::spawn_blocking(move || {
                let name = file.get_name().to_owned();
                match rewrite(table_id, dir, new_dir, file, table_info) {
                    Ok(mutated_file) => {
                        info!("rewrite done"; "file" => &name);
                        Ok((name, mutated_file))
                    }
                    Err(e) => {
                        error!("failed to rewrite file"; "file" => &name, "error" => ?e);
                        Err(e)
                    }
                }
            });
            handles.push(handle);
        }
    }

    let mutated_file_map = try_join_all(handles)
        .await?
        .into_iter()
        .collect::<Result<HashMap<_, _>, _>>()
        .unwrap();
    info!("rewrite all done");

    let new_meta = {
        let mut meta = meta;
        mutate_data_files(&storage, &new_storage, &mut meta, |file| {
            let mutated_file = mutated_file_map
                .get(file.get_name())
                .cloned()
                .ok_or_else(|| anyhow!("file not mutated: {}", file.get_name()))
                .unwrap();
            *file = mutated_file;
        })
        .await;
        meta
    };

    write_message(&new_storage, META_FILE, new_meta)?;
    info!("update meta file done");

    Ok(())
}

fn main() {
    let opt: Opt = Opt::from_args();

    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .max_blocking_threads(opt.threads)
        .build()
        .unwrap();

    let logger = {
        let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let drain = slog_term::CompactFormat::new(decorator).build();
        let drain = std::sync::Mutex::new(drain).fuse();
        slog::Logger::root(drain, slog::o!())
    };
    slog_global::set_global(logger);

    runtime.block_on(worker(opt)).unwrap()
}
