// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod br_models;
mod metafile;
mod rewrite;
mod utils;

use std::{fs, path::PathBuf};

use anyhow::{anyhow, Result};
use collections::HashMap;

use external_storage_export::{create_storage, make_local_backend};
use futures::future::try_join_all;
use kvproto::brpb::{BackupMeta, File};
use rewrite::RewriteMode;
use slog::Drain;
use slog_global::{error, info};
use structopt::StructOpt;
use tidb_query_datatype::codec::table::decode_table_id;
use tokio::runtime;

use crate::{
    br_models::schema_to_table_info,
    metafile::{mutate_data_files, read_data_files, read_schemas},
    rewrite::rewrite,
    utils::{read_message, write_message},
};

const META_FILE: &'static str = "backupmeta";

#[derive(StructOpt)]
#[structopt(
    name = "rewrite",
    about = "Rewrite backup files between `sst` and text formats."
)]
struct Opt {
    /// Rewrite mode, case insensitive
    #[structopt(possible_values = &RewriteMode::variants(), case_insensitive = true)]
    mode: RewriteMode,
    /// Path to the directory of original backup files
    #[structopt(parse(from_os_str))]
    path: PathBuf,
    /// Path to the directory of rewritten backup files, `<path>/rewrite` if not present
    #[structopt(parse(from_os_str))]
    new_path: Option<PathBuf>,
    /// Thread concurrency for rewriting
    #[structopt(short, long, default_value = "8")]
    threads: usize,
}

fn check_mode(mode: RewriteMode, file_map: &HashMap<i64, Vec<File>>) -> Result<()> {
    let paths = file_map
        .values()
        .flatten()
        .map(|f| PathBuf::from(f.get_name()));

    let bad_files = paths
        .filter(|path| {
            let ext = path.extension().unwrap_or_default();
            let ok = match (ext.to_str().unwrap(), mode) {
                ("sst", RewriteMode::ToText | RewriteMode::ToCsv) => true,
                ("txt" | "csv", RewriteMode::ToSst) => true,
                _ => false,
            };
            !ok
        })
        .collect::<Vec<_>>();

    if bad_files.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "bad rewrite mode `{}` on files: {:#?}",
            mode,
            bad_files
        ))
    }
}

async fn worker(opt: Opt) -> Result<()> {
    let Opt {
        path,
        new_path,
        mode,
        ..
    } = opt;

    let storage = {
        let backend = make_local_backend(&path); // todo: support other backends
        create_storage(&backend)?
    };

    let new_path = new_path.unwrap_or_else(|| {
        let mut new_path = path.clone();
        new_path.push("rewrite");
        new_path
    });
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
        check_mode(mode, &map)?;
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
            let (dir, new_dir) = (path.clone(), new_path.clone());
            let name = file.get_name().to_owned();
            let handle = tokio::task::spawn_blocking(move || {
                match rewrite(table_id, dir, new_dir, file, table_info, mode) {
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
