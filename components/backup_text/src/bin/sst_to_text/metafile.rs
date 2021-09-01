// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use external_storage_export::ExternalStorage;
use kvproto::brpb::{BackupMeta, File as BrFile, MetaFile, Schema};

use async_recursion::async_recursion;

use crate::utils::{read_message, write_message};

#[async_recursion(?Send)]
async fn walk_leaf_meta_file(
    storage: &impl ExternalStorage,
    file: &MetaFile,
    output: &mut impl FnMut(&MetaFile) -> (),
) {
    let metas = file.get_meta_files();
    if metas.is_empty() {
        output(file)
    } else {
        for node in metas {
            let name = &node.name;
            let child = read_message(storage, name).await.unwrap();
            walk_leaf_meta_file(storage, &child, output).await;
        }
    }
}

#[async_recursion(?Send)]
async fn mutate_leaf_meta_file(
    storage: &impl ExternalStorage,
    new_storage: &impl ExternalStorage,
    file: &mut MetaFile,
    mutate: &mut impl FnMut(&mut MetaFile) -> (),
) {
    let metas = file.mut_meta_files();
    if metas.is_empty() {
        mutate(file)
    } else {
        for node in metas.iter_mut() {
            let name = &node.name;
            let mut child = read_message(storage, name).await.unwrap();
            mutate_leaf_meta_file(storage, new_storage, &mut child, mutate).await;

            let new_name = format!("{}.rewrite", node.name);
            let new_size = write_message(new_storage, &new_name, child).unwrap();
            node.clear_crc64xor();
            node.clear_sha256();
            node.set_name(new_name);
            node.set_size(new_size);
        }
    }
}

pub async fn read_schemas(storage: &impl ExternalStorage, meta: &BackupMeta) -> Vec<Schema> {
    let mut schemas = vec![];
    schemas.extend_from_slice(meta.get_schemas());

    let mut output_fn = |m: &MetaFile| {
        schemas.extend_from_slice(m.get_schemas());
    };
    walk_leaf_meta_file(storage, meta.get_schema_index(), &mut output_fn).await;

    schemas
}

pub async fn read_data_files(storage: &impl ExternalStorage, meta: &BackupMeta) -> Vec<BrFile> {
    let mut files = vec![];
    files.extend_from_slice(meta.get_files());

    let mut output_fn = |m: &MetaFile| {
        files.extend_from_slice(m.get_data_files());
    };
    walk_leaf_meta_file(storage, meta.get_file_index(), &mut output_fn).await;

    files
}

pub async fn mutate_data_files(
    storage: &impl ExternalStorage,
    new_storage: &impl ExternalStorage,
    meta: &mut BackupMeta,
    mut mutate: impl FnMut(&mut BrFile) -> (),
) {
    meta.mut_files().iter_mut().for_each(&mut mutate);
    let mut mutate_fn = |m: &mut MetaFile| {
        m.mut_data_files().iter_mut().for_each(&mut mutate);
    };
    mutate_leaf_meta_file(storage, new_storage, meta.mut_file_index(), &mut mutate_fn).await;
}
