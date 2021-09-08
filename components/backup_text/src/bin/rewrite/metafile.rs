// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use external_storage_export::ExternalStorage;
use kvproto::brpb::{BackupMeta, File as BrFile, MetaFile, Schema};

use async_recursion::async_recursion;

use crate::utils::{read_message, update_file, write_message};

#[async_recursion(?Send)]
async fn walk_leaf_meta_file(
    storage: &impl ExternalStorage,
    file: &MetaFile,
    output: &mut impl FnMut(&MetaFile) -> (),
) -> Result<()> {
    let metas = file.get_meta_files();
    if metas.is_empty() {
        output(file)
    } else {
        for node in metas {
            let name = &node.name;
            let child = read_message(storage, name).await?;
            walk_leaf_meta_file(storage, &child, output).await?;
        }
    }
    Ok(())
}

#[async_recursion(?Send)]
async fn mutate_leaf_meta_file(
    storage: &impl ExternalStorage,
    new_storage: &impl ExternalStorage,
    file: &mut MetaFile,
    mutate: &mut impl FnMut(&mut MetaFile) -> (),
) -> Result<()> {
    let metas = file.mut_meta_files();
    if metas.is_empty() {
        mutate(file)
    } else {
        for node in metas.iter_mut() {
            let name = &node.name;
            let mut child = read_message(storage, name).await?;
            mutate_leaf_meta_file(storage, new_storage, &mut child, mutate).await?;

            let new_name = format!("{}.rewrite", node.name);
            let new_size = write_message(new_storage, &new_name, child)?;
            update_file(node, new_name, new_size);
        }
    }
    Ok(())
}

pub async fn read_schemas(
    storage: &impl ExternalStorage,
    meta: &BackupMeta,
) -> Result<Vec<Schema>> {
    let mut schemas = vec![];
    schemas.extend_from_slice(meta.get_schemas());

    let mut output_fn = |m: &MetaFile| {
        schemas.extend_from_slice(m.get_schemas());
    };
    walk_leaf_meta_file(storage, meta.get_schema_index(), &mut output_fn).await?;

    Ok(schemas)
}

pub async fn read_data_files(
    storage: &impl ExternalStorage,
    meta: &BackupMeta,
) -> Result<Vec<BrFile>> {
    let mut files = vec![];
    files.extend_from_slice(meta.get_files());

    let mut output_fn = |m: &MetaFile| {
        files.extend_from_slice(m.get_data_files());
    };
    walk_leaf_meta_file(storage, meta.get_file_index(), &mut output_fn).await?;

    Ok(files)
}

pub async fn mutate_data_files(
    storage: &impl ExternalStorage,
    new_storage: &impl ExternalStorage,
    meta: &mut BackupMeta,
    mut mutate: impl FnMut(&mut BrFile) -> (),
) -> Result<()> {
    meta.mut_files().iter_mut().for_each(&mut mutate);
    let mut mutate_fn = |m: &mut MetaFile| {
        m.mut_data_files().iter_mut().for_each(&mut mutate);
    };
    mutate_leaf_meta_file(storage, new_storage, meta.mut_file_index(), &mut mutate_fn).await?;

    Ok(())
}
