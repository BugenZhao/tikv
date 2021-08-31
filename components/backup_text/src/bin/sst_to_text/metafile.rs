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
    file: &mut MetaFile,
    mutate: &mut impl FnMut(&mut MetaFile) -> (),
) {
    let metas = file.get_meta_files();
    if metas.is_empty() {
        mutate(file)
    } else {
        for node in metas {
            let name = &node.name;
            let mut child = read_message(storage, name).await.unwrap();
            mutate_leaf_meta_file(storage, &mut child, mutate).await;
            write_message(storage, name, child).unwrap();
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
    meta: &mut BackupMeta,
    mut mutate: impl FnMut(&mut BrFile) -> (),
) {
    meta.mut_files().iter_mut().for_each(&mut mutate);
    let mut mutate_fn = |m: &mut MetaFile| {
        m.mut_data_files().iter_mut().for_each(&mut mutate);
    };
    mutate_leaf_meta_file(storage, meta.mut_file_index(), &mut mutate_fn).await;
}
