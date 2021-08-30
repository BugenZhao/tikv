use external_storage_export::ExternalStorage;
use futures::AsyncReadExt;
use kvproto::brpb::{BackupMeta, File as BrFile, MetaFile, Schema};
use protobuf::Message;

use async_recursion::async_recursion;

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
            let child = {
                let mut reader = storage.read(&node.name);
                let mut buf = vec![];
                let _ = reader.read_to_end(&mut buf).await.unwrap();
                let mut child = MetaFile::new();
                child.merge_from_bytes(&buf).unwrap();
                child
            };
            walk_leaf_meta_file(storage, &child, output).await;
        }
    }
}

pub async fn read_schemas(storage: &impl ExternalStorage, meta: &BackupMeta) -> Vec<Schema> {
    let mut schemas = vec![];
    schemas.extend_from_slice(meta.get_schemas());

    let mut output_fn = |m: &MetaFile| {
        schemas.extend_from_slice(m.get_schemas());
    };
    walk_leaf_meta_file(storage, meta.get_file_index(), &mut output_fn).await;

    schemas
}

pub async fn read_data_files(storage: &impl ExternalStorage, meta: &BackupMeta) -> Vec<BrFile> {
    let mut files = vec![];
    files.extend_from_slice(meta.get_files());

    let mut output_fn = |m: &MetaFile| {
        files.extend_from_slice(m.get_data_files());
    };
    walk_leaf_meta_file(storage, meta.get_schema_index(), &mut output_fn).await;

    files
}
