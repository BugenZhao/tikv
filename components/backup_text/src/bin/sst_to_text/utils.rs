// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use external_storage_export::ExternalStorage;
use futures::{io::Cursor, AsyncReadExt};

pub fn write_message(
    storage: &impl ExternalStorage,
    name: impl AsRef<str>,
    message: impl protobuf::Message,
) -> Result<u64> {
    let bytes = message.write_to_bytes()?;
    let length = bytes.len() as u64;
    let cursor = Cursor::new(bytes);
    storage.write(name.as_ref(), Box::new(cursor), length)?;
    Ok(length)
}

pub async fn read_message<M: protobuf::Message>(
    storage: &impl ExternalStorage,
    name: impl AsRef<str>,
) -> Result<M> {
    let mut reader = storage.read(name.as_ref());
    let mut buf = vec![];
    let _ = reader.read_to_end(&mut buf).await?;
    let message = protobuf::parse_from_bytes::<M>(&buf)?;
    Ok(message)
}
