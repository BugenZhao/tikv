use crate::{opt::RewriteMode, utils::update_file};
use anyhow::{anyhow, Result};
use backup_text::rwer::{TextReader, TextWriter};
use engine_rocks::{RocksSstReader, RocksSstWriterBuilder};
use engine_traits::{
    name_to_cf, ExternalSstFileInfo, Iterator, SeekKey, SstReader, SstWriter, SstWriterBuilder,
};
use futures_util::io::AllowStdIo;
use kvproto::brpb::{File, StorageBackend};
use std::{
    fs,
    path::{Path, PathBuf},
};
use tikv_util::time::Limiter;
use tipb::TableInfo;

pub fn rewrite(
    in_backend: StorageBackend,
    out_backend: StorageBackend,
    speed_limiter: Limiter,
    tmp_dir: PathBuf,
    mut file: File,
    rename_to: Option<String>,
    table_info: TableInfo,
    mode: RewriteMode,
) -> Result<Option<File>> {
    let cf = name_to_cf(file.get_cf()).ok_or_else(|| anyhow!("bad cf name: {}", file.get_cf()))?;
    let new_name = rename_to.unwrap_or_else(|| {
        let mut p = PathBuf::from(file.get_name());
        p.set_extension(mode.extension());
        p.to_str().unwrap().to_owned()
    });

    let mut in_tmp_path = tmp_dir;
    in_tmp_path.push(file.get_name());
    let in_tmp_path_str = in_tmp_path.to_str().unwrap().to_owned();
    let out_tmp_path_str = format!("{}.rewrite_tmp", in_tmp_path_str);
    let ext_storage = external_storage_export::create_storage(&in_backend)?;
    let out_storage = external_storage_export::create_storage(&out_backend)?;

    // Download the file to local path frist
    ext_storage.restore(
        file.get_name(),
        in_tmp_path.clone(),
        file.get_size(),
        &speed_limiter,
    )?;
    let file_size = match mode {
        RewriteMode::ToText | RewriteMode::ToZtext { .. } | RewriteMode::ToCsv { .. } => {
            let compression_level = match mode {
                RewriteMode::ToText => None,
                RewriteMode::ToZtext { level } => Some(level.clamp(0, 9)),
                RewriteMode::ToCsv { .. } => None,
                _ => unreachable!(),
            };
            let mut writer = TextWriter::new(
                table_info,
                cf,
                mode.file_format(),
                &out_tmp_path_str,
                compression_level,
            )?;

            let reader = RocksSstReader::open(&in_tmp_path_str)?;
            reader.verify_checksum()?;
            let mut iter = reader.iter();
            iter.seek(SeekKey::Start)?; // ignore start_key in file
            while iter.valid()? {
                let key = iter.key();
                let value = iter.value();
                writer.put_line(key, &value)?;
                iter.next()?;
            }

            let (file_size, reader) = writer.finish_read()?;
            if file_size != 0 {
                out_storage.write(
                    &new_name,
                    Box::new(speed_limiter.limit(AllowStdIo::new(reader))),
                    file_size,
                )?;
            }
            writer.cleanup()?;
            file_size
        }

        RewriteMode::ToSst => {
            let compressed_text = Path::new(file.get_name()).extension().unwrap_or_default()
                == RewriteMode::ToZtext { level: 1 }.extension();
            let mut reader = TextReader::new(&in_tmp_path_str, table_info, cf, compressed_text)?;
            let mut writer = RocksSstWriterBuilder::new()
                .set_in_memory(true)
                .set_cf(cf)
                .build(&out_tmp_path_str)?;

            while let Some((key, value)) = reader.pop_kv()? {
                writer.put(&key, &value)?;
            }

            let (sst_info, reader) = writer.finish_read()?;
            let file_size = sst_info.file_size();
            if file_size != 0 {
                out_storage.write(
                    &new_name,
                    Box::new(speed_limiter.limit(AllowStdIo::new(reader))),
                    file_size,
                )?;
            }
            file_size
        }
    };

    // Cleanup tmp file
    fs::remove_file(&in_tmp_path)?;

    if file_size != 0 {
        update_file(&mut file, new_name.to_owned(), file_size);
        Ok(Some(file))
    } else {
        Ok(None)
    }
}
