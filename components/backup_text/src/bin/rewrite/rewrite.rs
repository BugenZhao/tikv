use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};
use backup_text::rwer::{TextReader, TextWriter};
use engine_rocks::{RocksSstReader, RocksSstWriterBuilder};
use engine_traits::{
    name_to_cf, ExternalSstFileInfo, Iterator, SeekKey, SstReader, SstWriter, SstWriterBuilder,
};
use kvproto::brpb::File;
use tipb::TableInfo;

use crate::{opt::RewriteMode, utils::update_file};

pub fn rewrite(
    dir: impl AsRef<Path>,
    new_dir: impl AsRef<Path>,
    file: File,
    rename_to: Option<String>,
    table_info: TableInfo,
    mode: RewriteMode,
) -> Result<Option<File>> {
    let get_path = |dir: &Path, name: &str| {
        let mut path = PathBuf::from(dir);
        path.push(name);
        path
    };

    let path = get_path(dir.as_ref(), file.get_name());
    let path_str = path.to_str().unwrap();

    let cf = name_to_cf(file.get_cf()).ok_or_else(|| anyhow!("bad cf name: {}", file.get_cf()))?;

    let new_path = rename_to
        .map(|n| get_path(new_dir.as_ref(), &n))
        .unwrap_or_else(|| {
            get_path(new_dir.as_ref(), file.get_name()).with_extension(mode.extension())
        });
    let new_path_str = new_path.to_str().unwrap();

    let (new_path, size) = match mode {
        RewriteMode::ToText | RewriteMode::ToZtext { .. } | RewriteMode::ToCsv { .. } => {
            let reader = RocksSstReader::open(path_str)?;
            reader.verify_checksum()?;

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
                &format!("{}.rewrite_tmp", new_path_str),
                compression_level,
            )?;
            let temp_path_str = writer.name().to_owned();

            let mut iter = reader.iter();
            iter.seek(SeekKey::Start)?; // ignore start_key in file

            while iter.valid()? {
                let key = iter.key();
                let value = iter.value();
                writer.put_line(key, &value)?;
                iter.next()?;
            }

            let size = writer.finish()?;
            if matches!(mode, RewriteMode::ToCsv { .. }) && size == 0 {
                // remove empty csv files
                writer.cleanup()?;
                (None, size)
            } else {
                fs::rename(&temp_path_str, &new_path)?;
                (Some(new_path), size)
            }
        }

        RewriteMode::ToSst => {
            let compressed_text = path.extension().unwrap_or_default()
                == RewriteMode::ToZtext { level: 1 }.extension();
            let mut reader = TextReader::new(path_str, table_info, cf, compressed_text)?;
            let mut writer = RocksSstWriterBuilder::new()
                .set_cf(cf)
                .build(new_path_str)?;

            while let Some((key, value)) = reader.pop_kv()? {
                writer.put(&key, &value)?;
            }

            let size = writer.finish()?.file_size();
            (Some(new_path), size)
        }
    };

    let mutated_file = new_path.map(|p| {
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        let mut file = file;
        update_file(&mut file, name, size);
        file
    });

    Ok(mutated_file)
}
