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
use kvproto::brpb::{File, FileFormat};
use slog_global::warn;
use structopt::clap::arg_enum;
use tipb::TableInfo;

use crate::utils::update_file;

arg_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum RewriteMode {
        ToText,
        ToCsv,
        ToSst,
    }
}

impl RewriteMode {
    pub fn extension(&self) -> &'static str {
        match self {
            RewriteMode::ToText => "txt",
            RewriteMode::ToCsv => "csv",
            RewriteMode::ToSst => "sst",
        }
    }

    pub fn file_format(&self) -> FileFormat {
        match self {
            RewriteMode::ToText => FileFormat::Text,
            RewriteMode::ToCsv => FileFormat::Csv,
            RewriteMode::ToSst => FileFormat::Sst,
        }
    }
}

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

    let cf = name_to_cf(file.get_cf()).ok_or_else(|| anyhow!("bad cf name"))?;
    let mut count = 0;

    let new_path = rename_to
        .map(|n| get_path(new_dir.as_ref(), &n))
        .unwrap_or_else(|| {
            get_path(new_dir.as_ref(), file.get_name()).with_extension(mode.extension())
        });
    let new_path_str = new_path.to_str().unwrap();

    let (new_path, size) = match mode {
        RewriteMode::ToText | RewriteMode::ToCsv => {
            let reader = RocksSstReader::open(path_str)?;
            reader.verify_checksum()?;

            let mut writer = TextWriter::new(
                table_info,
                cf,
                mode.file_format(),
                &format!("{}.rewrite_tmp", new_path_str),
            )?;
            let temp_path_str = writer.name().to_owned();

            let mut iter = reader.iter();
            iter.seek(SeekKey::Start)?; // ignore start_key in file

            while iter.valid()? {
                let key = iter.key();
                let value = iter.value();
                writer.put_line(key, &value)?;

                count += 1;
                iter.next()?;
            }

            let _ = writer.finish()?;
            let size = writer.get_size();
            if mode == RewriteMode::ToCsv && size == 0 {
                // remove empty csv files
                writer.cleanup()?;
                (None, size)
            } else {
                fs::rename(&temp_path_str, &new_path)?;
                (Some(new_path), size)
            }
        }

        RewriteMode::ToSst => {
            let mut reader = TextReader::new(path_str, table_info, cf)?;
            let mut writer = RocksSstWriterBuilder::new()
                .set_cf(cf)
                .build(new_path_str)?;

            while let Some((key, value)) = reader.pop_kv()? {
                writer.put(&key, &value)?;
                count += 1;
            }

            let info = writer.finish()?;
            (Some(new_path), info.file_size())
        }
    };

    if count != file.get_total_kvs() {
        warn!(
            "kv pairs count mismatched";
            "file" => file.get_name(),
            "count" => count,
            "expected" => file.get_total_kvs()
        );
    }

    let mutated_file = new_path.map(|p| {
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        let mut file = file;
        update_file(&mut file, name, size);
        file
    });

    Ok(mutated_file)
}
