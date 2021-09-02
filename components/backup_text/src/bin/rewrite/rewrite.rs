use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};
use backup_text::rwer::{TextReader, TextWriter};
use engine_rocks::{RocksSstReader, RocksSstWriterBuilder};
use engine_traits::{name_to_cf, Iterator, SeekKey, SstReader, SstWriter, SstWriterBuilder};
use kvproto::brpb::File;
use slog_global::warn;
use tipb::TableInfo;

use crate::utils::update_file;

#[derive(Debug, Clone, Copy)]
pub enum RewriteMode {
    SstToText,
    TextToSst,
}

pub fn rewrite(
    _table_id: i64,
    dir: impl AsRef<Path>,
    new_dir: impl AsRef<Path>,
    file: File,
    table_info: TableInfo,
) -> Result<File> {
    let get_path = |dir: &Path| {
        let mut path = PathBuf::from(dir);
        path.push(file.get_name());
        path
    };

    let path = get_path(dir.as_ref());
    let path_str = path.to_str().unwrap();

    let mode = path
        .extension()
        .and_then(|ext| match ext.to_str().unwrap() {
            "sst" => Some(RewriteMode::SstToText),
            "txt" | "csv" => Some(RewriteMode::TextToSst),
            _ => None,
        })
        .ok_or_else(|| anyhow!("unable to detect rewrite mode"))
        .unwrap();

    let cf = name_to_cf(file.get_cf()).ok_or_else(|| anyhow!("bad cf name"))?;
    let mut count = 0;

    let new_path = match mode {
        RewriteMode::SstToText => {
            let new_path = get_path(new_dir.as_ref()).with_extension("txt");
            let new_path_str = new_path.to_str().unwrap();

            let reader = RocksSstReader::open(path_str)?;
            reader.verify_checksum()?;
            let mut writer =
                TextWriter::new(table_info, cf, &format!("{}.rewrite_tmp", new_path_str))?;
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
            fs::rename(&temp_path_str, &new_path)?;
            new_path
        }
        RewriteMode::TextToSst => {
            let new_path = get_path(new_dir.as_ref()).with_extension("sst");
            let new_path_str = new_path.to_str().unwrap();

            let mut reader = TextReader::new(path_str, table_info, cf)?;
            let mut writer = RocksSstWriterBuilder::new()
                .set_cf(cf)
                .build(new_path_str)?;

            while let Some((key, value)) = reader.pop_kv()? {
                writer.put(&key, &value)?;
                count += 1;
            }

            let _ = writer.finish()?;
            new_path
        }
    };

    if count != file.get_total_kvs() {
        warn!(
            "kv pairs count mismatched";
            "file" => path_str,
            "count" => count,
            "expected" => file.get_total_kvs()
        );
    }

    let mutated_file = {
        let name = new_path.file_name().unwrap().to_string_lossy().to_string();
        let size = fs::metadata(&new_path).unwrap().len();
        let mut file = file;
        update_file(&mut file, name, size);
        file
    };

    Ok(mutated_file)
}
