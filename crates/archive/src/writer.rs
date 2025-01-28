use crate::fs::FSRef;
use crate::layout::DataChunk;
use crate::metrics;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use prometheus_client::metrics::gauge::Atomic;
use rayon::prelude::*;
use sqd_data_core::PreparedChunk;
use sqd_dataset::{DatasetDescriptionRef, TableDescription};
use std::fs::File;
use std::path::Path;
use tokio::sync::mpsc::UnboundedReceiver;


pub struct WriterItem {
    pub chunk: DataChunk,
    pub data: PreparedChunk,
    pub description: DatasetDescriptionRef,
}


pub struct Writer {
    fs: FSRef,
    chunk_receiver: UnboundedReceiver<WriterItem>,
}


impl Writer {
    pub fn new(fs: FSRef, chunk_receiver: UnboundedReceiver<WriterItem>) -> Writer {
        Writer { fs, chunk_receiver }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        while let Some(mut item) = self.chunk_receiver.recv().await {
            tracing::info!("writing {}", item.chunk.path());

            let last_block = item.chunk.last_block;
            let target_dir = tempfile::tempdir()?.into_path();
            let writer_handle = tokio::task::spawn_blocking(move || {
                write_chunk(&mut item.data, &item.description, &target_dir)
            });

            let mut files = writer_handle.await??;

            let blocks_pos = files.iter().position(|file| file.ends_with("blocks.parquet")).unwrap();
            let blocks = files.swap_remove(blocks_pos);

            let chunk_path = item.chunk.path();
            for file in files {
                let file_name = file.file_name().unwrap().to_str().unwrap();
                let dest = format!("{}/{}", chunk_path, file_name);
                self.fs.move_local(&file, &dest).await?;
            }

            let dest = format!("{}/blocks.parquet", chunk_path);
            self.fs.move_local(&blocks, &dest).await?;

            metrics::LAST_SAVED_BLOCK.inner().set(last_block);
        }
        Ok(())
    }
}


fn write_chunk(
    prepared_chunk: &mut PreparedChunk,
    dataset_description: &DatasetDescriptionRef,
    target_dir: &Path,
) -> anyhow::Result<Vec<std::path::PathBuf>> {
    let default_desc = TableDescription::default();
    prepared_chunk
        .tables
        .par_iter_mut()
        .map(|(&name, table)| {
            let schema = table.schema();
            let desc = dataset_description
                .tables
                .get(name)
                .unwrap_or(&default_desc);

            let mut builder = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(6)?))
                .set_data_page_size_limit(desc.options.default_page_size)
                .set_max_row_group_size(desc.options.row_group_size)
                .set_dictionary_page_size_limit(192 * 1024)
                .set_write_batch_size(50);

            for (column, options) in &desc.options.column_options {
                if options.stats_enable {
                    builder = builder.set_column_statistics_enabled((*column).into(), EnabledStatistics::Page);
                }

                if options.dictionary_encoding {
                    builder = builder.set_column_dictionary_enabled((*column).into(), true);
                }
            }

            let props = builder.build();

            let path = target_dir.join(format!("{}.parquet", name));
            let file = File::create(&path)?;

            let mut writer = ArrowWriter::try_new(&file, schema, Some(props))?;
            let mut offset = 0;
            while offset < table.num_rows() {
                let len = std::cmp::min(50, table.num_rows() - offset);
                let batch = table.read_record_batch(offset, len)?;
                writer.write(&batch)?;
                offset += len;
            }
            writer.close()?;

            file.sync_all()?;

            Ok(path)
        })
        .collect()
}
