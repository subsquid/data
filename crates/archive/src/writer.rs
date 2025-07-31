use crate::fs::FSRef;
use crate::layout::DataChunk;
use crate::metrics;
use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use prometheus_client::metrics::gauge::Atomic;
use rayon::prelude::*;
use sqd_data_core::PreparedChunk;
use sqd_dataset::{DatasetDescriptionRef, TableDescription};
use std::fs::File;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;


pub struct WriterItem {
    pub chunk: DataChunk,
    pub data: PreparedChunk,
    pub description: DatasetDescriptionRef,
}


pub struct Writer {
    fs: FSRef,
    chunk_receiver: tokio::sync::mpsc::Receiver<WriterItem>,
    attach_index_field: bool,
}


impl Writer {
    pub fn new(fs: FSRef, chunk_receiver: tokio::sync::mpsc::Receiver<WriterItem>, attach_index_field: bool) -> Writer {
        Writer { fs, chunk_receiver, attach_index_field }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        while let Some(mut item) = self.chunk_receiver.recv().await {
            tracing::info!("writing {}", item.chunk.path());

            let last_block = item.chunk.last_block;
            let target_dir = tempfile::tempdir()?.into_path();
            let attach_index_field = self.attach_index_field;
            let writer_handle = tokio::task::spawn_blocking(move || {
                write_chunk(&mut item.data, &item.description, &target_dir, attach_index_field)
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

            metrics::LATEST_SAVED_BLOCK.set(last_block);
            metrics::LAST_SAVED_BLOCK.inner().set(last_block);
        }
        Ok(())
    }
}


fn add_index_column(batch: &mut RecordBatch, offset: usize) -> anyhow::Result<()> {
    let num_rows = batch.num_rows();
    let iter = offset as i32..(offset + num_rows) as i32;
    let idx_column = Arc::new(Int32Array::from_iter_values(iter));

    let mut new_columns = vec![idx_column as ArrayRef];
    new_columns.extend_from_slice(batch.columns());

    let mut new_fields = vec![Arc::new(Field::new("_idx", DataType::Int32, false))];
    new_fields.extend_from_slice(batch.schema().fields());

    let new_schema = Arc::new(Schema::new(new_fields));
    *batch = RecordBatch::try_new(new_schema, new_columns)?;

    Ok(())
}


fn write_chunk(
    prepared_chunk: &mut PreparedChunk,
    dataset_description: &DatasetDescriptionRef,
    target_dir: &Path,
    attach_index_field: bool,
) -> anyhow::Result<Vec<std::path::PathBuf>> {
    let default_desc = TableDescription::default();
    prepared_chunk
        .tables
        .par_iter_mut()
        .map(|(&name, table)| {
            let mut schema = table.schema();
            if attach_index_field && name != "blocks" {
                let mut new_fields = vec![Arc::new(Field::new("_idx", DataType::Int32, false))];
                new_fields.extend_from_slice(schema.fields());
                schema = Arc::new(Schema::new(new_fields));
            }
            let desc = dataset_description
                .tables
                .get(name)
                .unwrap_or(&default_desc);

            let mut builder = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(6)?))
                .set_data_page_size_limit(desc.options.default_page_size)
                .set_max_row_group_size(desc.options.row_group_size)
                .set_statistics_enabled(EnabledStatistics::None)
                .set_dictionary_enabled(false)
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
                let mut batch = table.read_record_batch(offset, len)?;
                if attach_index_field && name != "blocks" {
                    add_index_column(&mut batch, offset)?;
                }
                writer.write(&batch)?;
                offset += len;
            }
            writer.close()?;

            file.sync_all()?;

            Ok(path)
        })
        .collect()
}
