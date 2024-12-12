use crate::fs::Fs;
use arrow::array::RecordBatch;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use sqd_data_core::{ChunkProcessor, PreparedTable, Builder, HashAndHeight};


pub struct ParquetWriter {
    memory_treshold: usize,
    chunk_builder: Box<dyn Builder>,
    chunk_processor: ChunkProcessor,
    buffered_blocks: usize,
}


impl ParquetWriter {
    pub fn new(chunk_builder: Box<dyn Builder>) -> ParquetWriter {
        ParquetWriter {
            memory_treshold: 50 * 1024 * 1024,
            chunk_processor: chunk_builder.chunk_processor(),
            chunk_builder,
            buffered_blocks: 0,
        }
    }

    fn spill_on_disk(&mut self) -> anyhow::Result<()> {
        let slice = self.chunk_builder.as_slice();
        self.chunk_processor.push(slice)?;
        self.chunk_builder.clear();
        self.buffered_blocks = 0;
        Ok(())
    }
}


impl ParquetWriter {
    pub fn push(&mut self, line: &String) -> anyhow::Result<HashAndHeight> {
        let hash_and_height = self.chunk_builder.push(line)?;
        self.buffered_blocks += 1;

        if self.chunk_builder.byte_size() > self.memory_treshold {
            self.spill_on_disk()?;
        }

        Ok(hash_and_height)
    }

    pub fn buffered_bytes(&self) -> usize {
        self.chunk_builder.byte_size() + self.chunk_processor.byte_size()
    }

    pub fn flush(&mut self, fs: Box<dyn Fs>) -> anyhow::Result<()> {
        if self.buffered_blocks != 0 {
            self.spill_on_disk()?;
        }

        let dataset_description = self.chunk_builder.dataset_description();
        let new_chunk_processor = self.chunk_builder.chunk_processor();
        let chunk_processor = std::mem::replace(&mut self.chunk_processor, new_chunk_processor);
        let tables = chunk_processor.finish()?;
        for (name, table) in tables {
            let schema = table.schema();
            let batches = iterator(table);
            let desc = dataset_description.tables.get(&name).unwrap();
            let zstd_level = ZstdLevel::try_new(3)?;
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(zstd_level))
                .set_data_page_size_limit(32 * 1024)
                .set_dictionary_page_size_limit(192 * 1024)
                .set_write_batch_size(50)
                .set_max_row_group_size(desc.options.row_group_size)
                .build();
            let filename = format!("{}.parquet", &name);
            fs.write_parquet(&filename, batches, schema, Some(props))?;
        }

        Ok(())
    }
}


fn iterator(mut table: PreparedTable) -> Box<dyn Iterator<Item = RecordBatch>> {
    let step = 50;
    let mut offset = 0;
    let num_rows = table.num_rows();
    Box::new(std::iter::from_fn(move || {
        let len = std::cmp::min(num_rows - offset, step);
        debug_assert!(offset <= num_rows);
        if offset == num_rows {
            None
        } else {
            let batch = table.read_record_batch(offset, len).unwrap();
            offset += len;
            Some(batch)
        }
    }))
}
