use crate::chain_builder::{AnyChainBuilder, ChainBuilderBox};
use parking_lot::Mutex;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use sqd_data_core::ChunkProcessor;
use sqd_dataset::TableDescription;
use std::fs::File;
use std::ops::DerefMut;
use std::path::Path;


struct State {
    builder: ChainBuilderBox,
    processor: ChunkProcessor
}


impl State {
    fn spill_on_disk(&mut self) -> anyhow::Result<()> {
        self.builder.chunk_builder().submit_to_processor(&mut self.processor)?;
        self.builder.chunk_builder_mut().clear();
        Ok(())
    }
}


pub struct ParquetWriter {
    state: Mutex<State>,
    spare_processor: Mutex<Option<ChunkProcessor>>,
    memory_threshold: usize,
    max_num_rows: usize
}


impl ParquetWriter {
    pub fn new(chain_builder: ChainBuilderBox) -> ParquetWriter {
        ParquetWriter {
            state: Mutex::new(State {
                processor: chain_builder.chunk_builder().new_chunk_processor(),
                builder: chain_builder
            }),
            spare_processor: Mutex::new(None),
            memory_threshold: 40 * 1024 * 1024,
            max_num_rows: 200_000
        }
    }

    pub fn push(&self, block_json: &[u8]) -> anyhow::Result<()> {
        let mut state = self.state.lock();
        
        state.builder.push(block_json)?;
        
        if state.builder.chunk_builder().byte_size() > self.memory_threshold {
            state.spill_on_disk()?;
        }
        Ok(())
    }

    pub fn last_parent_block_hash(&self) -> &str {
        todo!()
    }

    pub fn buffered_bytes(&self) -> usize {
        let state = self.state.lock();
        state.builder.chunk_builder().byte_size() + state.processor.byte_size()
    }

    pub fn max_num_rows(&self) -> usize {
        let state = self.state.lock();
        // not precise, but should be OK practically
        state.builder.chunk_builder().max_num_rows() + state.processor.max_num_rows()
    }

    pub fn flush(&self, target_dir: &Path) -> anyhow::Result<()> {
        let mut state = self.state.lock();

        if state.builder.chunk_builder().max_num_rows() > 0 {
            state.spill_on_disk()?;
        }

        let dataset_description = state.builder
            .chunk_builder()
            .dataset_description();

        let spare_proc = {
            let mut spare = self.spare_processor.lock();
            std::mem::take(spare.deref_mut()).unwrap_or_else(|| {
                state.builder.chunk_builder().new_chunk_processor()
            })
        };

        let processor = std::mem::replace(&mut state.processor, spare_proc);

        drop(state);

        let mut prepared_chunk = scopeguard::guard(processor.finish()?, |chunk| {
            let mut spare = self.spare_processor.lock();
            if spare.is_none() {
                *spare = chunk.into_processor().ok()
            }
        });

        let default_desc = TableDescription::default();

        for (&name, table) in prepared_chunk.tables.iter_mut() {
            let schema = table.schema();
            let desc = dataset_description.tables.get(name).unwrap_or(&default_desc);

            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(6)?))
                .set_data_page_size_limit(32 * 1024)
                .set_dictionary_page_size_limit(192 * 1024)
                .set_write_batch_size(50)
                .set_max_row_group_size(desc.options.row_group_size)
                .build();

            let file = File::create(
                target_dir.join(format!("{}.parquet", name))
            )?;

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
        }

        Ok(())
    }
}