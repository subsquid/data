use crate::chain_builder::{AnyChainBuilder, ChainBuilderBox};
use sqd_data_core::{ChunkProcessor, PreparedChunk};
use sqd_dataset::DatasetDescriptionRef;
use sqd_primitives::BlockNumber;


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


pub struct LineProcessor {
    state: State,
    memory_threshold: usize,
}


impl LineProcessor {
    pub fn new(chain_builder: ChainBuilderBox) -> anyhow::Result<LineProcessor> {
        Ok(LineProcessor {
            state: State {
                processor: chain_builder.chunk_builder().new_chunk_processor()?,
                builder: chain_builder
            },
            memory_threshold: 40 * 1024 * 1024,
        })
    }

    pub fn push(&mut self, block_json: &[u8]) -> anyhow::Result<()> {
        self.state.builder.push(block_json)?;
        
        if self.state.builder.chunk_builder().byte_size() > self.memory_threshold {
            self.state.spill_on_disk()?;
        }
        Ok(())
    }

    pub fn last_block(&self) -> BlockNumber {
        self.state.builder.last_block_number()
    }

    pub fn last_block_hash(&self) -> &str {
        self.state.builder.last_block_hash()
    }

    pub fn last_parent_block_number(&self) -> BlockNumber {
        self.state.builder.last_parent_block_number()
    }

    pub fn last_parent_block_hash(&self) -> &str {
        self.state.builder.last_parent_block_hash()
    }

    pub fn buffered_bytes(&self) -> usize {
        let state = &self.state;
        state.builder.chunk_builder().byte_size() + state.processor.byte_size()
    }

    pub fn max_num_rows(&self) -> usize {
        let state = &self.state;
        // not precise, but should be OK practically
        state.builder.chunk_builder().max_num_rows() + state.processor.max_num_rows()
    }

    pub fn dataset_description(&self) -> DatasetDescriptionRef {
        self.state.builder.chunk_builder().dataset_description()
    }

    pub fn flush(&mut self) -> anyhow::Result<PreparedChunk> {
        let state = &mut self.state;

        if state.builder.chunk_builder().max_num_rows() > 0 {
            state.spill_on_disk()?;
        }

        let new_processor = state.builder.chunk_builder().new_chunk_processor()?;
        let processor = std::mem::replace(&mut state.processor, new_processor);
        let prepared_chunk = processor.finish()?;

        Ok(prepared_chunk)
    }
}