use sqd_data_core::{BlockChunkBuilder, ChunkProcessor, PreparedChunk};
use sqd_dataset::DatasetDescriptionRef;


pub struct ChunkWriter<B> {
    chunk_builder: B,
    processor: ChunkProcessor,
    memory_threshold: usize,
}


impl<B: BlockChunkBuilder> ChunkWriter<B> {
    pub fn new(chunk_builder: B) -> anyhow::Result<Self> {
        Ok(Self {
            processor: chunk_builder.new_chunk_processor()?,
            chunk_builder,
            memory_threshold: 40 * 1024 * 1024,
        })
    }

    pub fn push(&mut self, block: &B::Block) -> anyhow::Result<()> {
        self.chunk_builder.push(block)?;
        if self.chunk_builder.byte_size() > self.memory_threshold {
            self.spill_on_disk()?;
        }
        Ok(())
    }

    fn spill_on_disk(&mut self) -> anyhow::Result<()> {
        self.chunk_builder.submit_to_processor(&mut self.processor)?;
        self.chunk_builder.clear();
        Ok(())
    }

    pub fn buffered_bytes(&self) -> usize {
        self.chunk_builder.byte_size() + self.processor.byte_size()
    }

    pub fn max_num_rows(&self) -> usize {
        // not precise, but should be OK practically
        self.chunk_builder.max_num_rows() + self.processor.max_num_rows()
    }

    pub fn dataset_description(&self) -> DatasetDescriptionRef {
        self.chunk_builder.dataset_description()
    }

    pub fn finish(&mut self) -> anyhow::Result<PreparedChunk> {
        if self.chunk_builder.max_num_rows() > 0 {
            self.spill_on_disk()?;
        }
        let new_processor = self.chunk_builder.new_chunk_processor()?;
        std::mem::replace(&mut self.processor, new_processor).finish()
    }
}