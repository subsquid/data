use crate::ingest::write_controller::Rollback;
use anyhow::ensure;
use futures::{SinkExt, TryStreamExt};
use parking_lot::Mutex;
use sqd_data_client::{BlockStream, BlockStreamRequest, DataClient};
use sqd_data_core::{BlockChunkBuilder, ChunkProcessor, PreparedChunk, PreparedTable};
use sqd_primitives::{Block, BlockNumber, BlockRef, Name};
use std::ops::DerefMut;
use std::sync::{Arc, Weak};


pub enum IngestMessage {
    FinalizedHead(BlockRef),
    NewChunk(NewChunk),
    Fork {
        prev_blocks: Vec<BlockRef>,
        rollback_sender: tokio::sync::oneshot::Sender<Rollback>
    }
}


pub struct NewChunk {
    pub finalized_head: Option<BlockRef>,
    pub parent_block_hash: String,
    pub first_block: BlockNumber,
    pub last_block: BlockNumber,
    pub last_block_hash: String,
    pub tables: PreparedTables
}


pub struct PreparedTables {
    inner: PreparedChunk,
    spare_cell: Weak<Mutex<Option<ChunkProcessor>>>
}


impl PreparedTables {
    pub fn iter_tables_mut(&mut self) -> impl Iterator<Item = (Name, &mut PreparedTable)> {
        self.inner.tables.iter_mut().map(|(name, table)| (*name, table))
    }
}


impl Drop for PreparedTables {
    fn drop(&mut self) {
        if let Some(cell) = self.spare_cell.upgrade() {
            let mut lock = cell.lock();
            if lock.is_some() {
                return
            }
            *lock = std::mem::take(&mut self.inner).into_processor().ok()
        }
    }
}


struct DataBuilder<CB> {
    builder: CB,
    processor: ChunkProcessor,
    spare_processor: Arc<Mutex<Option<ChunkProcessor>>>,
}


impl<CB: BlockChunkBuilder> DataBuilder<CB> {
    pub fn new(builder: CB) -> Self {
        Self {
            processor: builder.new_chunk_processor(),
            builder,
            spare_processor: Arc::new(Mutex::new(None))
        }
    }

    pub fn push_block(&mut self, block: &CB::Block) {
        self.builder.push(block)
    }

    pub fn num_rows(&self) -> usize {
        self.builder.max_num_rows() + self.processor.max_num_rows()
    }

    pub fn in_memory_buffered_bytes(&self) -> usize {
        self.builder.byte_size()
    }

    pub fn flush_to_processor(&mut self) -> anyhow::Result<()> {
        self.builder.submit_to_processor(&mut self.processor)?;
        self.builder.clear();
        Ok(())
    }

    pub fn finish(&mut self) -> anyhow::Result<PreparedTables> {
        self.flush_to_processor()?;

        let spare_processor = std::mem::take(self.spare_processor.lock().deref_mut())
            .unwrap_or_else(|| self.builder.new_chunk_processor());

        let processor = std::mem::replace(&mut self.processor, spare_processor);
        let prepared_chunk = processor.finish()?;

        Ok(PreparedTables {
            inner: prepared_chunk,
            spare_cell: Arc::downgrade(&self.spare_processor)
        })
    }
    
    pub fn clear(&mut self) -> anyhow::Result<()> {
        self.builder.clear();
        if self.processor.max_num_rows() > 0 {
            let _ = self.finish()?;
        }
        Ok(())
    }
}


pub struct IngestGeneric<DC, CB> {
    message_sender: tokio::sync::mpsc::Sender<IngestMessage>,
    data_client: DC,
    builder: Option<DataBuilder<CB>>,
    buffered_blocks: usize,
    has_parent_block_hash: bool,
    parent_block_hash: String,
    first_block: BlockNumber,
    last_block: BlockNumber,
    last_block_hash: String
}


impl<DC, CB> IngestGeneric<DC, CB>
where
    DC: DataClient,
    DC::BlockStream: Unpin,
    CB: BlockChunkBuilder<Block=<DC::BlockStream as BlockStream>::Block> + Send + 'static
{
    pub fn new(
        data_client: DC,
        chunk_builder: CB,
        first_block: BlockNumber,
        prev_block_hash: Option<&str>,
        message_sender: tokio::sync::mpsc::Sender<IngestMessage>
    ) -> Self
    {
        Self {
            message_sender,
            data_client,
            builder: Some(DataBuilder::new(chunk_builder)),
            buffered_blocks: 0,
            has_parent_block_hash: prev_block_hash.is_some(),
            parent_block_hash: prev_block_hash.map(|s| s.to_string()).unwrap_or_default(),
            first_block,
            last_block: 0,
            last_block_hash: String::new()
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            let mut req = BlockStreamRequest::new(self.first_block);
            if self.has_parent_block_hash {
                req = req.with_prev_block_hash(self.parent_block_hash.as_ref())
            }

            let mut stream = self.data_client.stream(req).await?;

            if stream.prev_blocks().is_empty() {
                let finalized_head = stream.take_finalized_head()?;

                if let Some(finalized_head) = finalized_head.clone() {
                    self.message_sender.send(
                        IngestMessage::FinalizedHead(finalized_head)
                    ).await?;
                }

                if let Some(block) = stream.try_next().await? {
                    if self.has_parent_block_hash {
                        ensure!(block.parent_hash() == self.parent_block_hash.as_str());
                    } else {
                        self.parent_block_hash.clear();
                        self.parent_block_hash.push_str(block.parent_hash());
                        self.has_parent_block_hash = true;
                    }
                    
                    self.push_block(block);
                    self.maybe_flush(finalized_head.as_ref()).await?;

                    while let Some(block) = stream.try_next().await? {
                        ensure!(block.parent_hash() == self.last_block_hash.as_str());
                        self.push_block(block);
                        self.maybe_flush(finalized_head.as_ref()).await?;
                    }
                }

                if finalized_head.as_ref().map_or(true, |h| h.number <= self.last_block) {
                    self.flush(finalized_head.as_ref()).await?
                }
            } else {
                self.handle_rollback(stream.take_prev_blocks()).await?
            }
        }
    }

    async fn handle_rollback(&mut self, prev_blocks: Vec<BlockRef>) -> anyhow::Result<()> {
        ensure!(
            self.buffered_blocks == 0,
            "attempt to rollback beyond finalized head"
        );
        ensure!(
            self.has_parent_block_hash,
            "data service returned rollback while no base block was given"
        );

        let (rollback_sender, rollback_recv) = tokio::sync::oneshot::channel();

        self.message_sender.send(IngestMessage::Fork {
            prev_blocks,
            rollback_sender
        }).await?;

        self.with_blocking_builder(|b| b.clear()).await?;

        let rollback = rollback_recv.await?;
        
        self.first_block = rollback.first_block;
        self.has_parent_block_hash = rollback.parent_block_hash.is_some();
        self.parent_block_hash = rollback.parent_block_hash.unwrap_or_default();

        Ok(())
    }

    fn push_block(&mut self, block: CB::Block) {
        self.buffered_blocks += 1;
        self.builder.as_mut().unwrap().push_block(&block);
        self.last_block = block.number();
        self.last_block_hash.clear();
        self.last_block_hash.push_str(block.hash());
    }

    async fn maybe_flush(&mut self, finalized_head: Option<&BlockRef>) -> anyhow::Result<()> {
        if self.builder_ref().num_rows() > 200_000_000 {
            return self.flush(finalized_head).await;
        }

        if self.builder_ref().in_memory_buffered_bytes() > 30 * 1024 * 1024 {
            return self.with_blocking_builder(|b| b.flush_to_processor()).await;
        }

        Ok(())
    }

    async fn flush(&mut self, finalized_head: Option<&BlockRef>) -> anyhow::Result<()> {
        if self.buffered_blocks == 0 {
            return Ok(())
        }

        let tables = self.with_blocking_builder(|b| b.finish()).await?;

        let parent_block_hash = self.parent_block_hash.clone();
        let first_block = self.first_block;
        let last_block = self.last_block;
        let last_block_hash = self.last_block_hash.clone();

        self.first_block = self.last_block + 1;
        std::mem::swap(&mut self.parent_block_hash, &mut self.last_block_hash);
        self.has_parent_block_hash = true;
        self.buffered_blocks = 0;

        self.message_sender.send(IngestMessage::NewChunk(NewChunk {
            finalized_head: finalized_head.cloned(),
            parent_block_hash,
            first_block,
            last_block,
            last_block_hash,
            tables
        })).await?;

        Ok(())
    }

    async fn with_blocking_builder<R, F>(&mut self, cb: F) -> R
    where
        F: FnOnce(&mut DataBuilder<CB>) -> R + Send + 'static,
        R: Send + 'static
    {
        let mut builder = std::mem::take(&mut self.builder).unwrap();

        let (result, builder) = tokio::task::spawn_blocking(move || {
            let result = cb(&mut builder);
            (result, builder)
        }).await.unwrap();

        self.builder = Some(builder);

        result
    }

    fn builder_ref(&self) -> &DataBuilder<CB> {
        self.builder.as_ref().unwrap()
    }
}
