use crate::ingest::write_controller::Rollback;
use anyhow::ensure;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
use sqd_data_client::DataClient;
use sqd_data_core::{BlockChunkBuilder, ChunkProcessor, PreparedChunk, PreparedTable};
use sqd_data_source::{DataEvent, DataSource};
use sqd_primitives::{Block, BlockNumber, BlockRef, DisplayBlockRefOption, Name};
use std::fmt::{Display, Formatter};
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


impl Display for NewChunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f, 
            "{}-{}-{} (finalized_head = {})", 
            self.first_block, 
            self.last_block, 
            &self.last_block_hash,
            DisplayBlockRefOption(self.finalized_head.as_ref())
        )
    }
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
    data_source: DC,
    builder: Option<DataBuilder<CB>>,
    finalized_head: Option<BlockRef>,
    buffered_blocks: usize,
    parent_block_hash: String,
    first_block: BlockNumber,
    last_block: BlockNumber,
    last_block_hash: String
}


impl<DS, CB> IngestGeneric<DS, CB>
where
    DS: DataSource,
    CB: BlockChunkBuilder<Block = DS::Block> + Send + 'static
{
    pub fn new(
        mut data_source: DS,
        chunk_builder: CB,
        first_block: BlockNumber,
        parent_block_hash: Option<String>,
        message_sender: tokio::sync::mpsc::Sender<IngestMessage>
    ) -> Self
    {
        data_source.set_position(first_block, parent_block_hash);
        Self {
            message_sender,
            data_source,
            builder: Some(DataBuilder::new(chunk_builder)),
            finalized_head: None,
            buffered_blocks: 0,
            parent_block_hash: String::new(),
            first_block,
            last_block: 0,
            last_block_hash: String::new()
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        while let Some(event) = self.data_source.next().await {
            match event {
                DataEvent::FinalizedHead(head) => {
                    self.set_finalized_head(head.number, &head.hash);
                    if head.number < self.first_block {
                        self.message_sender.send(
                            IngestMessage::FinalizedHead(head)
                        ).await?;
                    }
                },
                DataEvent::Block { block, is_final } => {
                    self.push_block(block, is_final)?;
                    self.maybe_flush().await?
                },
                DataEvent::Fork(prev_blocks) => {
                    self.handle_fork(prev_blocks).await?
                },
                DataEvent::MaybeOnHead => {
                    self.flush().await?
                }
            }
        }
        Ok(())
    }

    async fn handle_fork(&mut self, prev_blocks: Vec<BlockRef>) -> anyhow::Result<()> {
        let (rollback_sender, rollback_recv) = tokio::sync::oneshot::channel();

        self.message_sender.send(IngestMessage::Fork {
            prev_blocks,
            rollback_sender
        }).await?;

        self.with_blocking_builder(|b| b.clear()).await?;
        let rollback = rollback_recv.await?;

        self.buffered_blocks = 0;
        self.first_block = rollback.first_block;
        self.data_source.set_position(rollback.first_block, rollback.parent_block_hash);

        Ok(())
    }

    fn push_block(&mut self, block: CB::Block, is_final: bool) -> anyhow::Result<()> {
        self.builder.as_mut().unwrap().push_block(&block);
        if self.buffered_blocks == 0 {
            self.parent_block_hash.clear();
            self.parent_block_hash.push_str(block.parent_hash());
        } else {
            ensure!(
                self.last_block_hash == block.parent_hash(),
                "chain continuity was violated around block {}#{}",
                block.number(),
                block.hash()
            );
        }
        self.buffered_blocks += 1;
        self.last_block = block.number();
        self.last_block_hash.clear();
        self.last_block_hash.push_str(block.hash());
        if is_final {
            self.set_finalized_head(block.number(), block.hash());
        }
        Ok(())
    }

    async fn maybe_flush(&mut self) -> anyhow::Result<()> {
        if self.builder_ref().num_rows() > 200_000 {
            return self.flush().await;
        }
        if self.builder_ref().in_memory_buffered_bytes() > 30 * 1024 * 1024 {
            return self.with_blocking_builder(|b| b.flush_to_processor()).await;
        }
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        if self.buffered_blocks == 0 {
            return Ok(())
        }

        let tables = self.with_blocking_builder(|b| b.finish()).await?;

        let parent_block_hash = self.parent_block_hash.clone();
        let first_block = self.first_block;
        let last_block = self.last_block;
        let last_block_hash = self.last_block_hash.clone();

        self.buffered_blocks = 0;
        self.first_block = last_block + 1;

        self.message_sender.send(IngestMessage::NewChunk(NewChunk {
            finalized_head: self.finalized_head.clone(),
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

    fn set_finalized_head(&mut self, number: BlockNumber, hash: &str) {
        if let Some(current) = self.finalized_head.as_mut() {
            current.number = number;
            current.hash.clear();
            current.hash.push_str(hash);
        } else {
            self.finalized_head = Some(BlockRef {
                number,
                hash: hash.to_string()
            })
        }
    }
}