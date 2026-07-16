use std::{
    fmt::{Display, Formatter},
    time::Instant
};

use anyhow::{anyhow, ensure};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use sqd_data_core::{BlockChunkBuilder, ChunkProcessor, PreparedChunk};
use sqd_data_source::{DataEvent, DataSource};
use sqd_primitives::{Block, BlockNumber, BlockRef, DataMask, DisplayBlockRefOption};
use sqd_storage::db::DatasetId;
use tracing::{debug, field::valuable, info};

use crate::{
    dataset_controller::write_controller::Rollback,
    metrics::{WriteStage, report_write_duration}
};

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
    pub first_block_time: Option<i64>,
    pub last_block_time: Option<i64>,
    pub tables: PreparedChunk
}

impl Display for NewChunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{} with finalized_head = {}",
            self.first_block,
            self.last_block,
            &self.last_block_hash,
            DisplayBlockRefOption(self.finalized_head.as_ref())
        )
    }
}

/// `maybe_flush` spills builder contents to the processor beyond this size.
const SPILL_BOUND_BYTES: usize = 30 * 1024 * 1024;

struct DataBuilder<CB> {
    builder: CB,
    processor: Option<ChunkProcessor>
}

impl<CB: BlockChunkBuilder> DataBuilder<CB> {
    pub fn new(builder: CB) -> Self {
        Self {
            builder,
            processor: None
        }
    }

    pub fn push_block(&mut self, block: &CB::Block) -> anyhow::Result<()> {
        self.builder.push(block)
    }

    pub fn num_rows(&self) -> usize {
        self.builder.max_num_rows() + self.processor.as_ref().map_or(0, |p| p.max_num_rows())
    }

    pub fn in_memory_buffered_bytes(&self) -> usize {
        self.builder.byte_size()
    }

    pub fn flush_to_processor(&mut self) -> anyhow::Result<()> {
        if self.processor.is_none() {
            let processor = self.builder.new_chunk_processor()?;
            self.processor = Some(processor);
        }
        self.builder.submit_to_processor(self.processor.as_mut().unwrap())?;
        self.builder.clear();
        Ok(())
    }

    pub fn finish(&mut self) -> anyhow::Result<PreparedChunk> {
        if self.processor.is_none() && self.builder.byte_size() <= SPILL_BOUND_BYTES {
            // no spill and within the spill bound — skip the temp files. The bound is
            // re-checked here: a row-count-triggered flush can carry an oversized final
            // block that maybe_flush's byte check never saw.
            return self.builder.prepare_in_memory();
        }
        self.flush_to_processor()?;
        self.processor.take().unwrap().finish()
    }

    pub fn clear(&mut self) {
        self.builder.clear();
        self.processor = None;
    }
}

pub struct IngestGeneric<DC, CB> {
    dataset_id: DatasetId,
    message_sender: tokio::sync::mpsc::Sender<IngestMessage>,
    data_source: DC,
    builder: Option<DataBuilder<CB>>,
    finalized_head: Option<BlockRef>,
    buffered_blocks: usize,
    parent_block_hash: String,
    first_block: BlockNumber,
    first_block_time: Option<i64>,
    last_block: BlockNumber,
    last_block_hash: String,
    last_block_time: Option<i64>,
    data_mask: DataMask
}

impl<DS, CB> IngestGeneric<DS, CB>
where
    DS: DataSource,
    CB: BlockChunkBuilder<Block = DS::Block> + Send + 'static
{
    pub fn new(
        dataset_id: DatasetId,
        data_source: DS,
        chunk_builder: CB,
        message_sender: tokio::sync::mpsc::Sender<IngestMessage>
    ) -> Self {
        let first_block = data_source.get_next_block();
        Self {
            dataset_id,
            message_sender,
            data_source,
            builder: Some(DataBuilder::new(chunk_builder)),
            finalized_head: None,
            buffered_blocks: 0,
            parent_block_hash: String::new(),
            first_block,
            first_block_time: None,
            last_block: 0,
            last_block_hash: String::new(),
            last_block_time: None,
            data_mask: DataMask::default()
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        while let Some(event) = self.data_source.next().await {
            match event {
                DataEvent::FinalizedHead(head) => {
                    self.set_finalized_head(head.number, &head.hash);
                    if head.number < self.first_block {
                        self.message_sender.send(IngestMessage::FinalizedHead(head)).await?;
                    }
                }
                DataEvent::Block { block, is_final } => {
                    let data_mask = block.data_availability_mask();
                    if self.data_mask != data_mask {
                        self.flush().await?;
                        self.data_mask = data_mask
                    }
                    self.push_block(block, is_final)?;
                    self.maybe_flush().await?
                }
                DataEvent::Fork(prev_blocks) => self.handle_fork(prev_blocks).await?,
                DataEvent::MaybeOnHead => self.flush().await?
            }
        }
        Ok(())
    }

    async fn handle_fork(&mut self, prev_blocks: Vec<BlockRef>) -> anyhow::Result<()> {
        info!(upstream_blocks = valuable(&prev_blocks), "fork received");

        let (rollback_sender, rollback_recv) = tokio::sync::oneshot::channel();

        self.message_sender
            .send(IngestMessage::Fork {
                prev_blocks,
                rollback_sender
            })
            .await?;

        self.with_blocking_builder(|b| b.clear()).await;
        let rollback = rollback_recv.await?;

        info!(
            block_number = rollback.first_block,
            parent_block_hash =? rollback.parent_block_hash,
            "resetting ingest position"
        );

        self.buffered_blocks = 0;
        self.finalized_head = None;
        self.first_block = rollback.first_block;
        self.data_source
            .set_position(rollback.first_block, rollback.parent_block_hash.as_deref());

        Ok(())
    }

    fn push_block(&mut self, block: CB::Block, is_final: bool) -> anyhow::Result<()> {
        self.builder.as_mut().unwrap().push_block(&block)?;
        if self.buffered_blocks == 0 {
            self.parent_block_hash.clear();
            self.parent_block_hash.push_str(block.parent_hash());
            self.first_block_time = block.timestamp();
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
        self.last_block_time = block.timestamp();
        if is_final {
            self.set_finalized_head(block.number(), block.hash());
        }
        Ok(())
    }

    async fn maybe_flush(&mut self) -> anyhow::Result<()> {
        if self.builder_ref().num_rows() > 200_000 {
            return self.flush().await;
        }
        if self.builder_ref().in_memory_buffered_bytes() > SPILL_BOUND_BYTES {
            return self.with_blocking_builder(|b| b.flush_to_processor()).await;
        }
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        if self.buffered_blocks == 0 {
            return Ok(());
        }

        let parent_block_hash = self.parent_block_hash.clone();
        let first_block = self.first_block;
        let last_block = self.last_block;
        let last_block_hash = self.last_block_hash.clone();

        let last_block_time = DateTime::<Utc>::from_timestamp_millis(self.last_block_time.unwrap_or(0))
            .ok_or_else(|| anyhow!("block time is out of range"))?;

        debug!(
            first_block = first_block,
            last_block = last_block,
            last_block_hash = %last_block_hash,
            last_block_time = %last_block_time.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            finalized_head = valuable(&self.finalized_head),
            "received new chunk"
        );

        let started = Instant::now();
        let tables = self.with_blocking_builder(|b| b.finish()).await;
        report_write_duration(self.dataset_id, WriteStage::Prepare, started.elapsed(), tables.is_ok());
        let mut tables = tables?;

        tables.retain(|&name, _| CB::Block::has_data(self.data_mask, name));

        self.buffered_blocks = 0;
        self.first_block = last_block + 1;

        self.message_sender
            .send(IngestMessage::NewChunk(NewChunk {
                finalized_head: self.finalized_head.clone(),
                parent_block_hash,
                first_block,
                last_block,
                last_block_hash,
                first_block_time: self.first_block_time,
                last_block_time: self.last_block_time,
                tables
            }))
            .await?;

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
        })
        .await
        .unwrap();

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

#[cfg(test)]
mod tests {
    use sqd_data::hyperliquid_fills::{model::Block, tables::HyperliquidFillsChunkBuilder};

    use super::*;

    fn fills_block() -> Block {
        serde_json::from_value(serde_json::json!({
            "header": {
                "number": 10_000,
                "hash": "0xabc",
                "parentHash": "0xabb",
                "timestamp": 1_760_000_000_000i64
            },
            "fills": [{
                "fillIndex": 0,
                "user": "0x1111111111111111111111111111111111111111",
                "coin": "BTC",
                "px": 123.25,
                "sz": 0.5,
                "side": "B",
                "time": 1_760_000_000_000i64,
                "startPosition": 1.25,
                "dir": "Open Long",
                "closedPnl": 0.25,
                "hash": "0x2222222222222222222222222222222222222222",
                "oid": 42,
                "crossed": true,
                "fee": 0.01,
                "tid": 43,
                "feeToken": "USDC",
                "cloid": "x".repeat(64 * 1024)
            }]
        }))
        .unwrap()
    }

    fn unspilled(over_bytes: usize) -> DataBuilder<HyperliquidFillsChunkBuilder> {
        let mut b = DataBuilder::new(HyperliquidFillsChunkBuilder::new());
        let block = fills_block();
        while b.in_memory_buffered_bytes() <= over_bytes {
            b.push_block(&block).unwrap();
        }
        b
    }

    /// A row-count-triggered flush can hand `finish` an unspilled builder above the spill
    /// bound — `maybe_flush`'s byte check never saw the final block. Such a chunk must
    /// spill instead of being copied in memory. The two paths are told apart by
    /// `into_processor`, which in-memory tables refuse.
    #[test]
    fn oversized_unspilled_chunk_spills_at_finish() {
        let mut b = unspilled(SPILL_BOUND_BYTES);
        let mut chunk = b.finish().unwrap();
        let (_, table) = chunk.pop_first().unwrap();
        assert!(table.into_processor().is_ok(), "expected the spill path");
    }

    #[test]
    fn bounded_unspilled_chunk_is_prepared_in_memory() {
        let mut b = unspilled(1);
        let mut chunk = b.finish().unwrap();
        let (_, table) = chunk.pop_first().unwrap();
        assert!(table.into_processor().is_err(), "expected the in-memory path");
    }
}
