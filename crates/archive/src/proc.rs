use crate::chunk_writer::ChunkWriter;
use crate::layout::ChunkTracker;
use crate::metrics;
use crate::progress::Progress;
use crate::writer::WriterItem;
use anyhow::ensure;
use futures::{Stream, TryStreamExt};
use prometheus_client::metrics::gauge::Atomic;
use sqd_data_core::BlockChunkBuilder;
use sqd_primitives::{Block, BlockNumber, DataMask};
use std::num::NonZeroUsize;
use std::pin::pin;
use std::time::{Duration, Instant};
use tracing::{enabled, info, Level};


pub struct Proc<B> {
    chunk_writer: ChunkWriter<B>,
    chunk_tracker: ChunkTracker,
    chunk_sender: tokio::sync::mpsc::Sender<WriterItem>,
    max_chunk_size_mb: usize,
    max_num_rows: usize,
    mask: DataMask,
    blocks_buffered: usize,
    first_block: BlockNumber,
    last_block: BlockNumber,
    last_block_hash: String,
    last_block_timestamp: i64,
    progress: Progress,
    last_progress_report: Instant
}


impl<B: BlockChunkBuilder<Block: Block>> Proc<B> {
    pub fn new(
        chunk_builder: B,
        chunk_tracker: ChunkTracker,
        chunk_sender: tokio::sync::mpsc::Sender<WriterItem>
    ) -> anyhow::Result<Self>
    {
        let chunk_writer = ChunkWriter::new(chunk_builder)?;
        let first_block = chunk_tracker.next_block();
        Ok(Self {
            chunk_writer,
            chunk_tracker,
            chunk_sender,
            max_chunk_size_mb: 4096,
            max_num_rows: 200_000,
            mask: DataMask::default(),
            blocks_buffered: 0,
            first_block,
            last_block: 0,
            last_block_hash: String::new(),
            last_block_timestamp: 0,
            progress: Progress::new(NonZeroUsize::new(10).unwrap(), Duration::from_secs(1)),
            last_progress_report: Instant::now()
        })
    }

    pub fn set_max_chunk_size(&mut self, mb: usize) {
        self.max_chunk_size_mb = mb;
    }

    pub fn set_max_num_rows(&mut self, count: usize) {
        self.max_num_rows = count;
    }

    pub async fn run(mut self, block_stream: impl Stream<Item = anyhow::Result<B::Block>>) -> anyhow::Result<()> {
        let mut block_stream = pin!(block_stream);
        let mut start = true;

        while let Some(block) = block_stream.try_next().await? {
            if start {
                start = false;
                if let Some(hash) = self.chunk_tracker.prev_chunk_hash() {
                    ensure!(
                    hash == short_hash(block.parent_hash()) || hash == fallback_short_hash(block.parent_hash()),
                    "previous chunk hash {} does not match parent hash {} of block {}",
                    hash,
                    block.parent_hash(),
                    block.number()
                );
                }
            } else {
                ensure!(
                    self.last_block_hash == block.parent_hash(),
                    "parent hash mismatch for block {}: expected {}, but got {}",
                    block.number(),
                    self.last_block_hash,
                    block.parent_hash()
                );
            }

            if self.mask != block.data_availability_mask() {
                self.submit_chunk().await?;
                self.mask = block.data_availability_mask();
            }

            self.chunk_writer.push(&block)?;
            self.last_block = block.number();
            self.last_block_hash.clear();
            self.last_block_hash.push_str(block.hash());
            self.last_block_timestamp = block.timestamp().unwrap_or(0);
            self.blocks_buffered += 1;

            if self.chunk_writer.buffered_bytes() > self.max_chunk_size_mb * 1024 * 1024
                || self.chunk_writer.max_num_rows() > self.max_num_rows
            {
                self.submit_chunk().await?
            }

            self.register_progress()
        }

        if self.progress.has_news() {
            self.report_progress()
        }

        Ok(())
    }

    async fn submit_chunk(&mut self) -> anyhow::Result<()> {
        if self.blocks_buffered == 0 {
            return Ok(())
        }

        let mut data = self.chunk_writer.finish()?;
        data.retain(|name, _| B::Block::has_data(self.mask, *name));

        let chunk = self.chunk_tracker.next_chunk(
            self.first_block,
            self.last_block,
            short_hash(&self.last_block_hash).to_string()
        );

        self.chunk_sender.send(
            WriterItem {
                data,
                chunk,
                description: self.chunk_writer.dataset_description()
            }
        ).await?;

        self.blocks_buffered = 0;
        self.first_block = self.last_block + 1;
        Ok(())
    }

    fn register_progress(&mut self) {
        metrics::LATEST_BLOCK_TIMESTAMP.set(self.last_block_timestamp / 1000);
        metrics::LATEST_BLOCK.set(self.last_block);
        metrics::LAST_BLOCK.inner().set(self.last_block);
        self.progress.set_current_value(self.last_block);
        metrics::PROGRESS.set(self.progress.speed());
        if enabled!(Level::INFO) && self.last_progress_report.elapsed() > Duration::from_secs(5) {
            self.report_progress();
            self.last_progress_report = Instant::now();
        }
    }

    fn report_progress(&mut self) {
        info!(
            "last block: {}, progress: {} blocks/sec",
            self.progress.get_current_value(),
            self.progress.speed().round(),
        );
    }
}


fn short_hash(value: &str) -> &str {
    let offset = value.len().saturating_sub(8);
    value.get(offset..).unwrap_or_default()
}


fn fallback_short_hash(value: &str) -> &str {
    value.get(2..10).unwrap_or_default()
}