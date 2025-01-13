use crate::chain_builder::{ChainBuilder, ChainBuilderBox};
use crate::ingest::ingest_from_service;
use crate::layout::ChunkWriter;
use crate::processor::LineProcessor;
use crate::progress::Progress;
use crate::writer::WriterItem;
use crate::metrics;
use bytes::Bytes;
use futures_util::TryStreamExt;
use sqd_data::solana::tables::SolanaChunkBuilder;
use std::num::NonZeroUsize;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;


pub struct Sink {
    builder: ChainBuilderBox,
    // processor: LineProcessor,
    // chunk_writer: ChunkWriter,
    chunk_size: usize,
    progress: Progress,
    builder_sender: std::sync::mpsc::Sender<ChainBuilderBox>,
}


impl Sink {
    pub fn new(
        builder: ChainBuilderBox,
        chunk_size: usize,
        builder_sender: std::sync::mpsc::Sender<ChainBuilderBox>,
    ) -> Sink {
        let window_size = NonZeroUsize::new(10).unwrap();
        let granularity = Duration::from_secs(1);
        let progress = Progress::new(window_size, granularity);
        Sink {
            builder,
            chunk_size,
            progress,
            builder_sender,
        }
    }

    pub async fn write(&mut self) -> anyhow::Result<()> {
        // let mut first_block = self.chunk_writer.next_block();
        let mut last_report = Instant::now();

        let stream = ingest_from_service("http://localhost:7373".parse().unwrap(), 220000000, Some(220001500));
        let mut stream = std::pin::pin!(stream);

        // let prev_chunk_hash = self.chunk_writer.prev_chunk_hash();
        // if let Some(prev_chunk_hash) = prev_chunk_hash {
        //     let line = stream.try_next().await?.unwrap();
        //     self.builder.push(&line)?;
        //     let parent_hash = self.builder.last_parent_block_hash();
        //     assert!(prev_chunk_hash == short_hash(&parent_hash));
        // }

        while let Some(line) = stream.try_next().await? {
            self.builder.push(&line)?;
            metrics::LAST_BLOCK.inc_by(self.builder.last_block_number());

            if self.builder.chunk_builder().byte_size() > 40 * 1024 * 1024 {
                let new_chunk_builder = Box::new(ChainBuilder::<SolanaChunkBuilder>::new(
                    self.builder.last_block_number(),
                    self.builder.last_block_hash().to_string(),
                    self.builder.last_parent_block_hash().to_string(),
                ));
                let old_chunk_builder = std::mem::replace(&mut self.builder, new_chunk_builder);
                self.builder_sender.send(old_chunk_builder)?;
            }

            // if self.processor.buffered_bytes() > self.chunk_size * 1024 * 1024 {
            //     let (data, description) = self.processor.flush()?;
            //     let last_block = self.processor.last_block();
            //     let last_block_hash = self.processor.last_block_hash();
            //     let last_hash = short_hash(last_block_hash).to_string();
            //     let chunk = self.chunk_writer.next_chunk(first_block, last_block, last_hash);
            //     let item = WriterItem { description, data, chunk };
            //     first_block = last_block + 1;
            //     self.chunk_sender.send(item)?;
            // }

            self.progress.set_current_value(self.builder.last_block_number());
            if last_report.elapsed() > Duration::from_secs(5) {
                self.report();
                last_report = Instant::now();
            }
        }

        if self.builder.chunk_builder().max_num_rows() > 0 {
            let new_chunk_builder = Box::new(ChainBuilder::<SolanaChunkBuilder>::default());
            let old_chunk_builder = std::mem::replace(&mut self.builder, new_chunk_builder);
            self.builder_sender.send(old_chunk_builder)?;
            // let (data, description) = self.processor.flush()?;
            // let last_block = self.processor.last_block();
            // let last_block_hash = self.processor.last_block_hash();
            // let last_hash = short_hash(last_block_hash).to_string();
            // let chunk = self.chunk_writer.next_chunk(first_block, last_block, last_hash);
            // let item = WriterItem { description, data, chunk };
            // self.chunk_sender.send(item)?;
        }

        if self.progress.has_news() {
            self.report();
        }

        Ok(())
    }

    fn report(&mut self) {
        let speed = self.progress.speed();
        metrics::PROGRESS.set(speed);
        tracing::info!(
            "last block: {}, progress: {} blocks/sec",
            self.progress.get_current_value(),
            speed.round(),
        );
    }
}


fn short_hash(value: &str) -> &str {
    &value[value.len().saturating_sub(5)..]
}
