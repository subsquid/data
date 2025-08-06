use crate::ingest::ingest_from_service;
use crate::layout::ChunkWriter;
use crate::metrics;
use crate::processor::LineProcessor;
use crate::progress::Progress;
use crate::writer::WriterItem;
use futures::TryStreamExt;
use prometheus_client::metrics::gauge::Atomic;
use sqd_primitives::BlockNumber;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::time::{Duration, Instant, UNIX_EPOCH};
use url::Url;


pub struct Sink {
    processor: LineProcessor,
    chunk_writer: ChunkWriter,
    chunk_size: usize,
    max_num_rows: usize,
    progress: Progress,
    url: Url,
    block_stream_interval: Duration,
    last_block: Option<BlockNumber>,
    chunk_sender: tokio::sync::mpsc::Sender<WriterItem>,
}


impl Sink {
    pub fn new(
        processor: LineProcessor,
        chunk_writer: ChunkWriter,
        chunk_size: usize,
        max_num_rows: usize,
        url: Url,
        block_stream_interval: Duration,
        last_block: Option<BlockNumber>,
        chunk_sender: tokio::sync::mpsc::Sender<WriterItem>,
    ) -> Sink {
        let window_size = NonZeroUsize::new(10).unwrap();
        let granularity = Duration::from_secs(1);
        let progress = Progress::new(window_size, granularity);
        Sink {
            processor,
            chunk_writer,
            chunk_size,
            max_num_rows,
            chunk_sender,
            progress,
            url,
            block_stream_interval,
            last_block,
        }
    }

    pub async fn r#loop(&mut self) -> anyhow::Result<()> {
        let mut chunk_first_block = self.chunk_writer.next_block();
        let mut next_block = self.chunk_writer.next_block();
        let mut last_report = Instant::now();

        'outer: loop {
            let mut stream = pin!(ingest_from_service(
                self.url.clone(),
                next_block,
                self.last_block
            ));
            let mut data_ingested = false;

            loop {
                let line = match stream.try_next().await {
                    Ok(Some(line)) => line,
                    Ok(None) => {
                        if let Some(last_block) = self.last_block {
                            if last_block == self.processor.last_block() {
                                break 'outer;
                            }
                        }
                        if !data_ingested {
                            tracing::info!(
                                "no blocks were found. waiting {} sec for a new try",
                                self.block_stream_interval.as_secs()
                            );
                            tokio::time::sleep(self.block_stream_interval).await;
                        }
                        continue 'outer;
                    }
                    Err(_) => {
                        tracing::error!("data streaming error, will pause for 5 sec and try again");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue 'outer;
                    }
                };

                self.processor.push(line.as_bytes())?;

                if chunk_first_block == next_block {
                    if let Some(prev_chunk_hash) = self.chunk_writer.prev_chunk_hash() {
                        let parent_hash = self.processor.last_parent_block_hash();
                        if prev_chunk_hash != short_hash(parent_hash) {
                            if prev_chunk_hash != fallback_short_hash(parent_hash) {
                                anyhow::bail!(
                                    "previous chunk hash {} does not match parent block hash {}",
                                    prev_chunk_hash,
                                    parent_hash
                                );
                            }
                        }
                    }
                }

                if self.processor.buffered_bytes() > self.chunk_size * 1024 * 1024
                    || self.processor.max_num_rows() >= self.max_num_rows
                {
                    self.submit_chunk(chunk_first_block).await?;
                    chunk_first_block = self.processor.last_block() + 1;
                }

                self.progress.set_current_value(self.processor.last_block());
                if last_report.elapsed() > Duration::from_secs(5) {
                    self.report();
                    last_report = Instant::now();
                }

                data_ingested = true;
                next_block = self.processor.last_block() + 1;

                let block_timestamp = self.processor.last_block_timestamp()
                    .map(|v| u64::try_from(v).unwrap() / 1000).unwrap_or(0);
                metrics::LATEST_BLOCK_TIMESTAMP.set(block_timestamp);
                metrics::LATEST_BLOCK.set(self.processor.last_block());
                metrics::LAST_BLOCK.inner().set(self.processor.last_block());
            }
        }

        if self.processor.max_num_rows() > 0 {
            self.submit_chunk(chunk_first_block).await?;
        }

        if self.progress.has_news() {
            self.report();
        }

        Ok(())
    }

    fn report(&mut self) {
        let speed = self.progress.speed();
        let block_timestamp = self.processor.last_block_timestamp();
        let processing_time = block_timestamp.map(|v| {
            let millis = u64::try_from(v).unwrap();
            let block_timestamp = UNIX_EPOCH + Duration::from_millis(millis);
            block_timestamp.elapsed().unwrap().as_secs_f64()
        }).unwrap_or(0.0);

        metrics::PROGRESS.set(speed);
        metrics::PROCESSING_TIME.set(processing_time);

        tracing::info!(
            "last block: {}, progress: {} blocks/sec",
            self.progress.get_current_value(),
            speed.round(),
        );
    }

    async fn submit_chunk(&mut self, first_block: BlockNumber) -> anyhow::Result<()> {
        let description = self.processor.dataset_description();
        let data = self.processor.flush()?;
        let last_block = self.processor.last_block();
        let last_block_hash = self.processor.last_block_hash();
        let last_hash = short_hash(last_block_hash).to_string();
        let chunk = self.chunk_writer.next_chunk(first_block, last_block, last_hash);
        let item = WriterItem { description, data, chunk };
        self.chunk_sender.send(item).await?;
        Ok(())
    }
}


fn short_hash(value: &str) -> &str {
    &value[value.len().saturating_sub(8)..]
}


fn fallback_short_hash(value: &str) -> &str {
    &value[2..10]
}
