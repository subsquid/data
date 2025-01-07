use crate::layout::ChunkWriter;
use crate::processor::LineProcessor;
use crate::progress::Progress;
use crate::writer::WriterItem;
use crate::metrics;
use bytes::Bytes;
use std::num::NonZeroUsize;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;


pub struct Sink {
    processor: LineProcessor,
    chunk_writer: ChunkWriter,
    chunk_size: usize,
    progress: Progress,
    line_receiver: Receiver<Bytes>,
    chunk_sender: UnboundedSender<WriterItem>,
}


impl Sink {
    pub fn new(
        processor: LineProcessor,
        chunk_writer: ChunkWriter,
        chunk_size: usize,
        line_receiver: Receiver<bytes::Bytes>,
        chunk_sender: UnboundedSender<WriterItem>,
    ) -> Sink {
        let window_size = NonZeroUsize::new(10).unwrap();
        let granularity = Duration::from_secs(1);
        let progress = Progress::new(window_size, granularity);
        Sink {
            processor,
            chunk_writer,
            chunk_size,
            progress,
            line_receiver,
            chunk_sender,
        }
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        let mut first_block = self.chunk_writer.next_block();
        let mut last_report = Instant::now();

        let prev_chunk_hash = self.chunk_writer.prev_chunk_hash();
        if let Some(prev_chunk_hash) = prev_chunk_hash {
            let line = self.line_receiver.recv()?;
            self.processor.push(&line)?;
            let parent_hash = self.processor.last_parent_block_hash();
            assert!(prev_chunk_hash == short_hash(&parent_hash));
        }

        while let Ok(line) = self.line_receiver.recv() {
            self.processor.push(&line)?;
            metrics::LAST_BLOCK.inc_by(self.processor.last_block());

            if self.processor.buffered_bytes() > self.chunk_size * 1024 * 1024 {
                let (data, description) = self.processor.flush()?;
                let last_block = self.processor.last_block();
                let last_block_hash = self.processor.last_block_hash();
                let last_hash = short_hash(last_block_hash).to_string();
                let chunk = self.chunk_writer.next_chunk(first_block, last_block, last_hash);
                let item = WriterItem { description, data, chunk };
                first_block = last_block + 1;
                self.chunk_sender.send(item)?;
            }

            self.progress.set_current_value(self.processor.last_block());
            if last_report.elapsed() > Duration::from_secs(5) {
                self.report();
                last_report = Instant::now();
            }
        }

        if self.processor.max_num_rows() > 0 {
            let (data, description) = self.processor.flush()?;
            let last_block = self.processor.last_block();
            let last_block_hash = self.processor.last_block_hash();
            let last_hash = short_hash(last_block_hash).to_string();
            let chunk = self.chunk_writer.next_chunk(first_block, last_block, last_hash);
            let item = WriterItem { description, data, chunk };
            self.chunk_sender.send(item)?;
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
