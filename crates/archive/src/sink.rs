use crate::fs::Fs;
use crate::layout::ChunkWriter;
use crate::writer::ParquetWriter;
use crate::progress::Progress;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};


pub struct Sink<'a> {
    writer: ParquetWriter,
    chunk_writer: ChunkWriter,
    fs: &'a Box<dyn Fs>,
    chunk_size: usize,
    progress: Progress,
}


impl<'a> Sink<'a> {
    pub fn new(
        writer: ParquetWriter,
        chunk_writer: ChunkWriter,
        fs: &'a Box<dyn Fs>,
        chunk_size: usize,
    ) -> Sink<'a> {
        let window_size = NonZeroUsize::new(10).unwrap();
        let granularity = Duration::from_secs(1);
        let progress = Progress::new(window_size, granularity);
        Sink {
            writer,
            chunk_writer,
            fs,
            chunk_size,
            progress,
        }
    }

    pub fn write(&mut self, stream: impl Iterator<Item = anyhow::Result<String>>) -> anyhow::Result<()> {
        let mut first_block = None;
        let mut last_block = None;
        let mut last_hash = None;
        let mut last_report = Instant::now();

        for result in stream {
            let line = result?;
            let header = self.writer.push(&line)?;

            if let Some(last_hash) = &last_hash {
                let block_hash = short_hash(&header.hash);
                let block_parent_hash = short_hash(&header.parent_hash);
                if last_hash != block_parent_hash {
                    anyhow::bail!(
                        "broken chain: block {}#{} is not a direct child of {}#{}",
                        header.height,
                        block_hash,
                        header.height - 1,
                        last_hash,
                    );
                }
            }

            if first_block.is_none() {
                first_block = Some(header.height);
            }
            last_block = Some(header.height);
            last_hash = Some(short_hash(&header.hash).to_string());

            if self.writer.buffered_bytes() > self.chunk_size * 1024 * 1024 {
                let first_block = first_block.take().unwrap();
                let last_block = last_block.unwrap();
                let last_hash = last_hash.as_ref().unwrap();
                let chunk = self.chunk_writer.next_chunk(first_block, last_block, last_hash)?;
                self.writer.flush(self.fs.cd(&[&chunk.path()]))?;
            }

            self.progress.set_current_value(header.height);
            if last_report.elapsed() > Duration::from_secs(5) {
                self.report();
                last_report = Instant::now();
            }
        }

        if self.writer.buffered_bytes() > 0 {
            let first_block = first_block.take().unwrap();
            let last_block = last_block.unwrap();
            let last_hash = last_hash.as_ref().unwrap();
            let chunk = self.chunk_writer.next_chunk(first_block, last_block, last_hash)?;
            self.writer.flush(self.fs.cd(&[&chunk.path()]))?;
        }

        if self.progress.has_news() {
            self.report();
        }

        Ok(())
    }

    fn report(&mut self) {
        tracing::info!(
            "last block: {}, progress: {} blocks/sec",
            self.progress.get_current_value(),
            self.progress.speed().round(),
        );
    }
}


fn short_hash(value: &str) -> &str {
    &value[0..5]
}
