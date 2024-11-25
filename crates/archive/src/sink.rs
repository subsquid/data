use std::marker::PhantomData;

use crate::fs::Fs;
use crate::layout::ChunkWriter;

pub trait Writer<B> {
    fn buffered_bytes(&self) -> usize;

    fn push(&mut self, block: B);

    fn flush(&mut self, fs: Box<dyn Fs>) -> anyhow::Result<()>;

    fn get_block_height(&self, block: &B) -> u64;

    fn get_block_hash<'a>(&self, block: &'a B) -> &'a String;
}

pub struct Sink<'a, B, W: Writer<B>> {
    writer: W,
    chunk_writer: ChunkWriter<'a>,
    fs: &'a Box<dyn Fs>,
    chunk_size: usize,
    phantom: PhantomData<B>,
}

impl<'a, B, W: Writer<B>> Sink<'a, B, W> {
    pub fn new(writer: W, chunk_writer: ChunkWriter<'a>, fs: &'a Box<dyn Fs>, chunk_size: usize) -> Sink<'a, B, W> {
        Sink {
            writer,
            chunk_writer,
            fs,
            chunk_size,
            phantom: PhantomData::default(),
        }
    }

    pub fn write(&mut self, stream: impl Iterator<Item=anyhow::Result<B>>) -> anyhow::Result<()> {
        let mut first_block = None;
        let mut last_block = None;
        let mut last_hash = None;

        for result in stream {
            let block = result?;

            if first_block.is_none() {
                first_block = Some(self.writer.get_block_height(&block));
            }
            last_block = Some(self.writer.get_block_height(&block));
            last_hash = Some(self.get_hash(&block).to_string());

            self.writer.push(block);

            if self.writer.buffered_bytes() > self.chunk_size * 1024 * 1024 {
                let first_block = first_block.take().unwrap();
                let last_block = last_block.unwrap();
                let last_hash = last_hash.as_ref().unwrap();
                let chunk = self.chunk_writer.next_chunk(first_block, last_block, last_hash)?;
                self.writer.flush(self.fs.cd(&[&chunk.path()]))?;
            }
        }

        if self.writer.buffered_bytes() > 0 {
            let first_block = first_block.take().unwrap();
            let last_block = last_block.unwrap();
            let last_hash = last_hash.as_ref().unwrap();
            let chunk = self.chunk_writer.next_chunk(first_block, last_block, last_hash)?;
            self.writer.flush(self.fs.cd(&[&chunk.path()]))?;
        }

        Ok(())
    }

    fn get_hash(&self, block: &'a B) -> &'a str {
        let hash = self.writer.get_block_hash(block);
        short_hash(&hash)
    }
}

fn short_hash(value: &str) -> &str {
    if value.starts_with("0x") {
        &value[2..10]
    } else {
        &value[0..5]
    }
}
