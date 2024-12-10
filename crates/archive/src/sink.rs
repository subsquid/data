use crate::fs::Fs;
use crate::layout::ChunkWriter;
use crate::writer::ParquetWriter;


pub struct Sink<'a> {
    writer: ParquetWriter,
    chunk_writer: ChunkWriter<'a>,
    fs: &'a Box<dyn Fs>,
    chunk_size: usize,
}


impl<'a> Sink<'a> {
    pub fn new(
        writer: ParquetWriter,
        chunk_writer: ChunkWriter<'a>,
        fs: &'a Box<dyn Fs>,
        chunk_size: usize,
    ) -> Sink<'a> {
        Sink {
            writer,
            chunk_writer,
            fs,
            chunk_size,
        }
    }

    pub fn write(&mut self, stream: impl Iterator<Item = anyhow::Result<String>>) -> anyhow::Result<()> {
        let mut first_block = None;
        let mut last_block = None;
        let mut last_hash = None;

        for result in stream {
            let line = result?;
            let hash_and_height = self.writer.push(&line)?;

            if first_block.is_none() {
                first_block = Some(hash_and_height.height);
            }
            last_block = Some(hash_and_height.height);
            last_hash = Some(short_hash(&hash_and_height.hash).to_string());

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
}


fn short_hash(value: &str) -> &str {
    &value[0..5]
}
