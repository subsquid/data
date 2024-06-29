use std::io::Write;

use arrow::array::{Array, RecordBatch, StructArray};

use crate::json::encoder::Encoder;
use crate::json::encoder::factory::make_struct_encoder;
use crate::plan::BlockWriter;


pub struct JsonArrayWriter<W> {
    write: W,
    buf: Vec<u8>,
    flush_threshold: usize,
    rows_written: bool
}


impl <W> JsonArrayWriter<W> {
    pub fn new(write: W) -> Self {
        Self {
            write,
            buf: Vec::with_capacity(256 * 1024),
            flush_threshold: 16 * 1024,
            rows_written: false
        }
    }
}


impl <W: Write> JsonArrayWriter<W> {
    pub fn write_batch(&mut self, batch: RecordBatch) -> anyhow::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let struct_array = StructArray::from(batch);

        let mut encoder = make_struct_encoder(&struct_array)?;

        for i in 0..struct_array.len() {
            self.begin_new_item()?;
            encoder.encode(i, &mut self.buf);
        }

        Ok(())
    }

    pub fn write_blocks(&mut self, blocks: &mut BlockWriter) -> std::io::Result<()> {
        while blocks.has_next_block() {
            self.begin_new_item()?;
            blocks.write_next_block(&mut self.buf)
        }
        Ok(())
    }

    fn begin_new_item(&mut self) -> std::io::Result<()> {
        if self.buf.len() > self.flush_threshold {
            self.flush()?;
        }
        if self.rows_written {
            self.buf.push(b',')
        } else {
            self.rows_written = true;
            self.buf.push(b'[')
        }
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.write.write_all(&self.buf)?;
        self.buf.clear();
        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<W> {
        if self.rows_written {
            self.buf.push(b']')
        } else {
            self.buf.extend(b"[]")
        }
        self.flush()?;
        Ok(self.write)
    }
}