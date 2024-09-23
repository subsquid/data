use crate::util::validate_offsets;
use crate::writer::OffsetsWriter;
use anyhow::{anyhow, ensure};
use arrow_buffer::ToByteSlice;
use std::io::Write;


pub struct OffsetsIOWriter<W> {
    writer: W,
    last_offset: i32,
    first_offset: bool
}


impl <W: Write> OffsetsIOWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            last_offset: 0,
            first_offset: true
        }
    }

    #[inline]
    fn write_first_offset(&mut self) -> anyhow::Result<()> {
        if self.first_offset {
            self.first_offset = false;
            self.writer.write_all(self.last_offset.to_byte_slice())?;
        }
        Ok(())
    }
}


impl <W: Write> OffsetsWriter for OffsetsIOWriter<W> {
    fn write_slice(&mut self, offsets: &[i32]) -> anyhow::Result<()> {
        validate_offsets(&offsets).map_err(|msg| anyhow!(msg))?;
        
        self.write_first_offset()?;

        let beg = offsets[0];

        for offset in offsets[1..].iter().copied() {
            let val = offset - beg + self.last_offset;
            self.writer.write_all(val.to_byte_slice())?;
            self.last_offset = val
        }

        Ok(())
    }

    #[inline]
    fn write_len(&mut self, len: usize) -> anyhow::Result<()> {
        self.write_first_offset()?;
        self.last_offset += len as i32;
        self.writer.write_all(self.last_offset.to_byte_slice())?;
        Ok(())
    }

    #[inline]
    fn write(&mut self, offset: i32) -> anyhow::Result<()> {
        ensure!(self.last_offset <= offset);
        self.write_first_offset()?;
        self.last_offset = offset;
        self.writer.write_all(self.last_offset.to_byte_slice())?;
        Ok(())
    }
}