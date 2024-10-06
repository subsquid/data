use crate::writer::{OffsetsWriter, RangeList};
use anyhow::ensure;
use arrow_buffer::ToByteSlice;
use std::io::Write;


pub struct OffsetsIOWriter<W> {
    write: W,
    last_offset: i32,
    first_offset: bool
}


impl <W: Write> OffsetsIOWriter<W> {
    pub fn new(write: W) -> Self {
        Self {
            write,
            last_offset: 0,
            first_offset: true
        }
    }
    
    pub fn finish(mut self) -> anyhow::Result<W> {
        self.write_first_offset()?;
        Ok(self.write)
    }

    #[inline]
    fn write_first_offset(&mut self) -> anyhow::Result<()> {
        if self.first_offset {
            self.first_offset = false;
            self.write.write_all(self.last_offset.to_byte_slice())?;
        }
        Ok(())
    }
    
    #[inline]
    fn write_slice(&mut self, offsets: &[i32]) -> anyhow::Result<()> {
        let beg = offsets[0];
        
        for offset in offsets[1..].iter().copied() {
            let val = offset - beg + self.last_offset;
            self.write.write_all(val.to_byte_slice())?;
            self.last_offset = val
        }
        
        Ok(())
    }
}


impl <W: Write> OffsetsWriter for OffsetsIOWriter<W> {
    fn write_slice(&mut self, offsets: &[i32]) -> anyhow::Result<()> {
        self.write_first_offset()?;
        self.write_slice(offsets)
    }

    fn write_slice_indexes(&mut self, offsets: &[i32], indexes: impl Iterator<Item=usize>) -> anyhow::Result<()> {
        self.write_first_offset()?;
        
        for i in indexes {
            let len = offsets[i + 1] - offsets[i];
            self.last_offset += len;
            self.write.write_all(self.last_offset.to_byte_slice())?;
        }
        
        Ok(())
    }

    fn write_slice_ranges(&mut self, offsets: &[i32], ranges: &mut impl RangeList) -> anyhow::Result<()> {
        self.write_first_offset()?;
        
        for r in ranges.iter() {
            self.write_slice(&offsets[r.start..r.end + 1])?
        }
        
        Ok(())
    }

    #[inline]
    fn write_len(&mut self, len: usize) -> anyhow::Result<()> {
        self.write_first_offset()?;
        self.last_offset += len as i32;
        self.write.write_all(self.last_offset.to_byte_slice())?;
        Ok(())
    }

    #[inline]
    fn write(&mut self, offset: i32) -> anyhow::Result<()> {
        ensure!(self.last_offset <= offset);
        self.write_first_offset()?;
        self.last_offset = offset;
        self.write.write_all(self.last_offset.to_byte_slice())?;
        Ok(())
    }
}