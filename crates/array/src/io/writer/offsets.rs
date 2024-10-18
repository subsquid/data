use crate::offsets::Offsets;
use crate::writer::OffsetsWriter;
use arrow_buffer::ToByteSlice;
use std::io::Write;
use crate::index::RangeList;


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
    fn write_slice(&mut self, offsets: Offsets<'_>) -> anyhow::Result<()> {
        let beg = offsets.first_offset();
        let last_offset = self.last_offset;
        
        for offset in offsets.values()[1..].iter().copied() {
            let val = offset - beg + last_offset;
            self.write.write_all(val.to_byte_slice())?;
            self.last_offset = val
        }
        
        Ok(())
    }
}


impl <W: Write> OffsetsWriter for OffsetsIOWriter<W> {
    fn write_slice(&mut self, offsets: Offsets<'_>) -> anyhow::Result<()> {
        self.write_first_offset()?;
        self.write_slice(offsets)
    }

    fn write_slice_indexes(&mut self, offsets: Offsets<'_>, indexes: impl Iterator<Item=usize>) -> anyhow::Result<()> {
        self.write_first_offset()?;
        
        for i in indexes {
            let len = offsets.values()[i + 1] - offsets.values()[i];
            self.last_offset += len;
            self.write.write_all(self.last_offset.to_byte_slice())?;
        }
        
        Ok(())
    }

    fn write_slice_ranges(&mut self, offsets: Offsets<'_>, ranges: &mut impl RangeList) -> anyhow::Result<()> {
        self.write_first_offset()?;
        
        for r in ranges.iter() {
            self.write_slice(offsets.slice_by_range(r))?
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
}