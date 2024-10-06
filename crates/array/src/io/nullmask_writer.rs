use std::io::Write;
use crate::io::bitmask_writer::BitmaskIOWriter;
use crate::util::bit_tools;
use crate::writer::{BitmaskWriter, RangeList};


pub struct NullmaskIOWriter<W> {
    bitmask: BitmaskIOWriter<W>,
    len: usize,
    has_bitmask: bool
}


impl <W: Write> NullmaskIOWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            bitmask: BitmaskIOWriter::new(writer),
            len: 0,
            has_bitmask: false
        }
    }

    pub fn finish(self) -> anyhow::Result<W> {
        self.bitmask.finish()
    }
    
    #[inline]
    fn check_bitmask_presence(&mut self, all_valid: impl FnOnce() -> Option<usize>) -> anyhow::Result<bool> {
        if self.has_bitmask {
            return Ok(true)
        }
        if let Some(len) = all_valid() {
            self.len += len;
            return Ok(false)
        }
        self.has_bitmask = true;
        self.bitmask.write_many(true, self.len)?;
        Ok(true)
    }
}


impl<W: Write> BitmaskWriter for NullmaskIOWriter<W> {
    fn write_slice(&mut self, data: &[u8], offset: usize, len: usize) -> anyhow::Result<()> {
        if self.check_bitmask_presence(|| bit_tools::all_valid(data, offset, len).then_some(len))? {
            self.bitmask.write_slice(data, offset, len)?;
        }
        Ok(())
    }

    fn write_slice_indexes(
        &mut self, 
        data: &[u8], 
        indexes: impl Iterator<Item = usize> + Clone
    ) -> anyhow::Result<()> 
    {
        if self.check_bitmask_presence(|| bit_tools::all_indexes_valid(data, indexes.clone()))? {
            self.bitmask.write_slice_indexes(data, indexes)?;
        }
        Ok(())
    }

    fn write_slice_ranges(&mut self, data: &[u8], ranges: &mut impl RangeList) -> anyhow::Result<()> {
        if self.check_bitmask_presence(|| bit_tools::all_ranges_valid(data, ranges.iter()))? {
            self.bitmask.write_slice_ranges(data, ranges)?;
        }
        Ok(())
    }

    fn write_many(&mut self, val: bool, count: usize) -> anyhow::Result<()> {
        if count == 0 {
            return Ok(())
        }
        match (self.has_bitmask, val) {
            (true, val) => self.bitmask.write_many(val, count),
            (false, true) => {
                self.len += count;
                Ok(())
            },
            (false, false) => {
                self.has_bitmask = true;
                self.bitmask.write_many(true, self.len)?;
                self.bitmask.write_many(false, count)
            }
        }
    }
}