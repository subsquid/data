use std::io::Write;

use arrow_buffer::ToByteSlice;

use crate::{index::RangeList, io::writer::bitmask::BitmaskIOWriter, util::bit_tools, writer::BitmaskWriter};

pub struct NullmaskIOWriter<W> {
    nulls: BitmaskIOWriter<W>,
    has_nulls: bool,
    len: usize
}

impl<W: Write> NullmaskIOWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            nulls: BitmaskIOWriter::new(writer),
            has_nulls: false,
            len: 0
        }
    }

    pub fn finish(self) -> anyhow::Result<W> {
        if self.has_nulls {
            self.nulls.finish()
        } else {
            let mut write = self.nulls.into_write();
            write.write_all((self.len as u32).to_byte_slice())?;
            Ok(write)
        }
    }

    #[inline]
    fn check_bitmask_presence(&mut self, all_valid: impl FnOnce() -> Option<usize>) -> anyhow::Result<bool> {
        if self.has_nulls {
            return Ok(true);
        }
        if let Some(len) = all_valid() {
            self.len += len;
            return Ok(false);
        }
        self.has_nulls = true;
        self.nulls.write_many(true, self.len)?;
        Ok(true)
    }
}

impl<W: Write> BitmaskWriter for NullmaskIOWriter<W> {
    fn write_slice(&mut self, data: &[u8], offset: usize, len: usize) -> anyhow::Result<()> {
        if self.check_bitmask_presence(|| bit_tools::all_valid(data, offset, len).then_some(len))? {
            self.nulls.write_slice(data, offset, len)?;
        }
        Ok(())
    }

    fn write_slice_indexes(&mut self, data: &[u8], indexes: impl Iterator<Item = usize> + Clone) -> anyhow::Result<()> {
        if self.check_bitmask_presence(|| bit_tools::all_indexes_valid(data, indexes.clone()))? {
            self.nulls.write_slice_indexes(data, indexes)?;
        }
        Ok(())
    }

    fn write_slice_ranges(&mut self, data: &[u8], ranges: &mut impl RangeList) -> anyhow::Result<()> {
        if self.check_bitmask_presence(|| bit_tools::all_ranges_valid(data, ranges.iter()))? {
            self.nulls.write_slice_ranges(data, ranges)?;
        }
        Ok(())
    }

    fn write_many(&mut self, val: bool, count: usize) -> anyhow::Result<()> {
        if count == 0 {
            return Ok(());
        }
        match (self.has_nulls, val) {
            (true, val) => self.nulls.write_many(val, count),
            (false, true) => {
                self.len += count;
                Ok(())
            }
            (false, false) => {
                self.has_nulls = true;
                self.nulls.write_many(true, self.len)?;
                self.nulls.write_many(false, count)
            }
        }
    }
}
