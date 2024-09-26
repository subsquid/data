use crate::writer::BitmaskWriter;
use arrow_buffer::bit_util;
use std::ops::Range;


pub struct BitmaskSlice<'a> {
    data: &'a [u8],
    offset: usize,
    len: usize
}


impl<'a> BitmaskSlice<'a> {
    #[inline]
    pub fn value(&self, i: usize) -> bool {
        assert!(self.offset + i < self.len);
        // Safety: bounds should be guaranteed by construction and the above assertion
        unsafe {
            bit_util::get_bit_raw(self.data.as_ptr(), self.offset + i)
        }
    }

    pub fn write(&self, dst: &mut impl BitmaskWriter) -> anyhow::Result<()> {
        dst.write_packed_bits(self.data, self.offset, self.len)
    }

    pub fn write_range(&self, dst: &mut impl BitmaskWriter, range: Range<usize>) -> anyhow::Result<()> {
        if range.is_empty() {
            return Ok(())
        }
        dst.write_packed_bits(self.data, self.offset + range.start, range.len())
    }

    pub fn write_indexes(
        &self,
        dst: &mut impl BitmaskWriter,
        indexes: impl IntoIterator<Item = usize>
    ) -> anyhow::Result<()>
    {
        // TODO: is it any good?
        let mut buf: [u8; 32] = [0; 32];
        let mut it = indexes.into_iter();
        let mut pos = 0;

        'L: loop {
            while pos < buf.len() * 8 {
                if let Some(i) = it.next() {
                    if self.value(i) {
                        unsafe {
                            bit_util::set_bit_raw(buf.as_mut_ptr(), pos);
                        }
                    }
                    pos += 1;
                } else {
                    break 'L;
                }
            }
            dst.write_packed_bits(&buf, 0, pos)?;
            pos = 0;
            buf.fill(0);
        }

        if pos > 0 {
            dst.write_packed_bits(&buf, 0, pos)?;
        }

        Ok(())
    }
}