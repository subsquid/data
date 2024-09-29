use crate::slice::any::AnySlice;
use crate::slice::nullmask::NullmaskSlice;
use crate::slice::Slice;
use crate::writer::{ArrayWriter, RangeList};
use std::ops::Range;
use std::sync::Arc;


#[derive(Clone)]
pub struct AnyStructSlice<'a> {
    nulls: NullmaskSlice<'a>,
    columns: Arc<[AnySlice<'a>]>,
    offset: usize,
    len: usize
}


impl<'a> AnyStructSlice<'a> {
    fn write_src_range(&self, dst: &mut impl ArrayWriter, range: Range<usize>) -> anyhow::Result<()> {
        self.nulls.write_range(dst.nullmask(0), range.clone())?;

        let mut shift = 1;
        for c in self.columns.iter() {
            c.write_range(&mut dst.shift(shift), range.clone())?;
            shift += c.num_buffers();
        }
        Ok(())
    }
}


impl<'a> Slice for AnyStructSlice<'a> {
    fn num_buffers(&self) -> usize {
        1 + self.columns.iter().map(|c| c.num_buffers()).sum::<usize>()
    }

    fn len(&self) -> usize {
        self.len
    }

    fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(offset + len <= self.len);
        Self {
            columns: self.columns.clone(),
            nulls: self.nulls.clone(),
            offset: self.offset + offset,
            len
        }
    }

    fn write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        self.write_src_range(dst, self.offset..self.offset + self.len)
    }

    fn write_range(&self, dst: &mut impl ArrayWriter, range: Range<usize>) -> anyhow::Result<()> {
        assert!(range.len() <= self.len);
        let range = range.start + self.offset..range.end + self.offset;
        self.write_src_range(dst, range)
    }

    fn write_ranges(&self, dst: &mut impl ArrayWriter, ranges: &mut impl RangeList) -> anyhow::Result<()> {
        let mut ranges = ranges.shift(self.offset, self.len);
        
        self.nulls.write_ranges(dst.nullmask(0), &mut ranges)?;

        let mut shift = 1;
        for c in self.columns.iter() {
            c.write_ranges(&mut dst.shift(shift), &mut ranges)?;
            shift += c.num_buffers();
        }
        Ok(())
    }

    fn write_indexes(&self, dst: &mut impl ArrayWriter, indexes: impl IntoIterator<Item=usize, IntoIter: Clone>) -> anyhow::Result<()> {
        let indexes = indexes.into_iter().map(|i| {
            assert!(i < self.len);
            i + self.offset
        });
        
        self.nulls.write_indexes(dst.nullmask(0), indexes.clone())?;

        let mut shift = 1;
        for c in self.columns.iter() {
            c.write_indexes(&mut dst.shift(shift), indexes.clone())?;
            shift += c.num_buffers();
        }
        Ok(())
    }
}