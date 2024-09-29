use crate::slice::nullmask::NullmaskSlice;
use crate::slice::Slice;
use crate::writer::{ArrayWriter, OffsetsWriter, RangeList, RangesIterable};
use std::ops::Range;


#[derive(Clone)]
pub struct ListSlice<'a, T: Clone> {
    nulls: NullmaskSlice<'a>,
    offsets: &'a [i32],
    values: T
}


impl <'a, T: Slice> Slice for ListSlice<'a, T> {
    #[inline]
    fn num_buffers(&self) -> usize {
        2 + self.values.num_buffers()
    }

    #[inline]
    fn len(&self) -> usize {
        self.nulls.len()
    }

    #[inline]
    fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            nulls: self.nulls.slice(offset, len),
            offsets: &self.offsets[offset..offset + len + 1],
            values: self.values.clone()
        }
    }

    fn write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        self.nulls.write(dst.nullmask(0))?;

        dst.offset(1).write_slice(self.offsets)?;

        let value_range = self.offsets[0] as usize..self.offsets.last().copied().unwrap() as usize;

        self.values.write_range(&mut dst.shift(2), value_range)
    }

    fn write_range(&self, dst: &mut impl ArrayWriter, range: Range<usize>) -> anyhow::Result<()> {
        if range.is_empty() {
            return Ok(())
        }

        self.nulls.write_range(dst.nullmask(0), range.clone())?;

        dst.offset(1).write_slice(&self.offsets[range.start..range.end + 1])?;

        let value_range = self.offsets[range.start] as usize..self.offsets[range.end] as usize;

        self.values.write_range(&mut dst.shift(2), value_range)
    }

    fn write_ranges(&self, dst: &mut impl ArrayWriter, ranges: &mut impl RangeList) -> anyhow::Result<()> {
        self.nulls.write_ranges(dst.nullmask(0), ranges)?;

        dst.offset(1).write_slice_ranges(self.offsets, ranges)?;

        let value_ranges = ranges.iter().map(|r| {
            let beg = self.offsets[r.start] as usize;
            let end = self.offsets[r.end] as usize;
            beg..end
        });

        self.values.write_ranges(&mut dst.shift(2), &mut RangesIterable::new(value_ranges))
    }

    fn write_indexes(
        &self,
        dst: &mut impl ArrayWriter,
        indexes: impl IntoIterator<Item=usize, IntoIter: Clone>
    ) -> anyhow::Result<()>
    {
        let indexes = indexes.into_iter();

        self.nulls.write_indexes(dst.nullmask(0), indexes.clone())?;

        dst.offset(1).write_slice_indexes(self.offsets, indexes.clone())?;

        let value_ranges = indexes.map(|i| {
            let beg = self.offsets[i] as usize;
            let end = self.offsets[i + 1] as usize;
            beg..end
        });

        self.values.write_ranges(&mut dst.shift(2), &mut RangesIterable::new(value_ranges))
    }
}