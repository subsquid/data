use crate::offsets::Offsets;
use crate::slice::bitmask::BitmaskSlice;
use crate::slice::nullmask::NullmaskSlice;
use crate::slice::{AnyListItem, AnySlice, Slice};
use crate::writer::{ArrayWriter, OffsetsWriter, RangeList, RangesIterable};
use arrow::array::{GenericByteArray, ListArray};
use arrow::datatypes::ByteArrayType;
use std::ops::Range;


#[derive(Clone)]
pub struct ListSlice<'a, T: Clone> {
    nulls: NullmaskSlice<'a>,
    offsets: Offsets<'a>,
    values: T
}


impl<'a, T: Slice> ListSlice<'a, T> {
    pub fn new(offsets: Offsets<'a>, values: T, nulls: Option<BitmaskSlice<'a>>) -> Self {
        assert!(offsets.last_index() <= values.len());
        Self {
            nulls: NullmaskSlice::new(offsets.len(), nulls),
            offsets,
            values
        }
    }
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
            offsets: self.offsets.slice(offset, len),
            values: self.values.clone()
        }
    }

    fn write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        self.nulls.write(dst.nullmask(0))?;

        dst.offset(1).write_slice(self.offsets)?;

        self.values.write_range(&mut dst.shift(2), self.offsets.range())
    }

    fn write_range(&self, dst: &mut impl ArrayWriter, range: Range<usize>) -> anyhow::Result<()> {
        if range.is_empty() {
            return Ok(())
        }

        self.nulls.write_range(dst.nullmask(0), range.clone())?;

        dst.offset(1).write_slice(self.offsets.slice_by_range(range.clone()))?;

        let value_range = self.offsets.index(range.start)..self.offsets.index(range.end);

        self.values.write_range(&mut dst.shift(2), value_range)
    }

    fn write_ranges(&self, dst: &mut impl ArrayWriter, ranges: &mut impl RangeList) -> anyhow::Result<()> {
        self.nulls.write_ranges(dst.nullmask(0), ranges)?;

        dst.offset(1).write_slice_ranges(self.offsets, ranges)?;

        let value_ranges = ranges.iter().map(|r| {
            let beg = self.offsets.index(r.start);
            let end = self.offsets.index(r.end);
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
            let beg = self.offsets.index(i);
            let end = self.offsets.index(i + 1);
            beg..end
        });

        self.values.write_ranges(&mut dst.shift(2), &mut RangesIterable::new(value_ranges))
    }
}


impl <'a, T: ByteArrayType<Offset=i32>> From<&'a GenericByteArray<T>> for ListSlice<'a, &'a [u8]> {
    fn from(value: &'a GenericByteArray<T>) -> Self {
        Self {
            nulls: NullmaskSlice::from_array(value),
            offsets: value.offsets().into(),
            values: value.values()
        }
    }
}


impl<'a> From<&'a ListArray> for ListSlice<'a, AnySlice<'a>> {
    fn from(value: &'a ListArray) -> Self {
        Self {
            nulls: NullmaskSlice::from_array(value),
            offsets: value.offsets().into(),
            values: value.values().as_ref().into()
        }
    }
}


impl<'a> From<&'a ListArray> for ListSlice<'a, AnyListItem<'a>> {
    fn from(value: &'a ListArray) -> Self {
        Self {
            nulls: NullmaskSlice::from_array(value),
            offsets: value.offsets().into(),
            values: AnyListItem::new(value.values().as_ref().into())
        }
    }
}