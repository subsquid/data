use crate::access::Access;
use crate::index::{MaterializedRangeList, RangeList, RangeListFromIterator};
use crate::slice::bitmask::BitmaskSlice;
use crate::slice::nullmask::NullmaskSlice;
use crate::slice::{AnyListItem, AnySlice, Slice};
use crate::writer::ArrayWriter;
use arrow::array::{FixedSizeListArray, FixedSizeBinaryArray};
use std::ops::Range;

#[derive(Clone)]
pub struct FixedSizeListSlice<'a, T: Clone> {
    size: usize,
    nulls: NullmaskSlice<'a>,
    values: T,
}

impl<'a, T: Slice> FixedSizeListSlice<'a, T> {
    pub fn new(size: usize, values: T, nulls: Option<BitmaskSlice<'a>>) -> Self {
        assert!(values.len() % size == 0);
        let len = values.len() / size;
        Self {
            size,
            nulls: NullmaskSlice::new(len, nulls),
            values,
        }
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn nulls(&self) -> NullmaskSlice<'a> {
        self.nulls.clone()
    }

    #[inline]
    pub fn values(&self) -> T {
        self.values.clone()
    }
}

impl<'a, T: Slice> Slice for FixedSizeListSlice<'a, T> {
    #[inline]
    fn num_buffers(&self) -> usize {
        1 + self.values.num_buffers()
    }

    #[inline]
    fn byte_size(&self) -> usize {
        self.nulls.byte_size() + self.values.byte_size()
    }

    #[inline]
    fn len(&self) -> usize {
        self.nulls.len()
    }

    #[inline]
    fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            size: self.size,
            nulls: self.nulls.slice(offset, len),
            values: self.values.slice(offset * self.size, len * self.size),
        }
    }

    fn write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        self.nulls.write(dst.nullmask(0))?;

        self.values.write(&mut dst.shift(1))
    }

    fn write_range(&self, dst: &mut impl ArrayWriter, range: Range<usize>) -> anyhow::Result<()> {
        if range.is_empty() {
            return Ok(());
        }

        self.nulls.write_range(dst.nullmask(0), range.clone())?;

        let value_range = (range.start * self.size)..(range.end * self.size);

        self.values.write_range(&mut dst.shift(1), value_range)
    }

    fn write_ranges(
        &self,
        dst: &mut impl ArrayWriter,
        ranges: &mut impl RangeList,
    ) -> anyhow::Result<()> {
        self.nulls.write_ranges(dst.nullmask(0), ranges)?;

        let mut value_ranges = MaterializedRangeList::from_iter(ranges.iter().map(|r| {
            let beg = r.start * self.size;
            let end = r.end * self.size;
            beg..end
        }));

        self.values
            .write_ranges(&mut dst.shift(1), &mut value_ranges)
    }

    fn write_indexes(
        &self,
        dst: &mut impl ArrayWriter,
        indexes: impl Iterator<Item = usize> + Clone,
    ) -> anyhow::Result<()> {
        self.nulls.write_indexes(dst.nullmask(0), indexes.clone())?;

        let item_ranges = indexes.map(|i| {
            let beg = i * self.size;
            let end = beg + self.size;
            beg..end
        });

        self.values.write_ranges(
            &mut dst.shift(1),
            &mut RangeListFromIterator::new(item_ranges),
        )
    }
}

impl<'a> From<&'a FixedSizeBinaryArray> for FixedSizeListSlice<'a, &'a[u8]> {
    fn from(value: &'a FixedSizeBinaryArray) -> Self {
        Self {
            size: value.value_length() as usize,
            nulls: NullmaskSlice::from_array(value),
            values: value.values().as_ref()
        }
    }
}

impl<'a> From<&'a FixedSizeListArray> for FixedSizeListSlice<'a, AnySlice<'a>> {
    fn from(value: &'a FixedSizeListArray) -> Self {
        Self {
            size: value.value_length() as usize,
            nulls: NullmaskSlice::from_array(value),
            values: value.values().as_ref().into()
        }
    }
}

impl<'a> From<&'a FixedSizeListArray> for FixedSizeListSlice<'a, AnyListItem<'a>> {
    fn from(value: &'a FixedSizeListArray) -> Self {
        Self {
            size: value.value_length() as usize,
            nulls: NullmaskSlice::from_array(value),
            values: AnyListItem::new(value.values().as_ref().into())
        }
    }
}

impl<'a, T> Access for FixedSizeListSlice<'a, &'a [T]> {
    type Value = &'a [T];

    #[inline]
    fn get(&self, i: usize) -> Self::Value {
        let beg = i * self.size;
        let end = beg + self.size;
        &self.values[beg..end]
    }

    #[inline]
    fn is_valid(&self, i: usize) -> bool {
        self.nulls.is_valid(i)
    }

    #[inline]
    fn has_nulls(&self) -> bool {
        self.nulls.has_nulls()
    }
}

impl<'a, T: Ord> FixedSizeListSlice<'a, &'a [T]> {
    pub fn max(&self) -> Option<&'a [T]> {
        if self.has_nulls() {
            (0..self.nulls.len())
                .flat_map(|i| self.is_valid(i).then(|| self.get(i)))
                .max()
        } else {
            (0..self.nulls.len()).map(|i| self.get(i)).max()
        }
    }

    pub fn min(&self) -> Option<&'a [T]> {
        if self.has_nulls() {
            (0..self.nulls.len())
                .flat_map(|i| self.is_valid(i).then(|| self.get(i)))
                .min()
        } else {
            (0..self.nulls.len()).map(|i| self.get(i)).min()
        }
    }
}
