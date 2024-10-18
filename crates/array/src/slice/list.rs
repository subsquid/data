use crate::access::Access;
use crate::index::{IndexList, RangeList, RangeListFromIterator};
use crate::offsets::Offsets;
use crate::slice::bitmask::BitmaskSlice;
use crate::slice::nullmask::NullmaskSlice;
use crate::slice::{AnyListItem, AnySlice, Slice};
use crate::writer::{ArrayWriter, OffsetsWriter};
use arrow::array::{GenericByteArray, ListArray};
use arrow::datatypes::ByteArrayType;
use arrow_buffer::ArrowNativeType;
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
    
    pub fn nulls(&self) -> &NullmaskSlice<'a> {
        &self.nulls
    }
    
    pub fn offsets(&self) -> Offsets<'a> {
        self.offsets
    }
    
    pub fn values(&self) -> &T {
        &self.values
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

        self.values.write_ranges(&mut dst.shift(2), &mut ranges.list_items(self.offsets))
    }

    fn write_indexes(
        &self,
        dst: &mut impl ArrayWriter,
        indexes: &(impl IndexList + ?Sized)
    ) -> anyhow::Result<()>
    {
        self.nulls.write_indexes(dst.nullmask(0), indexes)?;

        dst.offset(1).write_slice_indexes(self.offsets, indexes.index_iter())?;
        
        let item_ranges = indexes.index_iter().map(|i| {
            let beg = self.offsets.index(i);
            let end = self.offsets.index(i + 1);
            beg..end
        });
        
        self.values.write_ranges(&mut dst.shift(2), &mut RangeListFromIterator::new(item_ranges))
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


impl <'a, T: ArrowNativeType> Access for ListSlice<'a, &'a [T]>{
    type Value = &'a [T];

    #[inline]
    fn get(&self, i: usize) -> Self::Value {
        let beg = self.offsets.index(i);
        let end = self.offsets.index(i + 1);
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