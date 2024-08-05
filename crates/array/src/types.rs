use std::ops::Range;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;


pub trait StaticSlice<'a>: Slice<'a> {
    fn read_page(bytes: &'a [u8]) -> anyhow::Result<Self>;
}


pub trait Slice<'a>: Sized + Clone {
    fn write_page(&self, buf: &mut Vec<u8>);

    fn len(&self) -> usize;

    fn slice(&self, offset: usize, len: usize) -> Self {
        self.slice_range(offset..offset + len)
    }

    fn slice_range(&self, range: Range<usize>) -> Self {
        self.slice(range.start, range.len())
    }
}


pub trait Builder {
    type Slice<'a>: Slice<'a>;

    fn read_page<'a>(&self, page: &'a [u8]) -> anyhow::Result<Self::Slice<'a>>;

    fn push_slice(&mut self, slice: &Self::Slice<'_>);

    fn push_slice_ranges(
        &mut self,
        slice: &Self::Slice<'_>,
        ranges: impl Iterator<Item = Range<usize>> + Clone
    ) {
        for r in ranges {
            self.push_slice(&slice.slice_range(r))
        }
    }

    fn as_slice(&self) -> Self::Slice<'_>;

    fn len(&self) -> usize;

    fn capacity(&self) -> usize;

    fn into_arrow_array(self, data_type: Option<DataType>) -> ArrayRef;
}


impl <T: Builder> Builder for Box<T> {
    type Slice<'a> = T::Slice<'a>;

    fn read_page<'a>(&self, page: &'a [u8]) -> anyhow::Result<Self::Slice<'a>> {
        self.as_ref().read_page(page)
    }

    fn push_slice(&mut self, slice: &Self::Slice<'_>) {
        self.as_mut().push_slice(slice)
    }

    fn push_slice_ranges(
        &mut self,
        slice: &Self::Slice<'_>,
        ranges: impl Iterator<Item = Range<usize>> + Clone
    ) {
        self.as_mut().push_slice_ranges(slice, ranges)
    }

    fn as_slice(&self) -> Self::Slice<'_> {
        self.as_ref().as_slice()
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn capacity(&self) -> usize {
        self.as_ref().capacity()
    }

    fn into_arrow_array(self, data_type: Option<DataType>) -> ArrayRef {
        (*self).into_arrow_array(data_type)
    }
}


pub trait DataBuilder {
    fn push_page(&mut self, page: &[u8]) -> anyhow::Result<()>;

    fn push_page_ranges(
        &mut self,
        page: &[u8],
        ranges: &[Range<u32>]
    ) -> anyhow::Result<()>;

    fn into_arrow_array(self: Box<Self>, data_type: Option<DataType>) -> ArrayRef;
}


pub trait DefaultDataBuilder {}


impl <T: Builder + DefaultDataBuilder> DataBuilder for T {
    fn push_page(&mut self, page: &[u8]) -> anyhow::Result<()> {
        let slice = self.read_page(page)?;
        self.push_slice(&slice);
        Ok(())
    }

    fn push_page_ranges(
        &mut self, 
        page: &[u8],
        ranges: &[Range<u32>]
    ) -> anyhow::Result<()>
    {
        let slice = self.read_page(page)?;

        self.push_slice_ranges(&slice, ranges.iter().map(|r| {
            r.start as usize..r.end as usize
        }));

        Ok(())
    }

    fn into_arrow_array(self: Box<Self>, data_type: Option<DataType>) -> ArrayRef {
        Builder::into_arrow_array(self, data_type)
    }
}