use crate::index::RangeList;
use crate::slice::any::AnySlice;
use crate::slice::nullmask::NullmaskSlice;
use crate::slice::{AsSlice, Slice};
use crate::writer::ArrayWriter;
use arrow::array::{Array, RecordBatch, StructArray};
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
    pub fn new(nulls: NullmaskSlice<'a>, columns: Arc<[AnySlice<'a>]>) -> Self {
        let len = nulls.len();
        for c in columns.iter() {
            assert_eq!(c.len(), len);
        }
        Self {
            nulls,
            columns,
            offset: 0,
            len
        }
    }
    
    pub fn column(&self, i: usize) -> AnySlice<'a> {
        self.columns[i].slice(self.offset, self.len)
    }
    
    pub fn has_nulls(&self) -> bool {
        self.nulls.has_nulls()
    }
    
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
            nulls: self.nulls.clone(),
            columns: self.columns.clone(),
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
        self.nulls.slice(self.offset, self.len).write_ranges(dst.nullmask(0), ranges)?;
        
        let mut shift = 1;
        for c in self.columns.iter() {
            c.slice(self.offset, self.len).write_ranges(&mut dst.shift(shift), ranges)?;
            shift += c.num_buffers();
        }
        Ok(())
    }

    fn write_indexes(
        &self, 
        dst: &mut impl ArrayWriter, 
        indexes: impl Iterator<Item=usize> + Clone
    ) -> anyhow::Result<()> 
    {
        self.nulls.slice(self.offset, self.len).write_indexes(dst.nullmask(0), indexes.clone())?;

        let mut shift = 1;
        for c in self.columns.iter() {
            c.slice(self.offset, self.len).write_indexes(&mut dst.shift(shift), indexes.clone())?;
            shift += c.num_buffers();
        }
        Ok(())
    }
}


impl<'a> From<&'a StructArray> for AnyStructSlice<'a> {
    fn from(value: &'a StructArray) -> Self {
        Self {
            nulls: NullmaskSlice::from_array(value),
            columns: value.columns().iter().map(|c| c.as_ref().into()).collect(),
            offset: 0,
            len: value.len()
        }
    }
}


impl AsSlice for StructArray {
    type Slice<'a> = AnyStructSlice<'a>;

    fn as_slice(&self) -> Self::Slice<'_> {
        self.into()
    }
}


impl<'a> From<&'a RecordBatch> for AnyStructSlice<'a>  {
    fn from(value: &'a RecordBatch) -> Self {
        Self {
            nulls: NullmaskSlice::new(value.num_rows(), None),
            columns: value.columns().iter().map(|c| c.as_ref().into()).collect(),
            offset: 0,
            len: value.num_rows()
        }
    }
}


impl AsSlice for RecordBatch {
    type Slice<'a> = AnyStructSlice<'a>;

    fn as_slice(&self) -> Self::Slice<'_> {
        self.into()
    }
}