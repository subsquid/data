use std::sync::Arc;

use arrow::array::{Array, StructArray};
use arrow_buffer::BooleanBufferBuilder;

use crate::any::{AnyBuilder, AnySlice};
use crate::bitmask::{BitSlice, push_null_mask, write_null_mask};
use crate::types::{Builder, Slice};
use crate::util::PageWriter;


#[derive(Clone)]
pub struct AnyStructSlice<'a> {
    columns: Arc<[AnySlice<'a>]>,
    nulls: Option<BitSlice<'a>>,
    offset: usize,
    len: usize
}


impl <'a> Slice<'a> for AnyStructSlice<'a> {
    fn read_page(_bytes: &'a [u8]) -> anyhow::Result<Self> {
        panic!("Struct slice cannot be read from a page")
    }

    fn write_page(&self, buf: &mut Vec<u8>) {
        let mut write = PageWriter::new(buf);
        
        write.append_index(self.columns.len());
        write.reserve_index_section(self.columns.len());
        
        write_null_mask(
            &self.nulls.as_ref().map(|nulls| nulls.slice(self.offset, self.len)),
            write.buf
        );
        
        for (i, col) in self.columns.iter().enumerate() {
            write.pad();
            col.slice(self.offset, self.len).write_page(write.buf);
            write.set_buffer_index(i)
        }
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
}


pub struct AnyStructBuilder {
    columns: Vec<AnyBuilder>,
    nulls: Option<BooleanBufferBuilder>,
    len: usize
}


impl Builder for AnyStructBuilder {
    type Slice<'a> = AnyStructSlice<'a>;

    fn push_slice(&mut self, slice: &Self::Slice<'_>) {
        for (i, col) in slice.columns.iter().enumerate() {
            self.columns[i].push_slice(&col.slice(slice.offset, slice.len))
        }
        push_null_mask(
            self.len,
            &slice.nulls,
            self.capacity(),
            &mut self.nulls
        )
    }

    fn as_slice(&self) -> Self::Slice<'_> {
        AnyStructSlice {
            columns: self.columns.iter().map(|c| c.as_slice()).collect(),
            nulls: self.nulls.as_ref().map(Builder::as_slice),
            offset: 0,
            len: self.len
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn capacity(&self) -> usize {
        self.columns.get(0).map(|b| b.capacity()).unwrap_or(0).max(self.len)
    }
}


impl <'a> From<&'a StructArray> for AnyStructSlice<'a> {
    fn from(value: &'a StructArray) -> Self {
        Self {
            columns: value.columns().iter().map(|arr| arr.as_ref().into()).collect(),
            nulls: value.nulls().map(|nulls| nulls.inner().into()),
            offset: 0,
            len: value.len()
        }
    }
}