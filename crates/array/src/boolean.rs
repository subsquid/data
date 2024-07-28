use arrow::array::{Array, BooleanArray};
use arrow_buffer::BooleanBufferBuilder;

use crate::bitmask::{BitSlice, push_null_mask, write_null_mask};
use crate::types::{Builder, Slice};
use crate::util::Padding;


#[derive(Clone)]
pub struct BooleanSlice<'a> {
    values: BitSlice<'a>,
    nulls: Option<BitSlice<'a>>
}


impl <'a> Slice<'a> for BooleanSlice<'a> {
    fn read_page(_bytes: &'a [u8]) -> anyhow::Result<Self> {
        todo!()
    }

    unsafe fn read_valid_page(_bytes: &'a [u8]) -> Self {
        todo!()
    }

    fn write_page(&self, buf: &mut Vec<u8>) {
        let mut padding = Padding::new(buf);
        write_null_mask(&self.nulls, buf);
        padding.pad(buf);
        self.values.write_page(buf)
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            values: self.values.slice(offset, len),
            nulls: self.nulls.as_ref().map(|nulls| nulls.slice(offset, len))
        }
    }
}


pub struct BooleanBuilder {
    values: BooleanBufferBuilder,
    nulls: Option<BooleanBufferBuilder>
}


impl Builder for BooleanBuilder {
    type Slice<'a> = BooleanSlice<'a>;

    fn push_slice(&mut self, slice: &Self::Slice<'_>) {
        self.values.push_slice(&slice.values);

        push_null_mask(
            self.values.len(),
            &slice.nulls,
            self.values.capacity(),
            &mut self.nulls
        )
    }

    fn as_slice(&self) -> Self::Slice<'_> {
        BooleanSlice {
            values: Builder::as_slice(&self.values),
            nulls: self.nulls.as_ref().map(Builder::as_slice)
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn capacity(&self) -> usize {
        self.values.capacity()
    }
}


impl <'a> From<&'a BooleanArray> for BooleanSlice<'a> {
    fn from(value: &'a BooleanArray) -> Self {
        Self {
            values: value.values().into(),
            nulls: value.nulls().map(|nulls| nulls.inner().into())
        }
    }
}