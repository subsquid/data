use std::sync::Arc;
use anyhow::{Context, ensure};
use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::datatypes::DataType;
use arrow_buffer::BooleanBufferBuilder;

use crate::bitmask::{BitSlice, build_null_buffer, push_null_mask, write_null_mask};
use crate::{DefaultDataBuilder, StaticSlice};
use crate::types::{Builder, Slice};
use crate::util::{assert_data_type, PageReader, PageWriter};


#[derive(Clone)]
pub struct BooleanSlice<'a> {
    values: BitSlice<'a>,
    nulls: Option<BitSlice<'a>>
}


impl <'a> Slice<'a> for BooleanSlice<'a> {
    fn write_page(&self, buf: &mut Vec<u8>) {
        let mut write = PageWriter::new(buf);
        write_null_mask(&self.nulls, write.buf);
        write.pad();
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


impl <'a> StaticSlice<'a> for BooleanSlice<'a> {
    fn read_page(bytes: &'a [u8]) -> anyhow::Result<Self> {
        let mut page = PageReader::new(bytes, Some(1))?;
        let nulls = page.read_null_mask()?;

        let values_buf = page.read_next_buffer()?;

        let values = BitSlice::read_page(values_buf).context(
            "failed to read value buffer"
        )?;

        let null_mask_length_is_ok = nulls.as_ref()
            .map(|mask| mask.len() == values.len())
            .unwrap_or(true);

        ensure!(null_mask_length_is_ok, "null mask length doesn't match the value array length");

        Ok(Self {
            values,
            nulls
        })
    }
}


pub struct BooleanBuilder {
    values: BooleanBufferBuilder,
    nulls: Option<BooleanBufferBuilder>
}


impl DefaultDataBuilder for BooleanBuilder {}
impl Builder for BooleanBuilder {
    type Slice<'a> = BooleanSlice<'a>;

    fn read_page<'a>(&self, page: &'a [u8]) -> anyhow::Result<Self::Slice<'a>> {
        Self::Slice::read_page(page)
    }

    fn push_slice(&mut self, slice: &Self::Slice<'_>) {
        self.values.push_slice(&slice.values);

        push_null_mask(
            self.values.len(),
            slice.len(),
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

    fn into_arrow_array(mut self, data_type: Option<DataType>) -> ArrayRef {
        assert_data_type!(data_type, DataType::Boolean);
        Arc::new(
            BooleanArray::new(
                self.values.finish(), 
                self.nulls.and_then(build_null_buffer)
            )
        )
    }
}


impl BooleanBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: BooleanBufferBuilder::new(capacity),
            nulls: None
        }
    }
}


impl Default for BooleanBuilder {
    fn default() -> Self {
        Self::with_capacity(0)
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