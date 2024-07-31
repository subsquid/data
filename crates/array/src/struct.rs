use std::sync::Arc;

use anyhow::{Context, ensure};
use arrow::array::{Array, StructArray};
use arrow::datatypes::FieldRef;
use arrow_buffer::BooleanBufferBuilder;

use crate::any::{AnyBuilder, AnySlice};
use crate::bitmask::{BitSlice, push_null_mask, write_null_mask};
use crate::read_any_page;
use crate::types::{Builder, Slice};
use crate::util::{PageReader, PageWriter};


#[derive(Clone)]
pub struct AnyStructSlice<'a> {
    columns: Arc<[AnySlice<'a>]>,
    nulls: Option<BitSlice<'a>>,
    offset: usize,
    len: usize
}


impl <'a> Slice<'a> for AnyStructSlice<'a> {
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
            write.set_buffer_index(i + 1)
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
        // FIXME: validate
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


pub fn read_any_struct_page<'a>(
    bytes: &'a [u8],
    fields: &[FieldRef]
) -> anyhow::Result<AnyStructSlice<'a>>
{
    ensure!(fields.len() > 0, "structs with no columns are not readable");

    let mut page = PageReader::new(bytes, None)?;

    ensure!(
        page.buffers_left() == fields.len(),
        "page has {} columns, but field spec has {}",
        page.buffers_left(),
        fields.len()
    );

    let nulls = page.read_null_mask()?;

    let columns = fields.iter().enumerate().map(|(i, f)| {
        read_any_page(page.read_next_buffer()?, f.data_type()).with_context(|| {
            format!("failed to read column {}", i)
        })
    }).collect::<anyhow::Result<Arc<[AnySlice<'a>]>>>()?;

    let len = columns[0].len();

    for i in 1..columns.len() {
        ensure!(
            columns[i].len() == len,
            "columns {} and column 0 have different lengths",
            i
        );
    }

    let null_mask_length_ok = nulls.as_ref()
        .map(|nulls| nulls.len() + 1 == len)
        .unwrap_or(true);

    ensure!(
        null_mask_length_ok,
        "null mask length doesn't match the length of columns"
    );

    Ok(AnyStructSlice {
        columns,
        nulls,
        offset: 0,
        len
    })
}