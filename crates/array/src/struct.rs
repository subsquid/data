use std::sync::Arc;

use anyhow::{Context, ensure};
use arrow::array::{Array, ArrayRef, StructArray};
use arrow::datatypes::{DataType, Field, Fields};
use arrow_buffer::BooleanBufferBuilder;

use crate::any::{AnyBuilder, AnySlice};
use crate::bitmask::{BitSlice, push_null_mask, write_null_mask};
use crate::types::{Builder, DefaultDataBuilder, Slice};
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


impl DefaultDataBuilder for AnyStructBuilder {}
impl Builder for AnyStructBuilder {
    type Slice<'a> = AnyStructSlice<'a>;

    fn read_page<'a>(&self, bytes: &'a [u8]) -> anyhow::Result<Self::Slice<'a>> {
        let mut page = PageReader::new(bytes, None)?;

        ensure!(
            page.buffers_left() == self.columns.len(),
            "page has {} columns, but this builder has {}",
            page.buffers_left(),
            self.columns.len()
        );

        let nulls = page.read_null_mask()?;

        let columns = self.columns.iter().enumerate().map(|(i, b)| {
            b.read_page(page.read_next_buffer()?).with_context(|| {
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

    fn push_slice(&mut self, slice: &Self::Slice<'_>) {
        for (i, col) in slice.columns.iter().enumerate() {
            self.columns[i].push_slice(&col.slice(slice.offset, slice.len))
        }
        push_null_mask(
            self.len,
            &slice.nulls,
            self.capacity(),
            &mut self.nulls
        );
        self.len += slice.len;
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

    fn into_arrow_array(self, data_type: Option<DataType>) -> ArrayRef {
        let (columns, fields) = if let Some(data_type) = data_type {
            let fields = if let DataType::Struct(fields) = data_type {
                fields
            } else {
                panic!("struct builder got unexpected data type - {}", data_type)
            };

            assert_eq!(fields.len(), self.columns.len());

            let columns = fields.iter().zip(self.columns.into_iter()).map(|(f, c)| {
               Builder::into_arrow_array(c, Some(f.data_type().clone()))
            }).collect::<Vec<_>>();

            (columns, fields)
        } else {
            let columns = self.columns.into_iter().map(|c| {
                Builder::into_arrow_array(c, None)
            }).collect::<Vec<_>>();

            let fields = columns.iter().enumerate().map(|(i, c)| {
                Arc::new(Field::new(
                    format!("f{}", i), 
                    c.data_type().clone(), 
                    true
                ))
            }).collect::<Fields>();

            (columns, fields)
        };
        
        let array = StructArray::new(
            fields, 
            columns, 
            self.nulls.map(|mut nulls| nulls.finish().into())
        );
        
        Arc::new(array)
    }
}


impl AnyStructBuilder {
    pub fn new(columns: Vec<AnyBuilder>) -> Self {
        assert!(columns.len() > 0);
        let len = columns[0].len();
        for i in 1..columns.len() {
            assert_eq!(columns[i].len(), len);
        }
        Self {
            columns,
            len,
            nulls: None
        }
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