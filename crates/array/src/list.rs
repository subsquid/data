use std::sync::Arc;

use anyhow::ensure;
use arrow::array::{Array, ArrayRef, GenericByteArray, ListArray};
use arrow::datatypes::{ByteArrayType, DataType, Field};
use arrow_buffer::{BooleanBufferBuilder, OffsetBuffer};

use crate::{AnySlice, DefaultDataBuilder, StaticSlice};
use crate::bitmask::{BitSlice, push_null_mask, write_null_mask};
use crate::primitive::{NativeBuilder, NativeSlice};
use crate::types::{Builder, Slice};
use crate::util::{PageReader, PageWriter};


#[derive(Clone)]
pub struct ListSlice<'a, T> {
    offsets: NativeSlice<'a, i32>,
    values: T,
    nulls: Option<BitSlice<'a>>
}


impl <'a, T> ListSlice<'a, T> {
    fn zeroed_offsets(&self) -> impl Iterator<Item = i32> + '_ {
        let offset = self.offsets.value(0);
        (0..self.offsets.len()).map(move |i| {
            self.offsets.value(i) - offset
        })
    }
}


impl <'a, T: Slice<'a>> Slice<'a> for ListSlice<'a, T> {
    fn write_page(&self, buf: &mut Vec<u8>) {
        let mut write = PageWriter::new(buf);

        write.append_index(self.offsets.data().len());

        write_null_mask(&self.nulls, write.buf);
        write.pad();

        for offset in self.zeroed_offsets() {
            write.buf.extend_from_slice(&offset.to_le_bytes())
        }
        write.pad();

        let values = self.values.slice(
            self.offsets.value(0) as usize,
            self.offsets.value(self.offsets.len() - 1) as usize - self.offsets.value(0) as usize
        );
        values.write_page(buf);
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            offsets: self.offsets.slice(offset, len + 1),
            values: self.values.clone(),
            nulls: self.nulls.as_ref().map(|nulls| nulls.slice(offset, len))
        }
    }
}


fn read_page<'a, T, F>(bytes: &'a [u8], read_values: F) -> anyhow::Result<ListSlice<'a, T>>
where
    F: FnOnce(&'a [u8]) -> anyhow::Result<T>,
    T: Slice<'a>
{
    let mut page = PageReader::new(bytes, Some(2))?;

    let nulls = page.read_null_mask()?;

    let offsets = NativeSlice::<'a, i32>::read_page(page.read_next_buffer()?)?;

    ensure!(offsets.len() > 0, "got zero length offsets array");
    for i in 1..offsets.len() {
        let current = offsets.value(i);
        let prev = offsets.value(i - 1);
        ensure!(prev <= current, "offset values are not monotonically increasing");
    }

    let null_mask_length_ok = nulls.as_ref()
        .map(|nulls| nulls.len() + 1 == offsets.len())
        .unwrap_or(true);

    ensure!(
            null_mask_length_ok,
            "null mask length doesn't match the offsets array"
        );

    let values = read_values(page.read_next_buffer()?)?;

    ensure!(
            values.len() == offsets.last_value() as usize,
            "last offset and values array length does not match"
        );

    Ok(ListSlice {
        offsets,
        values,
        nulls
    })
}


impl <'a, T: StaticSlice<'a>> StaticSlice<'a> for ListSlice<'a, T> {
    fn read_page(bytes: &'a [u8]) -> anyhow::Result<Self> {
        read_page(bytes, T::read_page)
    }
}


pub struct ListBuilder<T> {
    offsets: NativeBuilder<i32>,
    values: T,
    nulls: Option<BooleanBufferBuilder>
}


impl <T> DefaultDataBuilder for ListBuilder<T> {}
impl <T: Builder> Builder for ListBuilder<T> {
    type Slice<'a> = ListSlice<'a, T::Slice<'a>>;

    fn read_page<'a>(&self, page: &'a [u8]) -> anyhow::Result<Self::Slice<'a>> {
        read_page(page, |b| self.values.read_page(b))
    }

    fn push_slice(&mut self, slice: &Self::Slice<'_>) {
        let top = self.offsets.len();
        let last_offset = self.offsets.values()[top - 1];

        let slice_value_range = slice.offsets.value(0)..slice.offsets.last_value();

        self.offsets.push_slice(
            &slice.offsets.slice(1, slice.offsets.len() - 1)
        );

        for v in self.offsets.values_mut()[top..].iter_mut() {
            *v = *v - slice_value_range.start + last_offset
        }

        self.values.push_slice(
            &slice.values.slice(
                slice_value_range.start as usize, 
                slice_value_range.len()
            )
        );

        push_null_mask(
            self.values.len(),
            &slice.nulls,
            self.values.capacity(),
            &mut self.nulls
        )
    }

    fn as_slice(&self) -> Self::Slice<'_> {
        ListSlice {
            offsets: self.offsets.as_slice(),
            values: self.values.as_slice(),
            nulls: self.nulls.as_ref().map(Builder::as_slice)
        }
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn capacity(&self) -> usize {
        self.offsets.capacity() - 1
    }

    fn into_arrow_array(self, data_type: Option<DataType>) -> ArrayRef {
        let item_field = data_type.map(|t| {
            if let DataType::List(f) = t {
                f
            } else {
                panic!("list builder got unexpected data type - {}", t)
            }
        });
        
        let offsets = self.offsets.into_scalar_buffer();

        let values = self.values.into_arrow_array(
            item_field.as_ref().map(|f| f.data_type().clone())
        );

        let nulls = self.nulls.map(|mut nulls| nulls.finish().into());
        
        let array = ListArray::new(
            item_field.unwrap_or_else(|| {
                Arc::new(Field::new_list_field(values.data_type().clone(), true))
            }), 
            OffsetBuffer::new(offsets), 
            values, 
            nulls
        );
        
        Arc::new(array)
    }
}


impl <T> ListBuilder<T> {
    pub fn new(capacity: usize, values: T) -> Self {
        Self {
            offsets: NativeBuilder::with_capacity(capacity),
            values,
            nulls: None
        }
    }
}


pub type BinarySlice<'a> = ListSlice<'a, NativeSlice<'a, u8>>;
pub type BinaryBuilder = ListBuilder<NativeBuilder<u8>>;


impl Default for BinaryBuilder {
    fn default() -> Self {
        ListBuilder::new(0, NativeBuilder::default())
    }
}


impl <'a, T: ByteArrayType<Offset = i32>> From<&'a GenericByteArray<T>> for BinarySlice<'a> {
    fn from(value: &'a GenericByteArray<T>) -> Self {
        Self {
            offsets: value.offsets().inner().into(),
            values: value.values().as_slice().into(),
            nulls: value.nulls().map(|nulls| nulls.inner().into())
        }
    }
}


impl <'a> From<&'a ListArray> for AnySlice<'a> {
    fn from(value: &'a ListArray) -> Self {
        ListSlice::<'a, AnySlice<'a>> {
            offsets: value.offsets().inner().into(),
            values: value.values().as_ref().into(),
            nulls: value.nulls().map(|nulls| nulls.inner().into())
        }.into()
    }
}