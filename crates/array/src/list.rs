use arrow::array::{Array, GenericByteArray, ListArray};
use arrow::datatypes::ByteArrayType;
use arrow_buffer::BooleanBufferBuilder;

use crate::AnySlice;
use crate::bitmask::{BitSlice, push_null_mask, write_null_mask};
use crate::primitive::{NativeBuilder, NativeSlice};
use crate::types::{Builder, Slice};
use crate::util::PageWriter;


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
    fn read_page(_bytes: &'a [u8]) -> anyhow::Result<Self> {
        todo!()
    }

    unsafe fn read_valid_page(_bytes: &'a [u8]) -> Self {
        todo!()
    }

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


pub struct ListBuilder<T> {
    offsets: NativeBuilder<i32>,
    values: T,
    nulls: Option<BooleanBufferBuilder>
}


impl <T: Builder> Builder for ListBuilder<T> {
    type Slice<'a> = ListSlice<'a, T::Slice<'a>>;

    fn push_slice(&mut self, slice: &Self::Slice<'_>) {
        let top = self.offsets.len();
        let offset = self.offsets.values()[top - 1];
        let slice_offset = slice.offsets.value(0);

        self.offsets.push_slice(
            &slice.offsets.slice(1, slice.offsets.len() - 1)
        );

        for v in self.offsets.values_mut()[top..].iter_mut() {
            *v = *v - slice_offset + offset
        }

        self.values.push_slice(
            &slice.values.slice(slice_offset as usize, slice.len())
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
}


pub type BinarySlice<'a> = ListSlice<'a, NativeSlice<'a, u8>>;
pub type BinaryBuilder = ListBuilder<NativeBuilder<u8>>;


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
