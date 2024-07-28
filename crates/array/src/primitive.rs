use std::marker::PhantomData;
use std::ops::Range;

use arrow::array::{Array, ArrowPrimitiveType, PrimitiveArray};
use arrow_buffer::{ArrowNativeType, BooleanBufferBuilder, MutableBuffer, ToByteSlice};

use crate::bitmask::{BitSlice, push_null_mask, write_null_mask};
use crate::types::{Builder, Slice};
use crate::util::Padding;


#[derive(Clone)]
pub struct NativeSlice<'a, T> {
    values: &'a [u8],
    phantom_data: PhantomData<T>
}


impl<'a, T: ArrowNativeType> NativeSlice<'a, T> {
    pub fn value(&self, idx: usize) -> T {
        let beg = idx * T::get_byte_width();
        let end = beg + T::get_byte_width();
        let ptr = self.values[beg..end].as_ptr();
        unsafe {
            std::ptr::read_unaligned(ptr.cast())
        }
    }
    
    pub fn data(&self) -> &'a [u8] {
        self.values
    }
}


impl <'a, T: ArrowNativeType> Slice<'a> for NativeSlice<'a, T> {
    fn read_page(_bytes: &'a [u8]) -> anyhow::Result<Self> {
        todo!()
    }

    unsafe fn read_valid_page(_bytes: &'a [u8]) -> Self {
        todo!()
    }

    fn write_page(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.values)
    }

    fn len(&self) -> usize {
        self.values.len() / T::get_byte_width()
    }

    fn slice_range(&self, range: Range<usize>) -> Self {
        let beg = range.start * T::get_byte_width();
        let end = range.end * T::get_byte_width();
        let values = &self.values[beg..end];
        Self {
            values,
            phantom_data: PhantomData::default()
        }
    }
}


pub struct NativeBuilder<T> {
    values: MutableBuffer,
    phantom_data: PhantomData<T>
}


impl <T: ArrowNativeType> NativeBuilder<T> {
    pub fn values(&self) -> &[T] {
        self.values.typed_data()
    }

    pub fn values_mut(&mut self) -> &mut [T] {
        self.values.typed_data_mut()
    }
}


impl <T: ArrowNativeType> Builder for NativeBuilder<T> {
    type Slice<'a> = NativeSlice<'a, T>;

    fn push_slice(&mut self, slice: &Self::Slice<'_>) {
        self.values.extend_from_slice(slice.values)
    }

    fn as_slice(&self) -> Self::Slice<'_> {
        NativeSlice {
            values: self.values.as_slice(),
            phantom_data: PhantomData::default()
        }
    }

    fn len(&self) -> usize {
        self.values().len()
    }

    fn capacity(&self) -> usize {
        self.values.capacity() / T::get_byte_width()
    }
}


pub struct PrimitiveSlice<'a, T: ArrowPrimitiveType> {
    values: NativeSlice<'a, T::Native>,
    nulls: Option<BitSlice<'a>>
}


impl <'a, T: ArrowPrimitiveType> Clone for PrimitiveSlice<'a, T> {
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            nulls: self.nulls.clone()
        }
    }
}


impl <'a, T: ArrowPrimitiveType> Slice<'a> for PrimitiveSlice<'a, T> {
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


pub struct PrimitiveBuilder<T: ArrowPrimitiveType> {
    values: NativeBuilder<T::Native>,
    nulls: Option<BooleanBufferBuilder>
}


impl <T: ArrowPrimitiveType> Builder for PrimitiveBuilder<T> {
    type Slice<'a> = PrimitiveSlice<'a, T>;

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
        PrimitiveSlice {
            values: self.values.as_slice(),
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


impl <'a, T: ArrowNativeType> From<&'a [T]> for NativeSlice<'a, T> {
    fn from(value: &'a [T]) -> Self {
        Self {
            values: value.to_byte_slice(),
            phantom_data: PhantomData::default()
        }
    }
}


impl <'a, T: ArrowNativeType, R: AsRef<[T]>> From<&'a R> for NativeSlice<'a, T> {
    fn from(value: &'a R) -> Self {
        value.as_ref().into()
    }
}


impl <'a, T: ArrowPrimitiveType> From<&'a PrimitiveArray<T>> for PrimitiveSlice<'a, T> {
    fn from(value: &'a PrimitiveArray<T>) -> Self {
        Self {
            values: value.values().into(),
            nulls: value.nulls().map(|nulls| nulls.inner().into())
        }
    }
}