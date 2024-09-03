use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;

use anyhow::ensure;
use arrow::array::{Array, ArrayDataBuilder, ArrayRef, ArrowPrimitiveType, PrimitiveArray};
use arrow::datatypes::{DataType, Date32Type, Date64Type, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type};
use arrow_buffer::{ArrowNativeType, BooleanBufferBuilder, MutableBuffer, NullBuffer, ScalarBuffer, ToByteSlice};

use crate::{DefaultDataBuilder, StaticSlice};
use crate::bitmask::{BitSlice, build_null_buffer, push_null_mask, write_null_mask};
use crate::types::{Builder, Slice};
use crate::util::{PageReader, PageWriter};


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

    pub unsafe fn value_unchecked(&self, idx: usize) -> T {
        let pos = idx * T::get_byte_width();
        let ptr = self.values.as_ptr().add(pos);
        std::ptr::read_unaligned(ptr.cast())
    }

    pub fn data(&self) -> &'a [u8] {
        self.values
    }

    pub fn last_value(&self) -> T {
        assert!(self.len() > 0);
        self.value(self.len() - 1)
    }
}


impl <'a, T: ArrowNativeType> Slice<'a> for NativeSlice<'a, T> {
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


impl <'a, T: ArrowNativeType> StaticSlice<'a> for NativeSlice<'a, T> {
    fn read_page(bytes: &'a [u8]) -> anyhow::Result<Self> {
        ensure!(bytes.len() % T::get_byte_width() == 0);
        Ok(Self {
            values: bytes,
            phantom_data: PhantomData::default()
        })
    }
}


pub struct NativeBuilder<T> {
    values: MutableBuffer,
    phantom_data: PhantomData<T>
}


impl <T: ArrowNativeType> NativeBuilder<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: MutableBuffer::with_capacity(capacity * T::get_byte_width()),
            phantom_data: PhantomData::default()
        }
    }

    pub fn values(&self) -> &[T] {
        self.values.typed_data()
    }

    pub fn values_mut(&mut self) -> &mut [T] {
        self.values.typed_data_mut()
    }

    pub fn push(&mut self, value: T) {
        self.values.push(value)
    }
}


impl <T: NativeType> DefaultDataBuilder for NativeBuilder<T> {}


impl <T: NativeType> Builder for NativeBuilder<T> {
    type Slice<'a> = NativeSlice<'a, T>;

    fn read_page<'a>(&self, page: &'a [u8]) -> anyhow::Result<Self::Slice<'a>> {
        Self::Slice::read_page(page)
    }

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

    fn into_arrow_array(self, data_type: Option<DataType>) -> ArrayRef {
        let data_type = if let Some(data_type) = data_type {
            assert!(T::check_data_type(&data_type), "got incompatible data type - {}", data_type);
            data_type
        } else {
            T::DEFAULT_DATA_TYPE.clone()
        };

        make_array(data_type, self.len(), self.values, None)
    }
}


fn make_array(
    data_type: DataType,
    len: usize,
    buffer: MutableBuffer,
    nulls: Option<NullBuffer>
) -> ArrayRef {
    macro_rules! make {
        ($t:ident) => {
            Arc::new(PrimitiveArray::<$t>::new(ScalarBuffer::from(buffer), nulls))
        };
    }

    match data_type {
        DataType::Int8 => make!(Int8Type),
        DataType::Int16 => make!(Int16Type),
        DataType::Int32 => make!(Int32Type),
        DataType::Int64 => make!(Int64Type),
        DataType::UInt8 => make!(UInt8Type),
        DataType::UInt16 => make!(UInt16Type),
        DataType::UInt32 => make!(UInt32Type),
        DataType::UInt64 => make!(UInt64Type),
        DataType::Float16 => make!(Float16Type),
        DataType::Float32 => make!(Float32Type),
        DataType::Float64 => make!(Float64Type),
        DataType::Date32 => make!(Date32Type),
        DataType::Date64 => make!(Date64Type),
        _ => {
            let data = ArrayDataBuilder::new(data_type)
                .len(len)
                .add_buffer(buffer.into())
                .nulls(nulls)
                .build()
                .unwrap();
            
            arrow::array::make_array(data)
        }
    }
}


impl <T: ArrowNativeType> NativeBuilder<T> {
    pub fn into_scalar_buffer(self) -> ScalarBuffer<T> {
        ScalarBuffer::from(self.values)
    }
}


impl <T: ArrowNativeType> Default for NativeBuilder<T> {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}


pub struct PrimitiveSlice<'a, T> {
    values: NativeSlice<'a, T>,
    nulls: Option<BitSlice<'a>>
}


impl <'a, T: ArrowNativeType> Clone for PrimitiveSlice<'a, T> {
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            nulls: self.nulls.clone()
        }
    }
}


impl <'a, T: ArrowNativeType> Slice<'a> for PrimitiveSlice<'a, T> {
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


impl <'a, T: ArrowNativeType> StaticSlice<'a> for PrimitiveSlice<'a, T> {
    fn read_page(bytes: &'a [u8]) -> anyhow::Result<Self> {
        let mut page = PageReader::new(bytes, Some(1))?;

        let nulls = page.read_null_mask()?;

        let values = NativeSlice::<'a, T>::read_page(page.read_next_buffer()?)?;

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


pub struct PrimitiveBuilder<T> {
    values: NativeBuilder<T>,
    nulls: Option<BooleanBufferBuilder>
}


impl <T> DefaultDataBuilder for PrimitiveBuilder<T> {}
impl <T: NativeType> Builder for PrimitiveBuilder<T> {
    type Slice<'a> = PrimitiveSlice<'a, T>;

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

    fn into_arrow_array(self, data_type: Option<DataType>) -> ArrayRef {
        let data_type = if let Some(data_type) = data_type {
            assert!(T::check_data_type(&data_type), "got incompatible data type - {}", data_type);
            data_type
        } else {
            T::DEFAULT_DATA_TYPE.clone()
        };

        make_array(data_type, self.len(), self.values.values, self.nulls.and_then(build_null_buffer))
    }
}


trait NativeType: ArrowNativeType {
    const DEFAULT_DATA_TYPE: DataType;

    fn check_data_type(data_type: &DataType) -> bool {
        data_type == &Self::DEFAULT_DATA_TYPE
    }
}


macro_rules! impl_simple_native_type {
    ($t:ident, $dt:ident) => {
        impl NativeType for $t {
            const DEFAULT_DATA_TYPE: DataType = DataType::$dt;
        }
    };
}
impl_simple_native_type!(u8, UInt8);
impl_simple_native_type!(u16, UInt16);
impl_simple_native_type!(u32, UInt32);
impl_simple_native_type!(u64, UInt64);
impl_simple_native_type!(i8, Int8);
impl_simple_native_type!(i16, Int16);
impl_simple_native_type!(i32, Int32);


impl NativeType for i64 {
    const DEFAULT_DATA_TYPE: DataType = DataType::Int64;

    fn check_data_type(data_type: &DataType) -> bool {
        match data_type {
            DataType::Int64 => true,
            DataType::Timestamp(_, _) => true,
            _ => false
        }
    }
}


impl <T: ArrowNativeType> Default for PrimitiveBuilder<T> {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}


impl <T: ArrowNativeType> PrimitiveBuilder<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: NativeBuilder::with_capacity(capacity),
            nulls: None
        }
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


impl <'a, T: ArrowPrimitiveType> From<&'a PrimitiveArray<T>> for PrimitiveSlice<'a, T::Native> {
    fn from(value: &'a PrimitiveArray<T>) -> Self {
        Self {
            values: value.values().into(),
            nulls: value.nulls().map(|nulls| nulls.inner().into())
        }
    }
}