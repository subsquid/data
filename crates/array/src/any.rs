use std::sync::Arc;

use arrow::array::{Array, AsArray, BinaryArray, BooleanArray, PrimitiveArray, StringArray, StructArray};
use arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type, TimestampMillisecondType, TimestampSecondType, TimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type};

use crate::{AnyStructBuilder, AnyStructSlice, BinaryBuilder, BinarySlice, ListSlice, read_any_list_page, read_any_struct_page, StaticSlice};
use crate::boolean::{BooleanBuilder, BooleanSlice};
use crate::list::ListBuilder;
use crate::primitive::{PrimitiveBuilder, PrimitiveSlice};
use crate::types::{Builder, Slice};


#[derive(Clone)]
enum InnerSlice<'a> {
    Boolean(BooleanSlice<'a>),
    UInt8(PrimitiveSlice<'a, u8>),
    UInt16(PrimitiveSlice<'a, u16>),
    UInt32(PrimitiveSlice<'a, u32>),
    UInt64(PrimitiveSlice<'a, u64>),
    Int8(PrimitiveSlice<'a, i8>),
    Int16(PrimitiveSlice<'a, i16>),
    Int32(PrimitiveSlice<'a, i32>),
    Int64(PrimitiveSlice<'a, i64>),
    Binary(BinarySlice<'a>),
    List(Arc<ListSlice<'a, AnySlice<'a>>>),
    Struct(AnyStructSlice<'a>)
}


// We could make AnySlice to be a simple enum,
// but we are trying to make slicing as fast as possible.
//
// The idea is that just coping an enum and setting a new offset and length
// will save us at least one variant dispatch.
//
#[derive(Clone)]
pub struct AnySlice<'a> {
    inner: InnerSlice<'a>,
    offset: usize,
    len: usize
}


impl <'a> Slice<'a> for AnySlice<'a> {
    fn write_page(&self, buf: &mut Vec<u8>) {
        match &self.inner {
            InnerSlice::Boolean(s) => s.slice(self.offset, self.len).write_page(buf),
            InnerSlice::UInt8(s) => s.slice(self.offset, self.len).write_page(buf),
            InnerSlice::UInt16(s) => s.slice(self.offset, self.len).write_page(buf),
            InnerSlice::UInt32(s) => s.slice(self.offset, self.len).write_page(buf),
            InnerSlice::UInt64(s) => s.slice(self.offset, self.len).write_page(buf),
            InnerSlice::Int8(s) => s.slice(self.offset, self.len).write_page(buf),
            InnerSlice::Int16(s) => s.slice(self.offset, self.len).write_page(buf),
            InnerSlice::Int32(s) => s.slice(self.offset, self.len).write_page(buf),
            InnerSlice::Int64(s) => s.slice(self.offset, self.len).write_page(buf),
            InnerSlice::Binary(s) => s.slice(self.offset, self.len).write_page(buf),
            InnerSlice::List(s) => s.slice(self.offset, self.len).write_page(buf),
            InnerSlice::Struct(s) => s.slice(self.offset, self.len).write_page(buf)
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(offset + len <= self.len);
        Self {
            inner: self.inner.clone(),
            offset: self.offset + offset,
            len
        }
    }
}


pub enum AnyBuilder {
    Boolean(BooleanBuilder),
    UInt8(PrimitiveBuilder<u8>),
    UInt16(PrimitiveBuilder<u16>),
    UInt32(PrimitiveBuilder<u32>),
    UInt64(PrimitiveBuilder<u64>),
    Int8(PrimitiveBuilder<i8>),
    Int16(PrimitiveBuilder<i16>),
    Int32(PrimitiveBuilder<i32>),
    Int64(PrimitiveBuilder<i64>),
    Binary(BinaryBuilder),
    List(ListBuilder<Box<AnyBuilder>>),
    Struct(AnyStructBuilder)
}


impl Builder for AnyBuilder {
    type Slice<'a> = AnySlice<'a>;

    fn push_slice(&mut self, slice: &Self::Slice<'_>) {
        macro_rules! push {
            ($b:ident, $s:ident) => {
                $b.push_slice(&$s.slice(slice.offset, slice.len))
            };
        }
        match (self, &slice.inner) {
            (AnyBuilder::Boolean(b), InnerSlice::Boolean(s)) => push!(b, s),
            (AnyBuilder::UInt8(b), InnerSlice::UInt8(s)) => push!(b, s),
            (AnyBuilder::UInt16(b), InnerSlice::UInt16(s)) => push!(b, s),
            (AnyBuilder::UInt32(b), InnerSlice::UInt32(s)) => push!(b, s),
            (AnyBuilder::UInt64(b), InnerSlice::UInt64(s)) => push!(b, s),
            (AnyBuilder::Int8(b), InnerSlice::Int8(s)) => push!(b, s),
            (AnyBuilder::Int16(b), InnerSlice::Int16(s)) => push!(b, s),
            (AnyBuilder::Int32(b), InnerSlice::Int32(s)) => push!(b, s),
            (AnyBuilder::Int64(b), InnerSlice::Int64(s)) => push!(b, s),
            (AnyBuilder::Binary(b), InnerSlice::Binary(s)) => push!(b, s),
            (AnyBuilder::Struct(b), InnerSlice::Struct(s)) => push!(b, s),
            (AnyBuilder::List(b), InnerSlice::List(s)) => push!(b, s),
            _ => panic!("slice type doesn't match the type of the builder")
        }
    }

    fn as_slice(&self) -> Self::Slice<'_> {
        match &self {
            AnyBuilder::Boolean(b) => b.as_slice().into(),
            AnyBuilder::UInt8(b) => b.as_slice().into(),
            AnyBuilder::UInt16(b) => b.as_slice().into(),
            AnyBuilder::UInt32(b) => b.as_slice().into(),
            AnyBuilder::UInt64(b) => b.as_slice().into(),
            AnyBuilder::Int8(b) => b.as_slice().into(),
            AnyBuilder::Int16(b) => b.as_slice().into(),
            AnyBuilder::Int32(b) => b.as_slice().into(),
            AnyBuilder::Int64(b) => b.as_slice().into(),
            AnyBuilder::Binary(b) => b.as_slice().into(),
            AnyBuilder::List(b) => b.as_slice().into(),
            AnyBuilder::Struct(b) => b.as_slice().into(),
        }
    }

    fn len(&self) -> usize {
        match self {
            AnyBuilder::Boolean(b) => b.len(),
            AnyBuilder::UInt8(b) => b.len(),
            AnyBuilder::UInt16(b) => b.len(),
            AnyBuilder::UInt32(b) => b.len(),
            AnyBuilder::UInt64(b) => b.len(),
            AnyBuilder::Int8(b) => b.len(),
            AnyBuilder::Int16(b) => b.len(),
            AnyBuilder::Int32(b) => b.len(),
            AnyBuilder::Int64(b) => b.len(),
            AnyBuilder::Binary(b) => b.len(),
            AnyBuilder::List(b) => b.len(),
            AnyBuilder::Struct(b) => b.len(),
        }
    }

    fn capacity(&self) -> usize {
        match self {
            AnyBuilder::Boolean(b) => b.capacity(),
            AnyBuilder::UInt8(b) => b.capacity(),
            AnyBuilder::UInt16(b) => b.capacity(),
            AnyBuilder::UInt32(b) => b.capacity(),
            AnyBuilder::UInt64(b) => b.capacity(),
            AnyBuilder::Int8(b) => b.capacity(),
            AnyBuilder::Int16(b) => b.capacity(),
            AnyBuilder::Int32(b) => b.capacity(),
            AnyBuilder::Int64(b) => b.capacity(),
            AnyBuilder::Binary(b) => b.capacity(),
            AnyBuilder::List(b) => b.capacity(),
            AnyBuilder::Struct(b) => b.capacity(),
        }
    }
}


impl <'a> From<BooleanSlice<'a>> for AnySlice<'a> {
    fn from(value: BooleanSlice<'a>) -> Self {
        let len = value.len();
        Self {
            inner: InnerSlice::Boolean(value),
            offset: 0,
            len
        }
    }
}


macro_rules! impl_from_primitive {
    ($t:ty, $case:ident) => {
        impl <'a> From<PrimitiveSlice<'a, $t>> for AnySlice<'a> {
            fn from(value: PrimitiveSlice<'a, $t>) -> Self {
                let len = value.len();
                Self {
                    inner: InnerSlice::$case(value),
                    offset: 0,
                    len
                }
            }
        }
    };
}
impl_from_primitive!(u8, UInt8);
impl_from_primitive!(u16, UInt16);
impl_from_primitive!(u32, UInt32);
impl_from_primitive!(u64, UInt64);
impl_from_primitive!(i8, Int8);
impl_from_primitive!(i16, Int16);
impl_from_primitive!(i32, Int32);
impl_from_primitive!(i64, Int64);


impl <'a> From<BinarySlice<'a>> for AnySlice<'a> {
    fn from(value: BinarySlice<'a>) -> Self {
        let len = value.len();
        Self {
            inner: InnerSlice::Binary(value),
            offset: 0,
            len
        }
    }
}


impl<'a> From<AnyStructSlice<'a>> for AnySlice<'a> {
    fn from(value: AnyStructSlice<'a>) -> Self {
        let len = value.len();
        Self {
            inner: InnerSlice::Struct(value),
            offset: 0,
            len
        }
    }
}


impl <'a> From<&'a BooleanArray> for AnySlice<'a> {
    fn from(value: &'a BooleanArray) -> Self {
        BooleanSlice::from(value).into()
    }
}


impl <'a> From<&'a BinaryArray> for AnySlice<'a> {
    fn from(value: &'a BinaryArray) -> Self {
        BinarySlice::from(value).into()
    }
}


impl <'a> From<&'a StringArray> for AnySlice<'a>  {
    fn from(value: &'a StringArray) -> Self {
        BinarySlice::from(value).into()
    }
}


macro_rules! impl_from_primitive_array {
    ($($arr:ty),*) => {
        $(
            impl <'a> From<&'a PrimitiveArray<$arr>> for AnySlice<'a> {
                fn from(value: &'a PrimitiveArray<$arr>) -> Self {
                    PrimitiveSlice::from(value).into()
                }
            }
        )*
    };
}
impl_from_primitive_array!(
    UInt8Type,
    UInt16Type,
    UInt32Type,
    UInt64Type,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    TimestampSecondType,
    TimestampMillisecondType
);


impl <'a> From<ListSlice<'a, AnySlice<'a>>> for AnySlice<'a> {
    fn from(value: ListSlice<'a, AnySlice<'a>>) -> Self {
        let len = value.len();
        Self {
            inner: InnerSlice::List(Arc::new(value)),
            offset: 0,
            len
        }
    }
}


impl <'a> From<&'a StructArray> for AnySlice<'a> {
    fn from(value: &'a StructArray) -> Self {
        let slice = AnyStructSlice::from(value);
        let len = slice.len();
        Self {
            inner: InnerSlice::Struct(slice),
            offset: 0,
            len
        }
    }
}


impl <'a> From<&'a dyn Array> for AnySlice<'a> {
    fn from(value: &'a dyn Array) -> Self {
        match value.data_type() {
            DataType::Boolean => value.as_boolean().into(),
            DataType::Int8 => value.as_primitive::<Int8Type>().into(),
            DataType::Int16 => value.as_primitive::<Int16Type>().into(),
            DataType::Int32 => value.as_primitive::<Int32Type>().into(),
            DataType::Int64 => value.as_primitive::<Int64Type>().into(),
            DataType::UInt8 => value.as_primitive::<UInt8Type>().into(),
            DataType::UInt16 => value.as_primitive::<UInt16Type>().into(),
            DataType::UInt32 => value.as_primitive::<UInt32Type>().into(),
            DataType::UInt64 => value.as_primitive::<UInt64Type>().into(),
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                value.as_primitive::<TimestampMillisecondType>().into()
            },
            DataType::Timestamp(TimeUnit::Second, _) => {
                value.as_primitive::<TimestampSecondType>().into()
            },
            DataType::Binary => value.as_binary::<i32>().into(),
            DataType::Utf8 => value.as_string::<i32>().into(),
            DataType::List(_) => value.as_list::<i32>().into(),
            DataType::Struct(_) => value.as_struct().into(),
            t => panic!("unsupported arrow data type - {}", t)
        }
    }
}


pub fn read_any_page<'a>(bytes: &'a [u8], data_type: &DataType) -> anyhow::Result<AnySlice<'a>> {
    match data_type {
        DataType::Boolean => BooleanSlice::read_page(bytes).map(Into::into),
        DataType::Int8 => PrimitiveSlice::<i8>::read_page(bytes).map(Into::into),
        DataType::Int16 => PrimitiveSlice::<i16>::read_page(bytes).map(Into::into),
        DataType::Int32 => PrimitiveSlice::<i32>::read_page(bytes).map(Into::into),
        DataType::Int64 => PrimitiveSlice::<i64>::read_page(bytes).map(Into::into),
        DataType::UInt8 => PrimitiveSlice::<u8>::read_page(bytes).map(Into::into),
        DataType::UInt16 => PrimitiveSlice::<u16>::read_page(bytes).map(Into::into),
        DataType::UInt32 => PrimitiveSlice::<u32>::read_page(bytes).map(Into::into),
        DataType::UInt64 => PrimitiveSlice::<u64>::read_page(bytes).map(Into::into),
        DataType::Timestamp(_, _) => PrimitiveSlice::<i64>::read_page(bytes).map(Into::into),
        DataType::Binary | DataType::Utf8 => BinarySlice::read_page(bytes).map(Into::into),
        DataType::List(f) => read_any_list_page(bytes, f.data_type()).map(Into::into),
        DataType::Struct(fields) => read_any_struct_page(bytes, &fields).map(Into::into),
        ty => panic!("unsupported arrow type - {}", ty)
    }
}