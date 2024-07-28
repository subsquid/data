use std::sync::Arc;

use arrow::array::{Array, AsArray, BinaryArray, BooleanArray, PrimitiveArray, StringArray, StructArray};
use arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type};

use crate::{AnyStructBuilder, AnyStructSlice, BinaryBuilder, BinarySlice, ListSlice};
use crate::boolean::{BooleanBuilder, BooleanSlice};
use crate::list::ListBuilder;
use crate::primitive::{PrimitiveBuilder, PrimitiveSlice};
use crate::types::{Builder, Slice};


#[derive(Clone)]
enum InnerSlice<'a> {
    Boolean(BooleanSlice<'a>),
    UInt8(PrimitiveSlice<'a, UInt8Type>),
    UInt16(PrimitiveSlice<'a, UInt16Type>),
    UInt32(PrimitiveSlice<'a, UInt32Type>),
    UInt64(PrimitiveSlice<'a, UInt64Type>),
    Int8(PrimitiveSlice<'a, Int8Type>),
    Int16(PrimitiveSlice<'a, Int16Type>),
    Int32(PrimitiveSlice<'a, Int32Type>),
    Int64(PrimitiveSlice<'a, Int64Type>),
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
    fn read_page(_bytes: &'a [u8]) -> anyhow::Result<Self> {
        panic!("AnySlice cannot be read from a page")
    }

    fn write_page(&self, _buf: &mut Vec<u8>) {
        todo!()
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
    UInt8(PrimitiveBuilder<UInt8Type>),
    UInt16(PrimitiveBuilder<UInt16Type>),
    UInt32(PrimitiveBuilder<UInt32Type>),
    UInt64(PrimitiveBuilder<UInt64Type>),
    Int8(PrimitiveBuilder<Int8Type>),
    Int16(PrimitiveBuilder<Int16Type>),
    Int32(PrimitiveBuilder<Int32Type>),
    Int64(PrimitiveBuilder<Int64Type>),
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
            _ => panic!("slice type doesn't match the type of the builder")
        }
    }

    fn as_slice(&self) -> Self::Slice<'_> {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn capacity(&self) -> usize {
        todo!()
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
impl_from_primitive!(UInt8Type, UInt8);
impl_from_primitive!(UInt16Type, UInt16);
impl_from_primitive!(UInt32Type, UInt32);
impl_from_primitive!(UInt64Type, UInt64);
impl_from_primitive!(Int8Type, Int8);
impl_from_primitive!(Int16Type, Int16);
impl_from_primitive!(Int32Type, Int32);
impl_from_primitive!(Int64Type, Int64);


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
    Int64Type
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
            DataType::Binary => value.as_binary::<i32>().into(),
            DataType::Utf8 => value.as_string::<i32>().into(),
            DataType::List(_) => value.as_list::<i32>().into(),
            DataType::Struct(_) => value.as_struct().into(),
            t => panic!("unsupported arrow data type - {}", t)
        }
    }
}