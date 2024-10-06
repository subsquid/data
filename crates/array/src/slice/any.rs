use crate::slice::boolean::BooleanSlice;
use crate::slice::list::ListSlice;
use crate::slice::primitive::PrimitiveSlice;
use crate::slice::r#struct::AnyStructSlice;
use crate::slice::Slice;
use crate::writer::{ArrayWriter, RangeList};
use std::ops::Range;
use std::sync::Arc;


#[derive(Clone)]
pub enum AnySlice<'a> {
    Boolean(BooleanSlice<'a>),
    UInt8(PrimitiveSlice<'a, u8>),
    UInt16(PrimitiveSlice<'a, u16>),
    UInt32(PrimitiveSlice<'a, u32>),
    UInt64(PrimitiveSlice<'a, u64>),
    Int8(PrimitiveSlice<'a, i8>),
    Int16(PrimitiveSlice<'a, i16>),
    Int32(PrimitiveSlice<'a, i32>),
    Int64(PrimitiveSlice<'a, i64>),
    Binary(ListSlice<'a, &'a [u8]>),
    List(ListSlice<'a, Arc<AnySlice<'a>>>),
    Struct(AnyStructSlice<'a>)
}


impl <'a> Slice for AnySlice<'a> {
    fn num_buffers(&self) -> usize {
        match self {
            AnySlice::Boolean(s) => s.num_buffers(),
            AnySlice::UInt8(s) => s.num_buffers(),
            AnySlice::UInt16(s) => s.num_buffers(),
            AnySlice::UInt32(s) => s.num_buffers(),
            AnySlice::UInt64(s) => s.num_buffers(),
            AnySlice::Int8(s) => s.num_buffers(),
            AnySlice::Int16(s) => s.num_buffers(),
            AnySlice::Int32(s) => s.num_buffers(),
            AnySlice::Int64(s) => s.num_buffers(),
            AnySlice::Binary(s) => s.num_buffers(),
            AnySlice::List(s) => s.num_buffers(),
            AnySlice::Struct(s) => s.num_buffers(),
        }
    }

    fn len(&self) -> usize {
        match self {
            AnySlice::Boolean(s) => s.len(),
            AnySlice::UInt8(s) => s.len(),
            AnySlice::UInt16(s) => s.len(),
            AnySlice::UInt32(s) => s.len(),
            AnySlice::UInt64(s) => s.len(),
            AnySlice::Int8(s) => s.len(),
            AnySlice::Int16(s) => s.len(),
            AnySlice::Int32(s) => s.len(),
            AnySlice::Int64(s) => s.len(),
            AnySlice::Binary(s) => s.len(),
            AnySlice::List(s) => s.len(),
            AnySlice::Struct(s) => s.len(),
        }
    }

    fn slice(&self, offset: usize, len: usize) -> Self {
        match self {
            AnySlice::Boolean(s) => AnySlice::Boolean(s.slice(offset, len)),
            AnySlice::UInt8(s) => AnySlice::UInt8(s.slice(offset, len)),
            AnySlice::UInt16(s) => AnySlice::UInt16(s.slice(offset, len)),
            AnySlice::UInt32(s) => AnySlice::UInt32(s.slice(offset, len)),
            AnySlice::UInt64(s) => AnySlice::UInt64(s.slice(offset, len)),
            AnySlice::Int8(s) => AnySlice::Int8(s.slice(offset, len)),
            AnySlice::Int16(s) => AnySlice::Int16(s.slice(offset, len)),
            AnySlice::Int32(s) => AnySlice::Int32(s.slice(offset, len)),
            AnySlice::Int64(s) => AnySlice::Int64(s.slice(offset, len)),
            AnySlice::Binary(s) => AnySlice::Binary(s.slice(offset, len)),
            AnySlice::List(s) => AnySlice::List(s.slice(offset, len)),
            AnySlice::Struct(s) => AnySlice::Struct(s.slice(offset, len)),
        }
    }

    fn write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        match self {
            AnySlice::Boolean(s) => s.write(dst),
            AnySlice::UInt8(s) => s.write(dst),
            AnySlice::UInt16(s) => s.write(dst),
            AnySlice::UInt32(s) => s.write(dst),
            AnySlice::UInt64(s) => s.write(dst),
            AnySlice::Int8(s) => s.write(dst),
            AnySlice::Int16(s) => s.write(dst),
            AnySlice::Int32(s) => s.write(dst),
            AnySlice::Int64(s) => s.write(dst),
            AnySlice::Binary(s) => s.write(dst),
            AnySlice::List(s) => s.write(dst),
            AnySlice::Struct(s) => s.write(dst),
        }
    }

    fn write_range(&self, dst: &mut impl ArrayWriter, range: Range<usize>) -> anyhow::Result<()> {
        match self {
            AnySlice::Boolean(s) => s.write_range(dst, range),
            AnySlice::UInt8(s) => s.write_range(dst, range),
            AnySlice::UInt16(s) => s.write_range(dst, range),
            AnySlice::UInt32(s) => s.write_range(dst, range),
            AnySlice::UInt64(s) => s.write_range(dst, range),
            AnySlice::Int8(s) => s.write_range(dst, range),
            AnySlice::Int16(s) => s.write_range(dst, range),
            AnySlice::Int32(s) => s.write_range(dst, range),
            AnySlice::Int64(s) => s.write_range(dst, range),
            AnySlice::Binary(s) => s.write_range(dst, range),
            AnySlice::List(s) => s.write_range(dst, range),
            AnySlice::Struct(s) => s.write_range(dst, range),
        }
    }

    fn write_ranges(&self, dst: &mut impl ArrayWriter, ranges: &mut impl RangeList) -> anyhow::Result<()> {
        match self {
            AnySlice::Boolean(s) => s.write_ranges(dst, ranges),
            AnySlice::UInt8(s) => s.write_ranges(dst, ranges),
            AnySlice::UInt16(s) => s.write_ranges(dst, ranges),
            AnySlice::UInt32(s) => s.write_ranges(dst, ranges),
            AnySlice::UInt64(s) => s.write_ranges(dst, ranges),
            AnySlice::Int8(s) => s.write_ranges(dst, ranges),
            AnySlice::Int16(s) => s.write_ranges(dst, ranges),
            AnySlice::Int32(s) => s.write_ranges(dst, ranges),
            AnySlice::Int64(s) => s.write_ranges(dst, ranges),
            AnySlice::Binary(s) => s.write_ranges(dst, ranges),
            AnySlice::List(s) => s.write_ranges(dst, ranges),
            AnySlice::Struct(s) => s.write_ranges(dst, ranges),
        }
    }

    fn write_indexes(&self, dst: &mut impl ArrayWriter, indexes: impl IntoIterator<Item=usize, IntoIter: Clone>) -> anyhow::Result<()> {
        match self {
            AnySlice::Boolean(s) => s.write_indexes(dst, indexes),
            AnySlice::UInt8(s) => s.write_indexes(dst, indexes),
            AnySlice::UInt16(s) => s.write_indexes(dst, indexes),
            AnySlice::UInt32(s) => s.write_indexes(dst, indexes),
            AnySlice::UInt64(s) => s.write_indexes(dst, indexes),
            AnySlice::Int8(s) => s.write_indexes(dst, indexes),
            AnySlice::Int16(s) => s.write_indexes(dst, indexes),
            AnySlice::Int32(s) => s.write_indexes(dst, indexes),
            AnySlice::Int64(s) => s.write_indexes(dst, indexes),
            AnySlice::Binary(s) => s.write_indexes(dst, indexes),
            AnySlice::List(s) => s.write_indexes(dst, indexes),
            AnySlice::Struct(s) => s.write_indexes(dst, indexes),
        }
    }
}