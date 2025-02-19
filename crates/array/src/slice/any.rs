use crate::index::RangeList;
use crate::slice::boolean::BooleanSlice;
use crate::slice::fixed_size_list::FixedSizeListSlice;
use crate::slice::list::ListSlice;
use crate::slice::primitive::PrimitiveSlice;
use crate::slice::r#struct::AnyStructSlice;
use crate::slice::{AsSlice, Slice};
use crate::writer::ArrayWriter;
use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit, TimestampMillisecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type};
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
    FixedSizeBinary(FixedSizeListSlice<'a, &'a [u8]>),
    List(ListSlice<'a, AnyListItem<'a>>),
    FixedSizeList(FixedSizeListSlice<'a, AnyListItem<'a>>),
    Struct(AnyStructSlice<'a>)
}


impl<'a> AnySlice<'a> {
    pub fn as_bool(&self) -> BooleanSlice<'a> {
        match self {
            AnySlice::Boolean(s) => s.clone(),
            _ => panic!("not a boolean slice")
        }
    }

    pub fn as_u8(&self) -> PrimitiveSlice<'a, u8> {
        match self {
            AnySlice::UInt8(s) => s.clone(),
            _ => panic!("not a u8 slice")
        }
    }

    pub fn as_u16(&self) -> PrimitiveSlice<'a, u16> {
        match self {
            AnySlice::UInt16(s) => s.clone(),
            _ => panic!("not a u16 slice")
        }
    }

    pub fn as_u32(&self) -> PrimitiveSlice<'a, u32> {
        match self {
            AnySlice::UInt32(s) => s.clone(),
            _ => panic!("not a u32 slice")
        }
    }

    pub fn as_u64(&self) -> PrimitiveSlice<'a, u64> {
        match self {
            AnySlice::UInt64(s) => s.clone(),
            _ => panic!("not a u64 slice")
        }
    }

    pub fn as_i8(&self) -> PrimitiveSlice<'a, i8> {
        match self {
            AnySlice::Int8(s) => s.clone(),
            _ => panic!("not a i8 slice")
        }
    }

    pub fn as_i16(&self) -> PrimitiveSlice<'a, i16> {
        match self {
            AnySlice::Int16(s) => s.clone(),
            _ => panic!("not a i16 slice")
        }
    }

    pub fn as_i32(&self) -> PrimitiveSlice<'a, i32> {
        match self {
            AnySlice::Int32(s) => s.clone(),
            _ => panic!("not a i32 slice")
        }
    }

    pub fn as_i64(&self) -> PrimitiveSlice<'a, i64> {
        match self {
            AnySlice::Int64(s) => s.clone(),
            _ => panic!("not a i64 slice")
        }
    }

    pub fn as_binary(&self) -> ListSlice<'a, &'a [u8]> {
        match self {
            AnySlice::Binary(s) => s.clone(),
            _ => panic!("not a binary slice")
        }
    }

    pub fn as_fixed_size_binary(&self) -> FixedSizeListSlice<'a, &'a [u8]> {
        match self {
            AnySlice::FixedSizeBinary(s) => s.clone(),
            _ => panic!("not a fixed size binary slice")
        }
    }

    pub fn as_list(&self) -> ListSlice<'a, AnySlice<'a>> {
        match self {
            AnySlice::List(s) => ListSlice::new(s.offsets(), s.values().item(), s.nulls().bitmask()),
            _ => panic!("not a list slice")
        }
    }

    pub fn as_fixed_size_list(&self) -> FixedSizeListSlice<'a, AnySlice<'a>> {
        match self {
            AnySlice::FixedSizeList(s) => FixedSizeListSlice::new(s.size(), s.values().item(), s.nulls().bitmask()),
            _ => panic!("not a fixed size list slice")
        }
    }

    pub fn as_struct(&self) -> AnyStructSlice<'a> {
        match self {
            AnySlice::Struct(s) => s.clone(),
            _ => panic!("not a struct slice")
        }
    }
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
            AnySlice::FixedSizeBinary(s) => s.num_buffers(),
            AnySlice::List(s) => s.num_buffers(),
            AnySlice::FixedSizeList(s) => s.num_buffers(),
            AnySlice::Struct(s) => s.num_buffers(),
        }
    }

    fn byte_size(&self) -> usize {
        match self {
            AnySlice::Boolean(s) => s.byte_size(),
            AnySlice::UInt8(s) => s.byte_size(),
            AnySlice::UInt16(s) => s.byte_size(),
            AnySlice::UInt32(s) => s.byte_size(),
            AnySlice::UInt64(s) => s.byte_size(),
            AnySlice::Int8(s) => s.byte_size(),
            AnySlice::Int16(s) => s.byte_size(),
            AnySlice::Int32(s) => s.byte_size(),
            AnySlice::Int64(s) => s.byte_size(),
            AnySlice::Binary(s) => s.byte_size(),
            AnySlice::FixedSizeBinary(s) => s.byte_size(),
            AnySlice::List(s) => s.byte_size(),
            AnySlice::FixedSizeList(s) => s.byte_size(),
            AnySlice::Struct(s) => s.byte_size(),
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
            AnySlice::FixedSizeBinary(s) => s.len(),
            AnySlice::List(s) => s.len(),
            AnySlice::FixedSizeList(s) => s.len(),
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
            AnySlice::FixedSizeBinary(s) => AnySlice::FixedSizeBinary(s.slice(offset, len)),
            AnySlice::List(s) => AnySlice::List(s.slice(offset, len)),
            AnySlice::FixedSizeList(s) => AnySlice::FixedSizeList(s.slice(offset, len)),
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
            AnySlice::FixedSizeBinary(s) => s.write(dst),
            AnySlice::List(s) => s.write(dst),
            AnySlice::FixedSizeList(s) => s.write(dst),
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
            AnySlice::FixedSizeBinary(s) => s.write_range(dst, range),
            AnySlice::List(s) => s.write_range(dst, range),
            AnySlice::FixedSizeList(s) => s.write_range(dst, range),
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
            AnySlice::FixedSizeBinary(s) => s.write_ranges(dst, ranges),
            AnySlice::List(s) => s.write_ranges(dst, ranges),
            AnySlice::FixedSizeList(s) => s.write_ranges(dst, ranges),
            AnySlice::Struct(s) => s.write_ranges(dst, ranges),
        }
    }

    fn write_indexes(
        &self, 
        dst: &mut impl ArrayWriter, 
        indexes: impl Iterator<Item=usize> + Clone
    ) -> anyhow::Result<()> 
    {
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
            AnySlice::FixedSizeBinary(s) => s.write_indexes(dst, indexes),
            AnySlice::List(s) => s.write_indexes(dst, indexes),
            AnySlice::FixedSizeList(s) => s.write_indexes(dst, indexes),
            AnySlice::Struct(s) => s.write_indexes(dst, indexes),
        }
    }
}


#[derive(Clone)]
pub struct AnyListItem<'a> {
    item: Arc<AnySlice<'a>>
}


impl<'a> AnyListItem<'a> {
    pub fn new(item: AnySlice<'a>) -> Self {
        Self {
            item: Arc::new(item)
        }
    }
    
    pub fn item(&self) -> AnySlice<'a> {
        self.item.as_ref().clone()
    }
}


impl<'a> Slice for AnyListItem<'a> {
    #[inline]
    fn num_buffers(&self) -> usize {
        self.item.num_buffers()
    }

    fn byte_size(&self) -> usize {
        self.item.byte_size()
    }

    #[inline]
    fn len(&self) -> usize {
        self.item.len()
    }

    #[inline]
    fn slice(&self, _offset: usize, _len: usize) -> Self {
        unimplemented!("list item does not support slicing")
    }

    #[inline]
    fn write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        self.item.write(dst)
    }

    #[inline]
    fn write_range(&self, dst: &mut impl ArrayWriter, range: Range<usize>) -> anyhow::Result<()> {
        self.item.write_range(dst, range)
    }

    #[inline]
    fn write_ranges(&self, dst: &mut impl ArrayWriter, ranges: &mut impl RangeList) -> anyhow::Result<()> {
        self.item.write_ranges(dst, ranges)
    }

    #[inline]
    fn write_indexes(
        &self, 
        dst: &mut impl ArrayWriter, 
        indexes: impl Iterator<Item=usize> + Clone
    ) -> anyhow::Result<()> 
    {
        self.item.write_indexes(dst, indexes)
    }
}


impl <'a> From<&'a dyn Array> for AnySlice<'a> {
    fn from(value: &'a dyn Array) -> Self {
        match value.data_type() {
            DataType::Boolean => AnySlice::Boolean(value.as_boolean().into()),
            DataType::Int8 => AnySlice::Int8(value.as_primitive::<Int8Type>().into()),
            DataType::Int16 => AnySlice::Int16(value.as_primitive::<Int16Type>().into()),
            DataType::Int32 => AnySlice::Int32(value.as_primitive::<Int32Type>().into()),
            DataType::Int64 => AnySlice::Int64(value.as_primitive::<Int64Type>().into()),
            DataType::UInt8 => AnySlice::UInt8(value.as_primitive::<UInt8Type>().into()),
            DataType::UInt16 => AnySlice::UInt16(value.as_primitive::<UInt16Type>().into()),
            DataType::UInt32 => AnySlice::UInt32(value.as_primitive::<UInt32Type>().into()),
            DataType::UInt64 => AnySlice::UInt64(value.as_primitive::<UInt64Type>().into()),
            DataType::Timestamp(TimeUnit::Second, _) => {
                AnySlice::Int64(value.as_primitive::<TimestampSecondType>().into())
            },
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                AnySlice::Int64(value.as_primitive::<TimestampMillisecondType>().into())
            },
            DataType::Binary => AnySlice::Binary(value.as_binary::<i32>().into()),
            DataType::FixedSizeBinary(_) => AnySlice::FixedSizeBinary(value.as_fixed_size_binary().into()),
            DataType::Utf8 => AnySlice::Binary(value.as_string::<i32>().into()),
            DataType::List(_) => AnySlice::List(value.as_list::<i32>().into()),
            DataType::FixedSizeList(_, _) => AnySlice::FixedSizeList(value.as_fixed_size_list().into()),
            DataType::Struct(_) => AnySlice::Struct(value.as_struct().into()),
            ty => panic!("unsupported arrow type - {}", ty)
        }
    }
}


impl<'a> AsSlice for &'a dyn Array {
    type Slice<'b> = AnySlice<'b> where Self: 'b;

    fn as_slice(&self) -> Self::Slice<'a> {
        AnySlice::from(*self)
    }
}


impl <'a> From<BooleanSlice<'a>> for AnySlice<'a> {
    fn from(value: BooleanSlice<'a>) -> Self {
        AnySlice::Boolean(value)
    }
}


impl <'a> From<ListSlice<'a, &'a [u8]>> for AnySlice<'a> {
    fn from(value: ListSlice<'a, &'a [u8]>) -> Self {
        AnySlice::Binary(value)
    }
}


impl <'a> From<FixedSizeListSlice<'a, &'a [u8]>> for AnySlice<'a> {
    fn from(value: FixedSizeListSlice<'a, &'a [u8]>) -> Self {
        AnySlice::FixedSizeBinary(value)
    }
}


impl <'a, T: Slice + Into<AnySlice<'a>>> From<ListSlice<'a, T>> for AnySlice<'a> {
    fn from(value: ListSlice<'a, T>) -> Self {
        let nulls = value.nulls();
        let offsets = value.offsets();
        let items = AnyListItem::new(
            value.values().into()
        );
        AnySlice::List(
            ListSlice::new(offsets, items, nulls.bitmask())
        )
    }
}


impl <'a, T: Slice + Into<AnySlice<'a>>> From<FixedSizeListSlice<'a, T>> for AnySlice<'a> {
    fn from(value: FixedSizeListSlice<'a, T>) -> Self {
        let size = value.size();
        let nulls = value.nulls();
        let items = AnyListItem::new(
            value.values().into()
        );
        AnySlice::FixedSizeList(
            FixedSizeListSlice::new(size, items, nulls.bitmask())
        )
    }
}


impl <'a> From<AnyStructSlice<'a>> for AnySlice<'a> {
    fn from(value: AnyStructSlice<'a>) -> Self {
        AnySlice::Struct(value)
    }
}


macro_rules! impl_from_prim {
    ($t:ty, $v:ident) => {
        impl <'a> From<PrimitiveSlice<'a, $t>> for AnySlice<'a> {
            fn from(value: PrimitiveSlice<'a, $t>) -> Self {
                AnySlice::$v(value)
            }
        }
    };
}
impl_from_prim!(u8, UInt8);
impl_from_prim!(u16, UInt16);
impl_from_prim!(u32, UInt32);
impl_from_prim!(u64, UInt64);
impl_from_prim!(i8, Int8);
impl_from_prim!(i16, Int16);
impl_from_prim!(i32, Int32);
impl_from_prim!(i64, Int64);
