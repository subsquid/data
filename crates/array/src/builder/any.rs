use crate::builder::memory_writer::MemoryWriter;
use crate::builder::r#struct::AnyStructBuilder;
use crate::builder::{ArrayBuilder, BinaryBuilder, BooleanBuilder, ListBuilder, PrimitiveBuilder, StringBuilder};
use crate::slice::{AnySlice, AsSlice};
use crate::writer::{ArrayWriter, Writer};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit, TimestampMillisecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type};


pub enum AnyBuilder {
    Boolean(BooleanBuilder),
    Int8(PrimitiveBuilder<Int8Type>),
    Int16(PrimitiveBuilder<Int16Type>),
    Int32(PrimitiveBuilder<Int32Type>),
    Int64(PrimitiveBuilder<Int64Type>),
    UInt8(PrimitiveBuilder<UInt8Type>),
    UInt16(PrimitiveBuilder<UInt16Type>),
    UInt32(PrimitiveBuilder<UInt32Type>),
    UInt64(PrimitiveBuilder<UInt64Type>),
    TimestampSecond(PrimitiveBuilder<TimestampSecondType>),
    TimestampMillisecond(PrimitiveBuilder<TimestampMillisecondType>),
    Binary(BinaryBuilder),
    String(StringBuilder),
    List(Box<ListBuilder<AnyBuilder>>),
    Struct(AnyStructBuilder)
}


impl ArrayWriter for AnyBuilder {
    type Writer = MemoryWriter;

    fn bitmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Bitmask {
        match self {
            AnyBuilder::Boolean(b) => b.bitmask(buf),
            AnyBuilder::Int8(b) => b.bitmask(buf),
            AnyBuilder::Int16(b) => b.bitmask(buf),
            AnyBuilder::Int32(b) => b.bitmask(buf),
            AnyBuilder::Int64(b) => b.bitmask(buf),
            AnyBuilder::UInt8(b) => b.bitmask(buf),
            AnyBuilder::UInt16(b) => b.bitmask(buf),
            AnyBuilder::UInt32(b) => b.bitmask(buf),
            AnyBuilder::UInt64(b) => b.bitmask(buf),
            AnyBuilder::TimestampSecond(b) => b.bitmask(buf),
            AnyBuilder::TimestampMillisecond(b) => b.bitmask(buf),
            AnyBuilder::Binary(b) => b.bitmask(buf),
            AnyBuilder::String(b) => b.bitmask(buf),
            AnyBuilder::List(b) => b.bitmask(buf),
            AnyBuilder::Struct(b) => b.bitmask(buf),
        }
    }

    fn nullmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Nullmask {
        match self {
            AnyBuilder::Boolean(b) => b.nullmask(buf),
            AnyBuilder::Int8(b) => b.nullmask(buf),
            AnyBuilder::Int16(b) => b.nullmask(buf),
            AnyBuilder::Int32(b) => b.nullmask(buf),
            AnyBuilder::Int64(b) => b.nullmask(buf),
            AnyBuilder::UInt8(b) => b.nullmask(buf),
            AnyBuilder::UInt16(b) => b.nullmask(buf),
            AnyBuilder::UInt32(b) => b.nullmask(buf),
            AnyBuilder::UInt64(b) => b.nullmask(buf),
            AnyBuilder::TimestampSecond(b) => b.nullmask(buf),
            AnyBuilder::TimestampMillisecond(b) => b.nullmask(buf),
            AnyBuilder::Binary(b) => b.nullmask(buf),
            AnyBuilder::String(b) => b.nullmask(buf),
            AnyBuilder::List(b) => b.nullmask(buf),
            AnyBuilder::Struct(b) => b.nullmask(buf),
        }
    }

    fn native(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Native {
        match self {
            AnyBuilder::Boolean(b) => b.native(buf),
            AnyBuilder::Int8(b) => b.native(buf),
            AnyBuilder::Int16(b) => b.native(buf),
            AnyBuilder::Int32(b) => b.native(buf),
            AnyBuilder::Int64(b) => b.native(buf),
            AnyBuilder::UInt8(b) => b.native(buf),
            AnyBuilder::UInt16(b) => b.native(buf),
            AnyBuilder::UInt32(b) => b.native(buf),
            AnyBuilder::UInt64(b) => b.native(buf),
            AnyBuilder::TimestampSecond(b) => b.native(buf),
            AnyBuilder::TimestampMillisecond(b) => b.native(buf),
            AnyBuilder::Binary(b) => b.native(buf),
            AnyBuilder::String(b) => b.native(buf),
            AnyBuilder::List(b) => b.native(buf),
            AnyBuilder::Struct(b) => b.native(buf),
        }
    }

    fn offset(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Offset {
        match self {
            AnyBuilder::Boolean(b) => b.offset(buf),
            AnyBuilder::Int8(b) => b.offset(buf),
            AnyBuilder::Int16(b) => b.offset(buf),
            AnyBuilder::Int32(b) => b.offset(buf),
            AnyBuilder::Int64(b) => b.offset(buf),
            AnyBuilder::UInt8(b) => b.offset(buf),
            AnyBuilder::UInt16(b) => b.offset(buf),
            AnyBuilder::UInt32(b) => b.offset(buf),
            AnyBuilder::UInt64(b) => b.offset(buf),
            AnyBuilder::TimestampSecond(b) => b.offset(buf),
            AnyBuilder::TimestampMillisecond(b) => b.offset(buf),
            AnyBuilder::Binary(b) => b.offset(buf),
            AnyBuilder::String(b) => b.offset(buf),
            AnyBuilder::List(b) => b.offset(buf),
            AnyBuilder::Struct(b) => b.offset(buf),
        }
    }
}


impl AsSlice for AnyBuilder {
    type Slice<'a> = AnySlice<'a>;

    fn as_slice(&self) -> Self::Slice<'_> {
        todo!()
    }
}


impl ArrayBuilder for AnyBuilder {
    fn len(&self) -> usize {
        match self {
            AnyBuilder::Boolean(b) => b.len(),
            AnyBuilder::Int8(b) => b.len(),
            AnyBuilder::Int16(b) => b.len(),
            AnyBuilder::Int32(b) => b.len(),
            AnyBuilder::Int64(b) => b.len(),
            AnyBuilder::UInt8(b) => b.len(),
            AnyBuilder::UInt16(b) => b.len(),
            AnyBuilder::UInt32(b) => b.len(),
            AnyBuilder::UInt64(b) => b.len(),
            AnyBuilder::TimestampSecond(b) => b.len(),
            AnyBuilder::TimestampMillisecond(b) => b.len(),
            AnyBuilder::Binary(b) => b.len(),
            AnyBuilder::String(b) => b.len(),
            AnyBuilder::List(b) => b.len(),
            AnyBuilder::Struct(b) => b.len(),
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            AnyBuilder::Boolean(b) => b.data_type(),
            AnyBuilder::Int8(b) => b.data_type(),
            AnyBuilder::Int16(b) => b.data_type(),
            AnyBuilder::Int32(b) => b.data_type(),
            AnyBuilder::Int64(b) => b.data_type(),
            AnyBuilder::UInt8(b) => b.data_type(),
            AnyBuilder::UInt16(b) => b.data_type(),
            AnyBuilder::UInt32(b) => b.data_type(),
            AnyBuilder::UInt64(b) => b.data_type(),
            AnyBuilder::TimestampSecond(b) => b.data_type(),
            AnyBuilder::TimestampMillisecond(b) => b.data_type(),
            AnyBuilder::Binary(b) => b.data_type(),
            AnyBuilder::String(b) => b.data_type(),
            AnyBuilder::List(b) => b.data_type(),
            AnyBuilder::Struct(b) => b.data_type(),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            AnyBuilder::Boolean(b) => ArrayBuilder::finish(b),
            AnyBuilder::Int8(b) => ArrayBuilder::finish(b),
            AnyBuilder::Int16(b) => ArrayBuilder::finish(b),
            AnyBuilder::Int32(b) => ArrayBuilder::finish(b),
            AnyBuilder::Int64(b) => ArrayBuilder::finish(b),
            AnyBuilder::UInt8(b) => ArrayBuilder::finish(b),
            AnyBuilder::UInt16(b) => ArrayBuilder::finish(b),
            AnyBuilder::UInt32(b) => ArrayBuilder::finish(b),
            AnyBuilder::UInt64(b) => ArrayBuilder::finish(b),
            AnyBuilder::TimestampSecond(b) => ArrayBuilder::finish(b),
            AnyBuilder::TimestampMillisecond(b) => ArrayBuilder::finish(b),
            AnyBuilder::Binary(b) => ArrayBuilder::finish(b),
            AnyBuilder::String(b) => ArrayBuilder::finish(b),
            AnyBuilder::List(b) => ArrayBuilder::finish(*b),
            AnyBuilder::Struct(b) => ArrayBuilder::finish(b),
        }
    }
}


impl From<BooleanBuilder> for AnyBuilder {
    fn from(value: BooleanBuilder) -> Self {
        AnyBuilder::Boolean(value)
    }
}


macro_rules! impl_from_primitive {
    ($kind:ident, $ty:ident) => {
        impl From<PrimitiveBuilder<$ty>> for AnyBuilder {
            fn from(value: PrimitiveBuilder<$ty>) -> Self {
                AnyBuilder::$kind(value)
            }
        }
    };
}
impl_from_primitive!(Int8, Int8Type);
impl_from_primitive!(Int16, Int16Type);
impl_from_primitive!(Int32, Int32Type);
impl_from_primitive!(Int64, Int64Type);
impl_from_primitive!(UInt8, UInt8Type);
impl_from_primitive!(UInt16, UInt16Type);
impl_from_primitive!(UInt32, UInt32Type);
impl_from_primitive!(UInt64, UInt64Type);
impl_from_primitive!(TimestampSecond, TimestampSecondType);
impl_from_primitive!(TimestampMillisecond, TimestampMillisecondType);


impl From<BinaryBuilder> for AnyBuilder {
    fn from(value: BinaryBuilder) -> Self {
        AnyBuilder::Binary(value)
    }
}


impl From<StringBuilder> for AnyBuilder {
    fn from(value: StringBuilder) -> Self {
        AnyBuilder::String(value)
    }
}


impl From<ListBuilder<AnyBuilder>> for AnyBuilder {
    fn from(value: ListBuilder<AnyBuilder>) -> Self {
        AnyBuilder::List(Box::new(value))
    }
}


impl From<AnyStructBuilder> for AnyBuilder {
    fn from(value: AnyStructBuilder) -> Self {
        AnyBuilder::Struct(value)
    }
}


impl AnyBuilder {
    pub fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean => BooleanBuilder::new(0).into(),
            DataType::Int8 => PrimitiveBuilder::<Int8Type>::new(0).into(),
            DataType::Int16 => PrimitiveBuilder::<Int16Type>::new(0).into(),
            DataType::Int32 => PrimitiveBuilder::<Int32Type>::new(0).into(),
            DataType::Int64 => PrimitiveBuilder::<Int64Type>::new(0).into(),
            DataType::UInt8 => PrimitiveBuilder::<UInt8Type>::new(0).into(),
            DataType::UInt16 => PrimitiveBuilder::<UInt16Type>::new(0).into(),
            DataType::UInt32 => PrimitiveBuilder::<UInt32Type>::new(0).into(),
            DataType::UInt64 => PrimitiveBuilder::<UInt64Type>::new(0).into(),
            DataType::Timestamp(TimeUnit::Second, _) => {
                PrimitiveBuilder::<TimestampSecondType>::new(0).into()
            },
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                PrimitiveBuilder::<TimestampMillisecondType>::new(0).into()
            },
            DataType::Binary => BinaryBuilder::new(0, 0).into(),
            DataType::Utf8 => StringBuilder::new(0, 0).into(),
            DataType::List(f) => {
                ListBuilder::new(0, Self::new(f.data_type()))
                    .with_field_name(f.name().to_string())
                    .into()
            },
            DataType::Struct(fields) => AnyStructBuilder::new(fields.iter().cloned().collect()).into(),
            ty => panic!("unsupported arrow type - {}", ty)
        }
    }
}