use arrow::datatypes::DataType;

use crate::{AnyBuilder, AnyStructBuilder, BooleanBuilder, DataBuilder, ListBuilder, NativeBuilder, PrimitiveBuilder};


pub fn make_data_builder(data_type: &DataType) -> Box<dyn DataBuilder> {
    match data_type {
        DataType::Boolean => Box::new(BooleanBuilder::default()),
        DataType::Int8 => Box::new(PrimitiveBuilder::<i8>::default()),
        DataType::Int16 => Box::new(PrimitiveBuilder::<i16>::default()),
        DataType::Int32 => Box::new(PrimitiveBuilder::<i32>::default()),
        DataType::Int64 => Box::new(PrimitiveBuilder::<i64>::default()),
        DataType::UInt8 => Box::new(PrimitiveBuilder::<u8>::default()),
        DataType::UInt16 => Box::new(PrimitiveBuilder::<u16>::default()),
        DataType::UInt32 => Box::new(PrimitiveBuilder::<u32>::default()),
        DataType::UInt64 => Box::new(PrimitiveBuilder::<u64>::default()),
        DataType::Timestamp(_, _) => Box::new(PrimitiveBuilder::<i64>::default()),
        DataType::Binary => Box::new(
            ListBuilder::new(0, NativeBuilder::<u8>::default())
        ),
        DataType::Utf8 => Box::new(
            ListBuilder::new(0, NativeBuilder::<u8>::default())
        ),
        DataType::List(f) if f.data_type() == &DataType::UInt32 => Box::new(
            ListBuilder::new(0, PrimitiveBuilder::<u32>::default())
        ),
        DataType::List(f) if f.data_type() == &DataType::UInt16 => Box::new(
            ListBuilder::new(0, PrimitiveBuilder::<u16>::default())
        ),
        DataType::List(f) => Box::new(
            ListBuilder::new(0, AnyBuilder::for_data_type(f.data_type()))
        ),
        DataType::Struct(fields) => Box::new(
            AnyStructBuilder::new(
                fields.iter().map(|f| AnyBuilder::for_data_type(f.data_type())).collect()
            )
        ),
        ty => panic!("unsupported arrow data type - {}",  ty)
    }
}