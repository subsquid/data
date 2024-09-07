use crate::table::write::array::boolean::BooleanBuilder;
use crate::table::write::array::list::{BinaryBuilder, ListBuilder};
use crate::table::write::array::primitive::PrimitiveBuilder;
use crate::table::write::array::r#struct::StructBuilder;
use crate::table::write::array::AnyBuilder;
use arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit, TimestampMillisecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type};


pub fn make_any_builder(page_size: usize, data_type: &DataType) -> AnyBuilder {
    match data_type {
        DataType::Boolean => Box::new(BooleanBuilder::new(page_size)),
        DataType::Int8 => Box::new(PrimitiveBuilder::<Int8Type>::new(page_size)),
        DataType::Int16 => Box::new(PrimitiveBuilder::<Int16Type>::new(page_size)),
        DataType::Int32 => Box::new(PrimitiveBuilder::<Int32Type>::new(page_size)),
        DataType::Int64 => Box::new(PrimitiveBuilder::<Int64Type>::new(page_size)),
        DataType::UInt8 => Box::new(PrimitiveBuilder::<UInt8Type>::new(page_size)),
        DataType::UInt16 => Box::new(PrimitiveBuilder::<UInt16Type>::new(page_size)),
        DataType::UInt32 => Box::new(PrimitiveBuilder::<UInt32Type>::new(page_size)),
        DataType::UInt64 => Box::new(PrimitiveBuilder::<UInt64Type>::new(page_size)),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Box::new(PrimitiveBuilder::<TimestampMillisecondType>::new(page_size))
        },
        DataType::Timestamp(TimeUnit::Second, _) => {
            Box::new(PrimitiveBuilder::<TimestampSecondType>::new(page_size))
        },
        DataType::Binary | DataType::Utf8 => Box::new(BinaryBuilder::new(page_size)),
        DataType::List(f) => {
            Box::new(ListBuilder::new(page_size, make_any_builder(page_size, f.data_type())))
        },
        DataType::Struct(fields) => {
            Box::new(StructBuilder::new(
                page_size, 
                fields.iter().map(|f| {
                    make_any_builder(page_size, f.data_type())
                }).collect()
            ))
        },
        ty => panic!("unsupported arrow data type - {}", ty)
    }
}