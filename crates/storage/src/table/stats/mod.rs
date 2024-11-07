use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, TimeUnit};
use arrow_buffer::OffsetBuffer;


mod builder;


pub use builder::*;


#[derive(Clone)]
pub struct Stats {
    pub offsets: OffsetBuffer<u32>,
    pub min: ArrayRef,
    pub max: ArrayRef
}


pub fn can_have_stats(data_type: &DataType) -> bool {
    match data_type {
        DataType::Int8 => true,
        DataType::Int16 => true,
        DataType::Int32 => true,
        DataType::Int64 => true,
        DataType::UInt8 => true,
        DataType::UInt16 => true,
        DataType::UInt32 => true,
        DataType::UInt64 => true,
        DataType::Timestamp(TimeUnit::Second, _) => true,
        DataType::Timestamp(TimeUnit::Millisecond, _) => true,
        DataType::Binary => true,
        DataType::Utf8 => true,
        _ => false
    }
}