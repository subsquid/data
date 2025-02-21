use arrow::array::ArrowPrimitiveType;
use arrow::datatypes::{DataType, FieldRef, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type};
use std::sync::Arc;


pub trait DataTypeVisitor {
    type Result;

    fn boolean(&mut self) -> Self::Result;

    fn primitive<T: ArrowPrimitiveType>(&mut self) -> Self::Result;

    fn timestamp(&mut self, time_unit: &TimeUnit, _tz: &Option<Arc<str>>) -> Self::Result {
        match time_unit {
            TimeUnit::Second => self.primitive::<TimestampSecondType>(),
            TimeUnit::Millisecond => self.primitive::<TimestampMillisecondType>(),
            TimeUnit::Microsecond => self.primitive::<TimestampMicrosecondType>(),
            TimeUnit::Nanosecond => self.primitive::<TimestampNanosecondType>()
        }
    }
    
    fn binary(&mut self) -> Self::Result;

    fn fixed_size_binary(&mut self, size: usize) -> Self::Result;

    #[inline]
    fn string(&mut self) -> Self::Result {
        self.binary()
    }
    
    fn list(&mut self, item: &DataType) -> Self::Result;
    
    fn r#struct(&mut self, fields: &[FieldRef]) -> Self::Result;
    
    fn visit(&mut self, data_type: &DataType) -> Self::Result {
        match data_type {
            DataType::Boolean => self.boolean(),
            DataType::Int8 => self.primitive::<Int8Type>(),
            DataType::Int16 => self.primitive::<Int16Type>(),
            DataType::Int32 => self.primitive::<Int32Type>(),
            DataType::Int64 => self.primitive::<Int64Type>(),
            DataType::UInt8 => self.primitive::<UInt8Type>(),
            DataType::UInt16 => self.primitive::<UInt16Type>(),
            DataType::UInt32 => self.primitive::<UInt32Type>(),
            DataType::UInt64 => self.primitive::<UInt64Type>(),
            DataType::Float16 => self.primitive::<Float16Type>(),
            DataType::Float32 => self.primitive::<Float32Type>(),
            DataType::Float64 => self.primitive::<Float64Type>(),
            DataType::Timestamp(time_unit, tz) => self.timestamp(time_unit, tz),
            DataType::Binary => self.binary(),
            DataType::FixedSizeBinary(size) => self.fixed_size_binary(*size as usize),
            DataType::Utf8 => self.string(),
            DataType::List(f) => self.list(f.data_type()),
            DataType::Struct(fields) => self.r#struct(fields),
            ty => panic!("unsupported arrow type - {}", ty)
        }
    }
}