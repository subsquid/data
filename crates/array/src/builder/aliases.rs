use crate::builder::PrimitiveBuilder;
use arrow::datatypes::{Decimal128Type, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampMillisecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type};


/// A signed 8-bit integer array builder.
pub type Int8Builder = PrimitiveBuilder<Int8Type>;
/// A signed 16-bit integer array builder.
pub type Int16Builder = PrimitiveBuilder<Int16Type>;
/// A signed 32-bit integer array builder.
pub type Int32Builder = PrimitiveBuilder<Int32Type>;
/// A signed 64-bit integer array builder.
pub type Int64Builder = PrimitiveBuilder<Int64Type>;
/// An unsigned 8-bit integer array builder.
pub type UInt8Builder = PrimitiveBuilder<UInt8Type>;
/// An unsigned 16-bit integer array builder.
pub type UInt16Builder = PrimitiveBuilder<UInt16Type>;
/// An unsigned 32-bit integer array builder.
pub type UInt32Builder = PrimitiveBuilder<UInt32Type>;
/// An unsigned 64-bit integer array builder.
pub type UInt64Builder = PrimitiveBuilder<UInt64Type>;
/// A 16-bit floating point array builder.
pub type Float16Builder = PrimitiveBuilder<Float16Type>;
/// A 32-bit floating point array builder.
pub type Float32Builder = PrimitiveBuilder<Float32Type>;
/// A 64-bit floating point array builder.
pub type Float64Builder = PrimitiveBuilder<Float64Type>;
/// A 128-bit decimal array builder.
pub type Decimal128Builder = PrimitiveBuilder<Decimal128Type>;

/// A timestamp second array builder.
pub type TimestampSecondBuilder = PrimitiveBuilder<TimestampSecondType>;
/// A timestamp millisecond array builder.
pub type TimestampMillisecondBuilder = PrimitiveBuilder<TimestampMillisecondType>;