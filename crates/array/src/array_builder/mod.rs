use crate::array_builder::writer::BufferWriter;
use crate::data_builder::DataBuilder;
use arrow::datatypes::DataType;


mod bitmask;
mod nullmask;
mod native;
mod offsets;
mod boolean;
mod writer;
mod primitive;
mod list;


pub trait ArrayBuilder: DataBuilder<Writer = BufferWriter> {
    fn len(&self) -> usize;
    
    fn data_type(&self) -> DataType;
}