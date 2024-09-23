use crate::array_builder::buffer_writer::BufferWriter;
use crate::writer::ArrayWriter;
use arrow::datatypes::DataType;


mod bitmask;
mod nullmask;
mod native;
mod offsets;
mod boolean;
mod buffer_writer;
mod primitive;
mod list;


pub trait ArrayBuilder: ArrayWriter<Writer = BufferWriter> {
    fn len(&self) -> usize;
    
    fn data_type(&self) -> DataType;
}