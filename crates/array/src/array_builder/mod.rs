use arrow::array::ArrayRef;
use crate::array_builder::memory_writer::MemoryWriter;
use crate::writer::ArrayWriter;
use arrow::datatypes::DataType;


mod bitmask;
mod nullmask;
mod native;
mod offsets;
mod boolean;
mod memory_writer;
mod primitive;
mod list;


pub trait ArrayBuilder: ArrayWriter<Writer=MemoryWriter> {
    fn len(&self) -> usize;
    
    fn data_type(&self) -> DataType;

    fn finish(self) -> ArrayRef;
}