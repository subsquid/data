use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use crate::builder::memory_writer::MemoryWriter;
use crate::slice::AsSlice;
use crate::writer::ArrayWriter;


mod binary;
mod bitmask;
mod boolean;
mod list;
mod memory_writer;
mod native;
mod nullmask;
mod offsets;
mod primitive;
mod any;
mod r#struct;


pub use any::*;
pub use binary::*;
pub use boolean::*;
pub use list::*;
pub use primitive::*;


pub trait ArrayBuilder: ArrayWriter<Writer=MemoryWriter> + AsSlice + 'static {
    fn len(&self) -> usize;

    fn data_type(&self) -> DataType;

    fn finish(self) -> ArrayRef;
}