use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use crate::builder::memory_writer::MemoryWriter;
use crate::slice::AsSlice;
use crate::writer::ArrayWriter;


mod binary;
pub mod bitmask;
mod boolean;
mod list;
mod memory_writer;
pub mod native;
pub mod nullmask;
pub mod offsets;
mod primitive;
mod any;
mod r#struct;


pub use any::*;
pub use binary::*;
pub use boolean::*;
pub use list::*;
pub use primitive::*;
pub use r#struct::*;


pub trait ArrayBuilder: ArrayWriter<Writer=MemoryWriter> + AsSlice + 'static {
    fn len(&self) -> usize;

    fn data_type(&self) -> DataType;

    fn finish(self) -> ArrayRef;
    
    unsafe fn finish_unchecked(self) -> ArrayRef {
        self.finish()
    }
}