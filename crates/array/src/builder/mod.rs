use arrow::array::ArrayRef;
use arrow::datatypes::DataType;


mod aliases;
mod binary;
pub mod bitmask;
mod boolean;
mod fixed_size_binary;
mod list;
mod memory_writer;
pub mod native;
pub mod nullmask;
pub mod offsets;
mod primitive;
mod any;
mod r#struct;
mod table;


pub use aliases::*;
pub use any::*;
pub use binary::*;
pub use boolean::*;
pub use fixed_size_binary::*;
pub use list::*;
pub use primitive::*;
pub use r#struct::*;
pub use table::*;


pub trait ArrayBuilder: Sized {
    fn data_type(&self) -> DataType;
    
    fn len(&self) -> usize;
    
    fn byte_size(&self) -> usize;
    
    fn clear(&mut self);

    fn finish(self) -> ArrayRef;
    
    unsafe fn finish_unchecked(self) -> ArrayRef {
        self.finish()
    }
}