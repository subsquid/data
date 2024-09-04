use arrow::array::Array;


mod bitmask;
mod boolean;
mod primitive;
mod list;
mod r#struct;
mod any;


pub use any::*;


pub type FlushCallback<'a> = &'a mut dyn FnMut(usize, usize, &[u8]) -> anyhow::Result<()>;


pub trait Builder {
    fn num_buffers(&self) -> usize;
    
    fn get_index(&self) -> usize;

    fn set_index(&mut self, index: usize);

    fn flush(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()>;

    fn flush_all(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()>;

    fn push_array(&mut self, array: &dyn Array);
    
    fn total_len(&self) -> usize;
}


pub type AnyBuilder = Box<dyn Builder>;