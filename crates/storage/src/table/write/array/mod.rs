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


impl Builder for AnyBuilder {
    fn num_buffers(&self) -> usize {
        self.as_ref().num_buffers()
    }

    fn get_index(&self) -> usize {
        self.as_ref().get_index()
    }

    fn set_index(&mut self, index: usize) {
        self.as_mut().set_index(index)
    }

    fn flush(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        self.as_mut().flush(cb)
    }

    fn flush_all(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        self.as_mut().flush_all(cb)
    }

    fn push_array(&mut self, array: &dyn Array) {
        self.as_mut().push_array(array)
    }

    fn total_len(&self) -> usize {
        self.as_ref().total_len()
    }
}