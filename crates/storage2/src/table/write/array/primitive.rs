use std::marker::PhantomData;
use arrow::array::Array;
use arrow_buffer::{ArrowNativeType, MutableBuffer};
use crate::table::write::array::bitmask::{flush_all_null_mask, flush_null_mask, set_nulls_index, BitmaskBuilder};
use crate::table::write::array::{Builder, FlushCallback};


pub struct NativeBuilder<T> {
    values: MutableBuffer,
    page_size: usize,
    index: usize,
    phantom_data: PhantomData<T>
}


impl <T: ArrowNativeType> Builder for NativeBuilder<T> {
    fn get_index(&self) -> usize {
        self.index
    }

    fn set_index(&mut self, index: usize) {
        self.index = index
    }

    fn num_buffers(&self) -> usize {
        1
    }

    fn flush(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        todo!()
    }

    fn flush_all(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        todo!()
    }

    fn push_array(&mut self, array: &dyn Array) {
        todo!()
    }
}


pub struct PrimitiveBuilder<T> {
    nulls: Option<BitmaskBuilder>,
    values: NativeBuilder<T>
}


impl <T: ArrowNativeType> Builder for PrimitiveBuilder<T> {
    fn get_index(&self) -> usize {
        self.values.get_index() - 1
    }

    fn set_index(&mut self, index: usize) {
        set_nulls_index!(self, index);
        self.values.set_index(index + 1)
    }

    fn num_buffers(&self) -> usize {
        2
    }

    fn flush(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        flush_null_mask!(self, cb);
        self.values.flush(cb)
    }

    fn flush_all(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        flush_all_null_mask!(self, cb);
        self.values.flush_all(cb)
    }

    fn push_array(&mut self, array: &dyn Array) {
        todo!()
    }
}