use crate::table::write::array::bitmask::{flush_all_null_mask, flush_null_mask, set_nulls_index, BitmaskBuilder};
use crate::table::write::array::primitive::NativeBuilder;
use crate::table::write::array::{Builder, FlushCallback};
use arrow::array::Array;


pub struct ListBuilder<T> {
    nulls: Option<BitmaskBuilder>,
    offsets: NativeBuilder<i32>,
    values: T
}


impl <T: Builder> Builder for ListBuilder<T> {
    fn get_index(&self) -> usize {
        self.offsets.get_index() - 1
    }

    fn set_index(&mut self, index: usize) {
        set_nulls_index!(self, index);
        self.offsets.set_index(index + 1);
        self.values.set_index(index + 2)
    }

    fn num_buffers(&self) -> usize {
        2 + self.values.num_buffers()
    }

    fn flush(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        flush_null_mask!(self, cb);
        self.offsets.flush(cb)?;
        self.values.flush(cb)
    }

    fn flush_all(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        flush_all_null_mask!(self, cb);
        self.offsets.flush_all(cb)?;
        self.values.flush_all(cb)
    }

    fn push_array(&mut self, array: &dyn Array) {
        todo!()
    }
}