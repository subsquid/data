use crate::table::write::array::bitmask::{flush_all_null_mask, flush_null_mask, set_nulls_index, BitmaskBuilder};
use crate::table::write::array::{Builder, FlushCallback};
use arrow::array::Array;


pub struct BooleanBuilder {
    nulls: Option<BitmaskBuilder>,
    values: BitmaskBuilder
}


impl Builder for BooleanBuilder {
    fn get_index(&self) -> usize {
        self.values.get_index() - 1
    }

    fn set_index(&mut self, index: usize) {
        set_nulls_index!(self, index);
        self.values.set_index(index + 1);
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