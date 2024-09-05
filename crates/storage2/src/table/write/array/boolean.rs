use crate::table::write::array::bitmask::{BitmaskBuilder, NullMaskBuilder};
use crate::table::write::array::{Builder, FlushCallback};
use arrow::array::{Array, AsArray};


pub struct BooleanBuilder {
    nulls: NullMaskBuilder,
    values: BitmaskBuilder
}


impl BooleanBuilder {
    pub fn new(page_size: usize) -> Self {
        let mut builder = Self {
            nulls: NullMaskBuilder::new(page_size),
            values: BitmaskBuilder::new(page_size)
        };
        builder.set_index(0);
        builder
    }
}


impl Builder for BooleanBuilder {
    fn num_buffers(&self) -> usize {
        2
    }

    fn get_index(&self) -> usize {
        self.nulls.get_index()
    }

    fn set_index(&mut self, index: usize) {
        self.nulls.set_index(index);
        self.values.set_index(index + 1);
    }

    fn flush(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        self.nulls.flush(cb)?;
        self.values.flush(cb)
    }

    fn flush_all(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        self.nulls.flush_all(cb)?;
        self.values.flush_all(cb)
    }

    fn push_array(&mut self, array: &dyn Array) {
        let array = array.as_boolean();
        self.nulls.push(array.len(), array.nulls());
        self.values.push_boolean_buffer(array.values())
    }

    fn total_len(&self) -> usize {
        self.values.total_len()
    }
}