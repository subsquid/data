use crate::table::write::array::bitmask::{flush_all_null_mask, flush_null_mask, set_nulls_index, BitmaskBuilder};
use crate::table::write::array::{AnyBuilder, Builder, FlushCallback};
use arrow::array::Array;


pub struct StructBuilder {
    nulls: Option<BitmaskBuilder>,
    columns: Vec<AnyBuilder>
}


impl Builder for StructBuilder {
    fn get_index(&self) -> usize {
        self.columns[0].get_index() - 1
    }

    fn set_index(&mut self, index: usize) {
        set_nulls_index!(self, index);
        let mut i = index + 1;
        for col in self.columns.iter_mut() {
            col.set_index(i);
            i += col.num_buffers();
        }
    }

    fn num_buffers(&self) -> usize {
        1 + self.columns.iter().map(|c| c.num_buffers()).sum::<usize>()
    }

    fn flush(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        flush_null_mask!(self, cb);
        for col in self.columns.iter_mut() {
            col.flush(cb)?;
        }
        Ok(())
    }

    fn flush_all(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        flush_all_null_mask!(self, cb);
        for col in self.columns.iter_mut() {
            col.flush_all(cb)?;
        }
        Ok(())
    }

    fn push_array(&mut self, array: &dyn Array) {
        todo!()
    }
}