use crate::table::write::array::bitmask::NullMaskBuilder;
use crate::table::write::array::{AnyBuilder, Builder, FlushCallback};
use arrow::array::{Array, AsArray};


pub struct StructBuilder {
    nulls: NullMaskBuilder,
    columns: Vec<AnyBuilder>
}


impl Builder for StructBuilder {
    fn num_buffers(&self) -> usize {
        1 + self.columns.iter().map(|c| c.num_buffers()).sum::<usize>()
    }

    fn get_index(&self) -> usize {
        self.nulls.get_index()
    }

    fn set_index(&mut self, index: usize) {
        self.nulls.set_index(index);
        let mut i = index + 1;
        for col in self.columns.iter_mut() {
            col.set_index(i);
            i += col.num_buffers();
        }
    }

    fn flush(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        self.nulls.flush(cb)?;
        for col in self.columns.iter_mut() {
            col.flush(cb)?;
        }
        Ok(())
    }

    fn flush_all(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        self.nulls.flush_all(cb)?;
        for col in self.columns.iter_mut() {
            col.flush_all(cb)?;
        }
        Ok(())
    }

    fn push_array(&mut self, array: &dyn Array) {
        let array = array.as_struct();
        assert_eq!(array.num_columns(), self.columns.len());
        self.nulls.push(array.len(), array.nulls());
        for (b, col) in self.columns.iter_mut().zip(array.columns()) {
            b.push_array(col)
        }
    }

    fn total_len(&self) -> usize {
        self.nulls.total_len()
    }
}