use crate::table::write::array::{Builder, FlushCallback};
use arrow::array::Array;
use arrow_buffer::BooleanBufferBuilder;


pub struct BitmaskBuilder {
    inner: BooleanBufferBuilder,
    page_size: usize,
    index: usize
}


impl Builder for BitmaskBuilder {
    fn get_index(&self) -> usize {
        todo!()
    }

    fn set_index(&mut self, index: usize) {
        todo!()
    }

    fn num_buffers(&self) -> usize {
        1
    }

    fn flush(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        if self.inner.len() >= self.page_size * 2 * 8 {
            todo!()
        }
        Ok(())
    }

    fn flush_all(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        todo!()
    }

    fn push_array(&mut self, array: &dyn Array) {
        todo!()
    }
}


macro_rules! flush_null_mask {
    ($s:ident, $cb:ident) => {
        if let Some(nulls) = $s.nulls.as_mut() {
            nulls.flush($cb)?;
        }
    };
}
pub(super) use flush_null_mask;


macro_rules! flush_all_null_mask {
    ($s:ident, $cb:ident) => {
        if let Some(nulls) = $s.nulls.as_mut() {
            nulls.flush_all($cb)?;
        } else {
            $cb($s.get_index(), 0, &[])?;
        }
    };
}
pub(super) use flush_all_null_mask;


macro_rules! set_nulls_index {
    ($s:ident, $index:ident) => {
        if let Some(nulls) = $s.nulls.as_mut() {
            nulls.set_index($index);
        }
    };
}
pub(super) use set_nulls_index;