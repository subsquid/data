use crate::table::write::array::bitmask::NullMaskBuilder;
use crate::table::write::array::primitive::NativeBuilder;
use crate::table::write::array::{Builder, FlushCallback};
use arrow::array::{Array, AsArray};


pub struct ListBuilder<T> {
    nulls: NullMaskBuilder,
    offsets: NativeBuilder<i32>,
    values: T,
    last_offset: i32
}


impl <T: Builder> Builder for ListBuilder<T> {
    fn num_buffers(&self) -> usize {
        2 + self.values.num_buffers()
    }

    fn get_index(&self) -> usize {
        self.nulls.get_index()
    }

    fn set_index(&mut self, index: usize) {
        self.nulls.set_index(index);
        self.offsets.set_index(index + 1);
        self.values.set_index(index + 2)
    }

    fn flush(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        self.nulls.flush(cb)?;
        self.offsets.flush(cb)?;
        self.values.flush(cb)
    }

    fn flush_all(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        self.nulls.flush_all(cb)?;
        self.offsets.flush_all(cb)?;
        self.values.flush_all(cb)
    }

    fn push_array(&mut self, array: &dyn Array) {
        let array = array.as_list::<i32>();

        self.nulls.push(array.len(), array.nulls());
        
        let offsets = array.offsets();
        let beg = offsets[0];
        self.offsets.extend(offsets[1..].iter().map(|o| {
            *o - beg + self.last_offset
        }));
        self.last_offset += offsets.last().unwrap().clone() - beg;
        
        self.values.push_array(array.values());
    }

    fn total_len(&self) -> usize {
        self.nulls.total_len()
    }
}