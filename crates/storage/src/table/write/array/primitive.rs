use crate::table::write::array::bitmask::NullMaskBuilder;
use crate::table::write::array::{Builder, FlushCallback};
use arrow::array::{Array, ArrowPrimitiveType, AsArray};
use arrow_buffer::{bit_util, ArrowNativeType, MutableBuffer};
use std::marker::PhantomData;


pub struct NativeBuilder<T> {
    buffer: MutableBuffer,
    page_size: usize,
    index: usize,
    total_len: usize,
    phantom_data: PhantomData<T>
}


impl <T: ArrowNativeType> NativeBuilder<T> {
    pub fn new(page_size: usize) -> Self {
        assert!(page_size > 0);
        Self {
            buffer: MutableBuffer::new(page_size * 2),
            page_size,
            index: 0,
            total_len: 0,
            phantom_data: PhantomData::default()
        }
    }

    pub fn push_slice(&mut self, values: &[T]) {
        self.buffer.extend_from_slice(values);
        self.total_len += values.len()
    }

    pub fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        let beg = self.buffer.len();
        self.buffer.extend(iter);
        let end = self.buffer.len();
        self.total_len += (end - beg) / T::get_byte_width();
    }

    fn take(&self, offset: &mut usize, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        while self.buffer.len() - *offset > self.page_size * 3 / 2 {
            let end = *offset + bit_util::ceil(self.page_size, T::get_byte_width()) * T::get_byte_width();
            if end > self.buffer.len() {
                break
            }
            let data = &self.buffer[*offset..end];
            let len = data.len() / T::get_byte_width();
            cb(self.index, len, data)?;
            *offset = end;
        }
        Ok(())
    }

    fn shift(&mut self, offset: usize) {
        let bytes = self.buffer.as_slice_mut();
        bytes.copy_within(offset.., 0);
        let new_len = bytes.len() - offset;
        self.buffer.truncate(new_len);
    }
}


impl <T: ArrowNativeType> Builder for NativeBuilder<T> {
    fn num_buffers(&self) -> usize {
        1
    }

    fn get_index(&self) -> usize {
        self.index
    }

    fn set_index(&mut self, index: usize) {
        self.index = index
    }

    fn flush(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        let mut offset = 0;
        let result = self.take(&mut offset, cb);
        if offset > 0 {
            self.shift(offset);
        }
        result
    }

    fn flush_all(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        let mut offset = 0;

        let result = self.take(&mut offset, cb).and_then(|_| {
            let data = &self.buffer[offset..];
            assert_eq!(data.len() % T::get_byte_width(), 0);
            let len = data.len() / T::get_byte_width();
            cb(self.index, len, data)?;
            offset = self.buffer.len();
            Ok(())
        });

        if offset > 0 {
            if offset == self.buffer.len() {
                self.buffer.truncate(0);
            } else {
                self.shift(offset);
            }
        }

        result
    }

    fn push_array(&mut self, _array: &dyn Array) {
        unimplemented!()
    }

    fn total_len(&self) -> usize {
        self.total_len
    }
}


pub struct PrimitiveBuilder<T: ArrowPrimitiveType> {
    nulls: NullMaskBuilder,
    values: NativeBuilder<T::Native>
}


impl <T: ArrowPrimitiveType> PrimitiveBuilder<T> {
    pub fn new(page_size: usize) -> Self {
        let mut builder = Self {
            nulls: NullMaskBuilder::new(page_size),
            values: NativeBuilder::new(page_size)
        };
        builder.set_index(0);
        builder
    }
}


impl <T: ArrowPrimitiveType> Builder for PrimitiveBuilder<T> {
    fn num_buffers(&self) -> usize {
        2
    }

    fn get_index(&self) -> usize {
        self.nulls.get_index()
    }

    fn set_index(&mut self, index: usize) {
        self.nulls.set_index(index);
        self.values.set_index(index + 1)
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
        let array = array.as_primitive::<T>();
        self.nulls.push(array.len(), array.nulls());
        self.values.push_slice(array.values())
    }

    fn total_len(&self) -> usize {
        self.nulls.total_len()
    }
}