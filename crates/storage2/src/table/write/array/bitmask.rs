use crate::table::write::array::{Builder, FlushCallback};
use arrow::array::{Array, AsArray};
use arrow_buffer::{bit_mask, bit_util, BooleanBuffer, MutableBuffer, NullBuffer};


pub struct BitmaskBuilder {
    buffer: MutableBuffer,
    len: usize,
    total_len: usize,
    page_size: usize,
    index: usize
}


impl Builder for BitmaskBuilder {
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
            self.shift(offset)
        }
        result
    }

    fn flush_all(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        let mut offset = 0;

        let result = self.take(&mut offset, cb).and_then(|_| {
            let data = &self.buffer[offset..];
            let bit_len = self.len - offset * 8;
            cb(self.index, bit_len, data)?;
            offset = self.buffer.len();
            Ok(())
        });

        if offset > 0 {
            if offset == self.buffer.len() {
                self.buffer.truncate(0);
                self.len = 0;
            } else {
                self.shift(offset);
            }
        }

        result
    }

    fn push_array(&mut self, array: &dyn Array) {
        let values = array.as_boolean().values();
        self.push_boolean_buffer(values)
    }

    fn total_len(&self) -> usize {
        self.total_len
    }
}


impl BitmaskBuilder {
    fn take(&self, offset: &mut usize, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        assert!(self.page_size > 0);
        while self.buffer.len() - *offset > self.page_size * 3 / 2 {
            let end = *offset + self.page_size;
            if end * 8 > self.len { // unthinkable case of page_size = 1
                break
            }
            let data = &self.buffer[*offset..end];
            cb(self.index, self.page_size * 8, data)?;
            *offset = end;
        }
        Ok(())
    }

    fn shift(&mut self, offset: usize) {
        let bytes = self.buffer.as_slice_mut();
        let count = bytes.len() - offset;
        let src = bytes[offset..].as_ptr();
        let dst = bytes.as_mut_ptr();
        unsafe {
            std::ptr::copy(src, dst, count);
        }
        self.buffer.truncate(count);
        self.len = self.len - offset * 8;
    }

    pub fn push_boolean_buffer(&mut self, values: &BooleanBuffer) {
        let new_byte_len = bit_util::ceil(self.len + values.len(), 8);
        if new_byte_len > self.buffer.len() {
            self.buffer.resize(new_byte_len, 0)
        }

        bit_mask::set_bits(
            self.buffer.as_slice_mut(),
            values.values(),
            self.len,
            values.offset(),
            values.len()
        );

        self.len += values.len();
        self.total_len += values.len();
    }

    pub fn push_value(&mut self, val: bool, count: usize) {
        let new_len = self.len + count;
        let new_len_bytes = bit_util::ceil(new_len, 8);
        match val {
            true => {
                let cur_remainder = self.len % 8;
                let new_remainder = new_len % 8;

                if cur_remainder != 0 {
                    // Pad last byte with 1s
                    *self.buffer.as_slice_mut().last_mut().unwrap() |= !((1 << cur_remainder) - 1)
                }
                self.buffer.resize(new_len_bytes, 0xFF);
                if new_remainder != 0 {
                    // Clear remaining bits
                    *self.buffer.as_slice_mut().last_mut().unwrap() &= (1 << new_remainder) - 1
                }
            },
            false => {
                if new_len_bytes > self.buffer.len() {
                    self.buffer.resize(new_len_bytes, 0);
                }
            }
        }
        self.len = new_len;
        self.total_len += count;
    }

    pub fn new(page_size: usize) -> Self {
        assert!(page_size > 0);
        Self {
            buffer: MutableBuffer::new(page_size * 2),
            len: 0,
            total_len: 0,
            page_size,
            index: 0
        }
    }
}


pub struct NullMaskBuilder {
    nulls: Option<BitmaskBuilder>,
    total_len: usize,
    page_size: usize,
    index: usize
}


impl NullMaskBuilder {
    pub fn new(page_size: usize) -> Self {
        assert!(page_size > 0);
        Self {
            nulls: None,
            total_len: 0,
            page_size,
            index: 0
        }
    }

    pub fn get_index(&self) -> usize {
        self.index
    }

    pub fn set_index(&mut self, index: usize) {
        self.index = index;
        if let Some(nulls) = self.nulls.as_mut() {
            nulls.set_index(index)
        }
    }

    pub fn flush(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        if let Some(mask) = self.nulls.as_mut() {
            mask.flush(cb)
        } else {
            Ok(())
        }
    }

    pub fn flush_all(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        if let Some(mask) = self.nulls.as_mut() {
            mask.flush_all(cb)
        } else {
            cb(self.index, 0, &[])
        }
    }

    pub fn push(&mut self, len: usize, nulls: Option<&NullBuffer>) {
        match (self.nulls.as_mut(), nulls) {
            (Some(b), Some(nulls)) => {
                let mask = nulls.inner();
                assert_eq!(len, mask.len());
                b.push_boolean_buffer(mask)
            },
            (Some(b), None) => {
                b.push_value(true, len)
            },
            (None, Some(nulls)) => {
                let mut b = BitmaskBuilder::new(self.page_size);
                b.set_index(self.index);
                // FIXME: we have unbounded memory demand here,
                // but in our context it is never big
                b.push_value(true, self.total_len);
                let mask = nulls.inner();
                assert_eq!(len, mask.len());
                b.push_boolean_buffer(mask);
                self.nulls = Some(b)
            },
            (None, None) => {}
        }
        self.total_len += len;
    }

    pub fn total_len(&self) -> usize {
        self.total_len
    }
}