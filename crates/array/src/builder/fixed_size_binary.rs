use crate::builder::memory_writer::MemoryWriter;
use crate::builder::nullmask::NullmaskBuilder;
use crate::builder::ArrayBuilder;
use crate::slice::{AsSlice, FixedSizeListSlice};
use crate::util::invalid_buffer_access;
use crate::writer::{ArrayWriter, Writer};
use arrow::array::{ArrayRef, FixedSizeBinaryArray};
use arrow::datatypes::DataType;
use arrow_buffer::MutableBuffer;
use std::sync::Arc;

pub struct FixedSizeBinaryBuilder {
    size: usize,
    nulls: NullmaskBuilder,
    values: MutableBuffer,
}

impl FixedSizeBinaryBuilder {
    pub fn new(size: usize, item_capacity: usize) -> Self {
        Self {
            size,
            nulls: NullmaskBuilder::new(item_capacity),
            values: MutableBuffer::new(item_capacity * size),
        }
    }

    pub fn append(&mut self, val: &[u8]) {
        assert_eq!(val.len(), self.size);
        self.values.extend_from_slice(val);
        self.nulls.append(true);
    }

    pub fn append_option(&mut self, val: Option<&[u8]>) {
        if let Some(val) = val {
            self.append(val);
        } else {
            self.append_null();
        }
    }

    pub fn append_null(&mut self) {
        self.values.extend_zeros(self.size);
        self.nulls.append(false);
    }

    pub fn finish(self) -> FixedSizeBinaryArray {
        FixedSizeBinaryArray::new(self.size as _, self.values.into(), self.nulls.finish())
    }
}

impl ArrayBuilder for FixedSizeBinaryBuilder {
    fn data_type(&self) -> DataType {
        DataType::FixedSizeBinary(self.size as _)
    }

    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn byte_size(&self) -> usize {
        self.nulls.byte_size() + self.values.len()
    }

    fn clear(&mut self) {
        self.nulls.clear();
        self.values.clear()
    }

    fn finish(self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl ArrayWriter for FixedSizeBinaryBuilder {
    type Writer = MemoryWriter;

    fn bitmask(&mut self, _buf: usize) -> &mut <Self::Writer as Writer>::Bitmask {
        invalid_buffer_access!()
    }

    #[inline]
    fn nullmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Nullmask {
        if buf == 0 {
            &mut self.nulls
        } else {
            invalid_buffer_access!()
        }
    }

    #[inline]
    fn native(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Native {
        if buf == 1 {
            &mut self.values
        } else {
            invalid_buffer_access!()
        }
    }

    #[inline]
    fn offset(&mut self, _buf: usize) -> &mut <Self::Writer as Writer>::Offset {
        invalid_buffer_access!()
    }
}

impl AsSlice for FixedSizeBinaryBuilder {
    type Slice<'a> = FixedSizeListSlice<'a, &'a [u8]>;

    fn as_slice(&self) -> Self::Slice<'_> {
        FixedSizeListSlice::new(
            self.size,
            self.values.as_slice(),
            self.nulls.as_slice().bitmask(),
        )
    }
}

impl Default for FixedSizeBinaryBuilder {
    fn default() -> Self {
        unimplemented!()
    }
}
