use crate::builder::memory_writer::MemoryWriter;
use crate::builder::nullmask::NullmaskBuilder;
use crate::builder::offsets::OffsetsBuilder;
use crate::builder::ArrayBuilder;
use crate::slice::{AsSlice, ListSlice};
use crate::util::invalid_buffer_access;
use crate::writer::{ArrayWriter, Writer};
use arrow::array::{ArrayRef, BinaryArray, StringArray};
use arrow::datatypes::DataType;
use arrow_buffer::MutableBuffer;
use std::sync::Arc;


pub struct BinaryBuilder {
    nulls: NullmaskBuilder,
    offsets: OffsetsBuilder,
    values: MutableBuffer
}


impl BinaryBuilder {
    pub fn new(item_capacity: usize, content_capacity: usize) -> Self {
        Self {
            nulls: NullmaskBuilder::new(item_capacity),
            offsets: OffsetsBuilder::new(item_capacity),
            values: MutableBuffer::new(content_capacity)
        }
    }

    pub fn append(&mut self, val: &[u8]) {
        self.values.extend_from_slice(val);
        self.nulls.append(true);
        self.offsets.append(self.values.len() as i32);
    }
    
    pub fn append_option(&mut self, val: Option<&[u8]>) {
        if let Some(val) = val {
            self.values.extend_from_slice(val);
            self.nulls.append(true);
        } else {
            self.nulls.append(false);
        }
        self.offsets.append(self.values.len() as i32);
    }

    pub fn append_null(&mut self) {
        self.nulls.append(false);
        self.offsets.append(self.values.len() as i32);
    }
    
    pub fn finish(self) -> BinaryArray {
        BinaryArray::new(
            self.offsets.finish(),
            self.values.into(),
            self.nulls.finish()
        )
    }
}


impl ArrayBuilder for BinaryBuilder {
    fn data_type(&self) -> DataType {
        DataType::Binary
    }

    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn byte_size(&self) -> usize {
        self.nulls.byte_size() + self.offsets.byte_size() + self.values.len()
    }

    fn clear(&mut self) {
        self.nulls.clear();
        self.offsets.clear();
        self.values.clear()
    }

    fn finish(self) -> ArrayRef {
        Arc::new(self.finish())
    }
}


impl ArrayWriter for BinaryBuilder {
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
        if buf == 2 {
            &mut self.values
        } else {
            invalid_buffer_access!()
        }
    }

    #[inline]
    fn offset(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Offset {
        if buf == 1 {
            &mut self.offsets
        } else {
            invalid_buffer_access!()
        }
    }
}


impl AsSlice for BinaryBuilder {
    type Slice<'a> = ListSlice<'a, &'a [u8]>;

    fn as_slice(&self) -> Self::Slice<'_> {
        ListSlice::new(
            self.offsets.as_slice(),
            self.values.as_slice(),
            self.nulls.as_slice().bitmask()
        )
    }
}


impl Default for BinaryBuilder {
    fn default() -> Self {
        Self::new(0, 0)
    }
}


pub struct StringBuilder {
    nulls: NullmaskBuilder,
    offsets: OffsetsBuilder,
    values: MutableBuffer,
    validity: Option<usize>
}


impl StringBuilder {
    pub fn new(item_capacity: usize, content_capacity: usize) -> Self {
        Self {
            nulls: NullmaskBuilder::new(item_capacity),
            offsets: OffsetsBuilder::new(item_capacity),
            values: MutableBuffer::new(content_capacity),
            validity: None
        }
    }

    pub fn append(&mut self, val: &str) {
        self.values.extend_from_slice(val.as_bytes());
        self.nulls.append(true);
        self.offsets.append(self.values.len() as i32);
    }

    pub fn append_option(&mut self, val: Option<&str>) {
        if let Some(val) = val {
            self.values.extend_from_slice(val.as_bytes());
            self.nulls.append(true);
        } else {
            self.nulls.append(false);
        }
        self.offsets.append(self.values.len() as i32);
    }

    pub fn append_null(&mut self) {
        self.nulls.append(false);
        self.offsets.append(self.values.len() as i32);
    }
    
    fn mark_maybe_invalid(&mut self) {
        if self.validity == None {
            self.validity = Some(self.offsets.as_slice().len())
        }
    }

    pub fn finish(self) -> StringArray {
        StringArray::new(
            self.offsets.finish(),
            self.values.into(),
            self.nulls.finish()
        )
    }
    
    pub unsafe fn finish_unchecked(self) -> StringArray {
        StringArray::new_unchecked(
            self.offsets.finish(),
            self.values.into(),
            self.nulls.finish()
        )
    }
}


impl ArrayBuilder for StringBuilder {
    fn data_type(&self) -> DataType {
        DataType::Utf8
    }

    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn byte_size(&self) -> usize {
        self.nulls.byte_size() + self.offsets.byte_size() + self.values.len()
    }

    fn clear(&mut self) {
        self.nulls.clear();
        self.offsets.clear();
        self.values.clear();
        self.validity = None
    }

    fn finish(self) -> ArrayRef {
        Arc::new(self.finish())
    }

    unsafe fn finish_unchecked(self) -> ArrayRef {
        Arc::new(self.finish_unchecked())
    }
}


impl ArrayWriter for StringBuilder {
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
        self.mark_maybe_invalid();
        if buf == 2 {
            &mut self.values
        } else {
            invalid_buffer_access!()
        }
    }

    #[inline]
    fn offset(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Offset {
        self.mark_maybe_invalid();
        if buf == 1 {
            &mut self.offsets
        } else {
            invalid_buffer_access!()
        }
    }
}


impl AsSlice for StringBuilder {
    type Slice<'a> = ListSlice<'a, &'a [u8]>;

    fn as_slice(&self) -> Self::Slice<'_> {
        ListSlice::new(
            self.offsets.as_slice(),
            self.values.as_slice(),
            self.nulls.as_slice().bitmask()
        )
    }
}


impl Default for StringBuilder {
    fn default() -> Self {
        Self::new(0, 0)
    }
}


impl std::fmt::Write for StringBuilder {
    #[inline]
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.values.extend_from_slice(s.as_bytes());
        Ok(())
    }
}