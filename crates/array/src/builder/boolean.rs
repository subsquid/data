use std::sync::Arc;
use crate::builder::bitmask::BitmaskBuilder;
use crate::builder::memory_writer::MemoryWriter;
use crate::builder::nullmask::NullmaskBuilder;
use crate::util::invalid_buffer_access;
use crate::writer::{ArrayWriter, Writer};
use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::DataType;
use crate::builder::ArrayBuilder;
use crate::slice::{AsSlice, BooleanSlice};


pub struct BooleanBuilder {
    nulls: NullmaskBuilder,
    values: BitmaskBuilder
}


impl BooleanBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            nulls: NullmaskBuilder::new(capacity),
            values: BitmaskBuilder::new(capacity)
        }
    }
    
    pub fn append(&mut self, val: bool) {
        self.nulls.append(true);
        self.values.append(val)
    }
    
    pub fn append_option(&mut self, val: Option<bool>) {
        if let Some(val) = val {
            self.append(val)
        } else {
            self.nulls.append(false);
            self.values.append(false)
        }
    }
    
    pub fn finish(self) -> BooleanArray {
        BooleanArray::new(self.values.finish(), self.nulls.finish())
    }
}


impl ArrayBuilder for BooleanBuilder {
    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn finish(self) -> ArrayRef {
        Arc::new(self.finish())
    }
}


impl ArrayWriter for BooleanBuilder {
    type Writer = MemoryWriter;

    #[inline]
    fn bitmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Bitmask {
        if buf == 1 {
            &mut self.values
        } else {
            invalid_buffer_access!()
        }
    }

    #[inline]
    fn nullmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Nullmask {
        if buf == 0 {
            &mut self.nulls
        } else {
            invalid_buffer_access!()
        }
    }

    fn native(&mut self, _buf: usize) -> &mut <Self::Writer as Writer>::Native {
        invalid_buffer_access!()
    }

    fn offset(&mut self, _buf: usize) -> &mut <Self::Writer as Writer>::Offset {
        invalid_buffer_access!()
    }
}


impl AsSlice for BooleanBuilder {
    type Slice<'a> = BooleanSlice<'a>;

    fn as_slice(&self) -> Self::Slice<'_> {
        BooleanSlice::with_nullmask(self.values.as_slice(), self.nulls.as_slice())
    }
}