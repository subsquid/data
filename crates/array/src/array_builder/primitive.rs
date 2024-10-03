use std::marker::PhantomData;
use arrow::array::ArrowPrimitiveType;
use arrow_buffer::{ArrowNativeType, MutableBuffer};
use crate::array_builder::nullmask::NullmaskBuilder;
use crate::array_builder::buffer_writer::BufferWriter;
use crate::writer::{ArrayWriter, Writer};
use crate::util::invalid_buffer_access;


pub struct PrimitiveBuilder<T: ArrowPrimitiveType> {
    nulls: NullmaskBuilder,
    values: MutableBuffer,
    phantom_data: PhantomData<T>
}


impl <T: ArrowPrimitiveType> PrimitiveBuilder<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            nulls: NullmaskBuilder::new(capacity),
            values: MutableBuffer::new(capacity * T::Native::get_byte_width()),
            phantom_data: PhantomData::default()
        }
    }
    
    pub fn append(&mut self, val: T::Native) {
        self.nulls.append(true);
        self.values.push(val)
    }
    
    pub fn append_option(&mut self, val: Option<T::Native>) {
        if let Some(val) = val {
            self.append(val)
        } else {
            self.nulls.append(false);
            self.values.push(T::default_value())
        }
    }
}


impl <T: ArrowPrimitiveType> ArrayWriter for PrimitiveBuilder<T> {
    type Writer = BufferWriter;

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

    fn offset(&mut self, _buf: usize) -> &mut <Self::Writer as Writer>::Offset {
        invalid_buffer_access!()
    }
}