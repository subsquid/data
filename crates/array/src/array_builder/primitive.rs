use crate::array_builder::memory_writer::MemoryWriter;
use crate::array_builder::nullmask::NullmaskBuilder;
use crate::util::invalid_buffer_access;
use crate::writer::{ArrayWriter, Writer};
use arrow::array::{ArrayRef, ArrowPrimitiveType, PrimitiveArray};
use arrow_buffer::{ArrowNativeType, MutableBuffer, ScalarBuffer};
use std::marker::PhantomData;
use std::sync::Arc;
use arrow::datatypes::DataType;
use crate::array_builder::ArrayBuilder;
use crate::slice::{AsSlice, PrimitiveSlice};


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
    
    pub fn finish(self) -> PrimitiveArray<T> {
        PrimitiveArray::new(
            ScalarBuffer::from(self.values), 
            self.nulls.finish()
        )
    }
}


impl <T: ArrowPrimitiveType> ArrayBuilder for PrimitiveBuilder<T> {
    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn data_type(&self) -> DataType {
        T::DATA_TYPE
    }

    fn finish(self) -> ArrayRef {
        Arc::new(self.finish())
    }
}


impl <T: ArrowPrimitiveType> ArrayWriter for PrimitiveBuilder<T> {
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

    fn offset(&mut self, _buf: usize) -> &mut <Self::Writer as Writer>::Offset {
        invalid_buffer_access!()
    }
}


impl <T: ArrowPrimitiveType> AsSlice for PrimitiveBuilder<T> {
    type Slice<'a> = PrimitiveSlice<'a, T::Native>;

    fn as_slice(&self) -> Self::Slice<'_> {
        PrimitiveSlice::new(
            self.values.typed_data::<T::Native>(),
            self.nulls.as_slice().bitmask()
        )
    }
}