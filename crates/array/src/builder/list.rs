use crate::builder::memory_writer::MemoryWriter;
use crate::builder::nullmask::NullmaskBuilder;
use crate::builder::offsets::OffsetsBuilder;
use crate::builder::ArrayBuilder;
use crate::slice::{AsSlice, ListSlice};
use crate::util::invalid_buffer_access;
use crate::writer::{ArrayWriter, Writer};
use arrow::array::{ArrayRef, ListArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use std::sync::Arc;


pub struct ListBuilder<T> {
    nulls: NullmaskBuilder,
    offsets: OffsetsBuilder,
    values: T,
    field: FieldRef
}


impl <T: ArrayBuilder> ListBuilder<T> {
    pub fn new(capacity: usize, values: T, field_name: Option<String>) -> Self {
        let field = Field::new(
            field_name.unwrap_or_else(|| "item".to_string()),
            values.data_type(),
            true
        );
        Self {
            nulls: NullmaskBuilder::new(capacity),
            offsets: OffsetsBuilder::new(capacity),
            values,
            field: Arc::new(field)
        }
    }
    
    pub fn append(&mut self) {
        self.nulls.append(true);
        self.offsets.append(self.values.len() as i32);
    }
    
    pub fn append_null(&mut self) {
        self.nulls.append(false);
        self.offsets.append(self.values.len() as i32);
    }
    
    pub fn values(&mut self) -> &mut T {
        &mut self.values
    }
    
    pub fn finish(self) -> ListArray {
        ListArray::new(
            self.field, 
            self.offsets.finish(), 
            self.values.finish(), 
            self.nulls.finish()
        )
    }
}


impl <T: ArrayBuilder> ArrayBuilder for ListBuilder<T> {
    fn data_type(&self) -> DataType {
        DataType::List(self.field.clone())
    }

    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn byte_size(&self) -> usize {
        self.nulls.byte_size() + self.offsets.byte_size() + self.values.byte_size()
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


impl <T: ArrayWriter<Writer=MemoryWriter>> ArrayWriter for ListBuilder<T> {
    type Writer = MemoryWriter;

    #[inline]
    fn bitmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Bitmask {
        if buf >= 2 {
            self.values.bitmask(buf - 2)
        } else {
            invalid_buffer_access!()
        }
    }

    #[inline]
    fn nullmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Nullmask {
        match buf { 
            0 => &mut self.nulls,
            1 => invalid_buffer_access!(),
            i => self.values.nullmask(i - 2)
        }
    }

    #[inline]
    fn native(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Native {
        if buf >= 2 {
            self.values.native(buf - 2)
        } else {
            invalid_buffer_access!()
        }
    }

    #[inline]
    fn offset(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Offset {
        match buf { 
            0 => invalid_buffer_access!(),
            1 => &mut self.offsets,
            i => self.values.offset(i - 2)
        }
    }
}


impl <T: AsSlice + 'static> AsSlice for ListBuilder<T> {
    type Slice<'a> = ListSlice<'a, T::Slice<'a>>;

    fn as_slice(&self) -> Self::Slice<'_> {
        ListSlice::new(
            self.offsets.as_slice(),
            self.values.as_slice(),
            self.nulls.as_slice().bitmask()
        )
    }
}


impl <T: ArrayBuilder + Default> Default for ListBuilder<T> {
    fn default() -> Self {
        Self::new(0, T::default(), None)
    }
}