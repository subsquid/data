use crate::array_builder::ArrayBuilder;
use crate::array_builder::nullmask::NullmaskBuilder;
use crate::array_builder::offsets::OffsetsBuilder;
use crate::array_builder::buffer_writer::BufferWriter;
use crate::writer::{ArrayWriter, Writer};
use crate::util::invalid_buffer_access;


pub struct ListBuilder<T> {
    nulls: NullmaskBuilder,
    offsets: OffsetsBuilder,
    values: T
}


impl <T: ArrayBuilder> ListBuilder<T> {
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
}


impl <T: ArrayBuilder> ArrayWriter for ListBuilder<T> {
    type Writer = BufferWriter;

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