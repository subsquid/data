use crate::array_builder::bitmask::BitmaskBuilder;
use crate::array_builder::nullmask::NullmaskBuilder;
use crate::array_builder::writer::BufferWriter;
use crate::data_builder::{DataBuilder, Writer};
use crate::util::invalid_buffer_access;


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
}


impl DataBuilder for BooleanBuilder {
    type Writer = BufferWriter;

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