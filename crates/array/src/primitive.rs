use arrow_buffer::{ArrowNativeType, BufferBuilder, NullBufferBuilder};
use crate::bitmask::BitSlice;


pub struct PrimitiveSlice<'a, T> {
    values: &'a [T],
    nulls: Option<BitSlice<'a>>
}


impl <'a, T: ArrowNativeType> PrimitiveSlice<'a, T> {
    pub fn append_to(&self, builder: &mut PrimitiveBuilder<T>) {
        todo!()
    }
}


pub struct PrimitiveBuilder<T: ArrowNativeType> {
    values: BufferBuilder<T>,
    nulls: NullBufferBuilder
}