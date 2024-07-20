use arrow_buffer::BooleanBufferBuilder;


pub struct BitSlice<'a> {
    buf: &'a [u8],
    offset: usize,
    len: usize
}


impl BitSlice {
    pub fn append_to(&self, builder: &mut BooleanBufferBuilder) {
        todo!()
    }
}