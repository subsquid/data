use arrow::buffer::NullBuffer;
use crate::json::encoder::Encoder;


pub struct NullableEncoder<E> {
    encoder: E,
    nulls: NullBuffer
}


impl <E> NullableEncoder<E> {
    pub fn new(encoder: E, nulls: NullBuffer) -> Self {
        Self {
            encoder,
            nulls
        }
    }
}


impl <E: Encoder> Encoder for NullableEncoder<E> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        if self.nulls.is_null(idx) {
            out.extend_from_slice(b"null")
        } else {
            self.encoder.encode(idx, out)
        }
    }
}

