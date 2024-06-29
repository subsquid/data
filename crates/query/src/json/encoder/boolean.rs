use arrow::buffer::BooleanBuffer;
use crate::json::encoder::Encoder;


pub struct BooleanEncoder {
    values: BooleanBuffer
}


impl BooleanEncoder {
    pub fn new(values: BooleanBuffer) -> Self {
        Self {
            values
        }
    }
}


impl Encoder for BooleanEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        match self.values.value(idx) {
            true => {
                out.extend_from_slice(b"true")
            }
            false => {
                out.extend_from_slice(b"false")
            }
        }
    }
}