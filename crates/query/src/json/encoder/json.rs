use arrow::buffer::{Buffer, OffsetBuffer};
use arrow::datatypes::ArrowNativeType;
use crate::json::encoder::Encoder;


pub struct JsonEncoder {
    buffer: Buffer,
    offsets: OffsetBuffer<i32>
}


impl JsonEncoder {
    pub fn new(buffer: Buffer, offsets: OffsetBuffer<i32>) -> Self {
        Self {
            buffer,
            offsets
        }
    }
}


impl Encoder for JsonEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let start = self.offsets[idx].as_usize();
        let end = self.offsets[idx + 1].as_usize();
        out.extend_from_slice(&self.buffer[start..end])
    }
}