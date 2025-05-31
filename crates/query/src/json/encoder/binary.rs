use crate::json::encoder::Encoder;
use arrow::array::{BinaryArray, FixedSizeBinaryArray};


pub struct BinaryEncoder {
    array: BinaryArray
}


impl BinaryEncoder {
    pub fn new(array: BinaryArray) -> Self {
        Self {
            array
        }
    }
}


impl Encoder for BinaryEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let bytes = self.array.value(idx);
        write_hex(bytes, out)
    }
}


pub struct FixedSizedBinaryEncoder {
    array: FixedSizeBinaryArray
}


impl FixedSizedBinaryEncoder {
    pub fn new(array: FixedSizeBinaryArray) -> Self {
        Self {
            array
        }
    }
}


impl Encoder for FixedSizedBinaryEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let bytes = self.array.value(idx);
        write_hex(bytes, out)
    }
}


fn write_hex(bytes: &[u8], out: &mut Vec<u8>) {
    let offset = out.len();
    let len = bytes.len() + 4;
    out.resize(offset + len, 0);
    let dst = &mut out[offset..];
    dst[0] = b'"';
    dst[1] = b'0';
    dst[2] = b'x';
    faster_hex::hex_encode(bytes, &mut dst[3..len - 1]).unwrap();
    dst[len - 1] = b'"';
}