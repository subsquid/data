use arrow::buffer::OffsetBuffer;
use arrow::datatypes::ArrowNativeType;
use crate::json::encoder::Encoder;
use crate::json::encoder::util::json_close;


pub struct ListEncoder<E> {
    encoder: E,
    offsets: OffsetBuffer<i32>
}


impl <E> ListEncoder<E> {
    pub fn new(encoder: E, offsets: OffsetBuffer<i32>) -> Self {
        Self {
            encoder,
            offsets
        }
    }
}


impl <E: Encoder> Encoder for ListEncoder<E> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let start = self.offsets[idx].as_usize();
        let end = self.offsets[idx + 1].as_usize();
        out.push(b'[');
        for i in start..end {
            self.encoder.encode(i, out);
            out.push(b',');
        }
        json_close(b']', out)
    }
}


pub struct ListSpreadEncoder<E> {
    encoder: E,
    offsets: OffsetBuffer<i32>
}


impl <E> ListSpreadEncoder<E> {
    pub fn new(encoder: E, offsets: OffsetBuffer<i32>) -> Self {
        Self {
            encoder,
            offsets
        }
    }
}


impl <E: Encoder> Encoder for ListSpreadEncoder<E> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let start = self.offsets[idx].as_usize();
        let end = self.offsets[idx + 1].as_usize();
        for i in start..end {
            self.encoder.encode(i, out);
            out.push(b',');
        }
        if start < end {
            out.pop();
        }
    }
}