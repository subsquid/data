use crate::json::encoder::util::json_close;
use crate::json::encoder::Encoder;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::ArrowNativeType;


pub struct ListEncoder<E> {
    spread: ListSpreadEncoder<E>
}


impl <E> ListEncoder<E> {
    pub fn new(encoder: E, offsets: OffsetBuffer<i32>) -> Self {
        Self {
            spread: ListSpreadEncoder::new(encoder, offsets)
        }
    }
}


impl <E: Encoder> Encoder for ListEncoder<E> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.push(b'[');
        self.spread.encode(idx, out);
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
    }
}