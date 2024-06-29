use arrow::array::StringArray;

use crate::json::encoder::Encoder;
use crate::json::encoder::util::encode_string;


pub struct StringEncoder {
    array: StringArray
}


impl StringEncoder {
    pub fn new(array: StringArray) -> Self {
        Self {
            array
        }
    }
}


impl Encoder for StringEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let s = self.array.value(idx);
        encode_string(s, out)
    }
}


pub struct SafeStringEncoder<E> {
    encode: E
}


impl <E> SafeStringEncoder<E> {
    pub fn new(encode: E) -> Self {
        Self {
            encode
        }
    }
}


impl <E: Encoder> Encoder for SafeStringEncoder<E> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.push(b'"');
        self.encode.encode(idx, out);
        out.push(b'"')
    }
}