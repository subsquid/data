use crate::json::encoder::{Encoder, EncoderObject};
use crate::json::encoder::util::{json_close, make_object_prop};


pub struct StructField {
    prop: Vec<u8>,
    value: EncoderObject
}


impl StructField {
    pub fn new(name: &str, value: EncoderObject) -> Self {
        let prop = make_object_prop(name);
        Self {
            prop,
            value
        }
    }

    #[inline]
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.prop);
        self.value.encode(idx, out);
        out.push(b',')
    }
}


pub struct StructEncoder {
    fields: Vec<StructField>
}


impl StructEncoder {
    pub fn new(fields: Vec<StructField>) -> Self {
        Self {
            fields
        }
    }
}


impl Encoder for StructEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.push(b'{');
        for f in self.fields.iter_mut() {
            f.encode(idx, out);
        }
        json_close(b'}', out)
    }
}