use convert_case::{Case, Casing};
use serde::Serializer;


pub fn encode_string(s: &str, out: &mut Vec<u8>) {
    let mut serializer = serde_json::Serializer::new(out);
    serializer.serialize_str(s).unwrap()
}


#[inline]
pub fn json_close(end: u8, out: &mut Vec<u8>) {
    let last = out.len() - 1;
    if out[last] == b',' {
        out[last] = end
    } else {
        out.push(end)
    }
}


pub fn to_camel_case(s: &str) -> String {
    s.to_case(Case::Camel)
}


pub fn make_object_prop(name: &str) -> Vec<u8> {
    let name = to_camel_case(name);
    let mut prop = Vec::with_capacity(name.len() + 3);
    encode_string(&name, &mut prop);
    prop.push(b':');
    prop
}