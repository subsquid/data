use bincode::config::{Configuration, standard};
use bincode::{Encode, Decode, encode_into_std_write, decode_from_slice};


const CONFIG: Configuration = standard();


pub trait Row: SerDe + 'static {
    type Key: Ord;

    fn key(&self) -> Self::Key;
}


pub trait SerDe {
    fn serialize(&self, out: &mut Vec<u8>);

    fn deserialize(bytes: &[u8]) -> Self;
}


impl <T: Encode + Decode> SerDe for T {
    fn serialize(&self, out: &mut Vec<u8>) {
        encode_into_std_write(self, out, CONFIG).unwrap();
    }

    fn deserialize(bytes: &[u8]) -> Self {
        decode_from_slice(bytes, CONFIG).unwrap().0
    }
}