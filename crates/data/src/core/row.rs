use borsh::{BorshDeserialize, BorshSerialize};


pub trait Row: RowSerDe + 'static {
    type Key: Ord;

    fn key(&self) -> Self::Key;
}


pub trait RowSerDe {
    fn serialize(&self, out: &mut Vec<u8>);

    fn deserialize(bytes: &[u8]) -> Self;
}


impl <T: BorshSerialize + BorshDeserialize> RowSerDe for T {
    fn serialize(&self, out: &mut Vec<u8>) {
        self.serialize(out).unwrap();
    }

    fn deserialize(bytes: &[u8]) -> Self {
        borsh::from_slice(bytes).unwrap()
    }
}