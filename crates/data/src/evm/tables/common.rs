use sqd_array::builder::{ListBuilder, StringBuilder, Int32Builder};


pub type HexBytesBuilder = StringBuilder;
pub type BlobHashesListBuilder = ListBuilder<HexBytesBuilder>;
pub type TraceAddressListBuilder = ListBuilder<Int32Builder>;


pub fn sighash(bytes: &str) -> Option<&str> {
    (bytes.len() >= 10).then(|| { &bytes[0..10] })
}