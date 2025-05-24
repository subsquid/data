use sqd_array::builder::{ListBuilder, StringBuilder, UInt32Builder};


pub type HexBytesBuilder = StringBuilder;
pub type BlobHashesListBuilder = ListBuilder<HexBytesBuilder>;
pub type TraceAddressListBuilder = ListBuilder<UInt32Builder>;


pub fn sighash(bytes: &str) -> Option<&str> {
    (bytes.len() >= 10).then_some(&bytes[0..10])
}