use sqd_array::builder::{ListBuilder, StringBuilder, UInt32Builder};


pub type HexBytesBuilder = StringBuilder;
pub type BlobHashesListBuilder = ListBuilder<HexBytesBuilder>;
pub type TraceAddressListBuilder = ListBuilder<UInt32Builder>;
