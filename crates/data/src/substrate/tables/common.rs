use sqd_array::builder::{ListBuilder, StringBuilder, UInt32Builder};


pub type BytesBuilder = StringBuilder;
pub type TopicsListBuilder = ListBuilder<BytesBuilder>;
pub type JsonBuilder = StringBuilder;
pub type CallAddressListBuilder = ListBuilder<UInt32Builder>;
