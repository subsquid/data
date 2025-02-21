use sqd_array::builder::{ListBuilder, StringBuilder, UInt32Builder, UInt8Builder};


pub type Base58Builder = StringBuilder;
pub type BytesBuilder = StringBuilder;
pub type JsonBuilder = StringBuilder;
pub type AccountListBuilder = ListBuilder<Base58Builder>;
pub type AccountIndexList = ListBuilder<UInt8Builder>;
pub type InstructionAddressListBuilder = ListBuilder<UInt32Builder>;
pub type SignatureListBuilder = ListBuilder<Base58Builder>;
pub type AddressListBuilder = ListBuilder<Base58Builder>;
