use sqd_array::builder::{StringBuilder};

pub type HexBytesBuilder = StringBuilder;
pub type JsonBuilder = StringBuilder;

pub fn sighash(bytes: &str) -> Option<&str> {
    (bytes.len() >= 8).then(|| { &bytes[0..8] })
}