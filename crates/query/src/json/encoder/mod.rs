mod binary;
mod boolean;
pub mod factory;
mod hex;
mod json;
mod list;
mod nullable;
mod primitive;
mod r#struct;
mod string;
pub mod util;


pub use binary::*;
pub use boolean::*;
pub use hex::*;
pub use json::*;
pub use list::*;
pub use nullable::*;
pub use primitive::*;
pub use r#struct::*;
pub use string::*;


pub trait Encoder: Send {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>);
}


pub type EncoderObject = Box<dyn Encoder>;


impl Encoder for Box<dyn Encoder> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        self.as_mut().encode(idx, out)
    }
}
