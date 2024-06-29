
pub use nullable::*;
pub use primitive::*;
pub use boolean::*;
pub use string::*;
pub use list::*;
pub use r#struct::*;
pub use json::*;

mod nullable;
mod primitive;
mod boolean;
mod string;
mod list;
mod r#struct;
pub mod util;
pub mod factory;
mod json;


pub trait Encoder: Send {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>);
}


pub type EncoderObject = Box<dyn Encoder>;


impl Encoder for Box<dyn Encoder> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        self.as_mut().encode(idx, out)
    }
}
