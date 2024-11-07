use crate::reader::Reader;
use std::marker::PhantomData;


mod byte_reader;
mod bitmask;
mod native;
mod nullmask;
mod offsets;


pub use bitmask::*;
pub use byte_reader::*;
pub use native::*;
pub use nullmask::*;
pub use offsets::*;


pub struct IOReader<B> {
    byte_reader: PhantomData<B>
}


impl <B: ByteReader> Reader for IOReader<B> {
    type Nullmask = NullmaskIOReader<B>;
    type Bitmask = BitmaskIOReader<B>;
    type Native = NativeIOReader<B>;
    type Offset = OffsetsIOReader<B>;
} 