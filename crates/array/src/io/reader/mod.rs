use crate::reader::{Reader, ReaderFactory};
use arrow_buffer::ArrowNativeType;
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


pub trait IOReaderFactory {
    type ByteReader: ByteReader;
    
    fn next_byte_reader(&mut self) -> anyhow::Result<Self::ByteReader>;
}


impl <F: IOReaderFactory> ReaderFactory for F {
    type Reader = IOReader<F::ByteReader>;

    fn nullmask(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Nullmask> {
        let byte_reader = self.next_byte_reader()?;
        NullmaskIOReader::new(byte_reader)
    }

    fn bitmask(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Bitmask> {
        let byte_reader = self.next_byte_reader()?;
        BitmaskIOReader::new(byte_reader)
    }

    fn native<T: ArrowNativeType>(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Native> {
        let byte_reader = self.next_byte_reader()?;
        NativeIOReader::new(byte_reader, T::get_byte_width())
    }

    fn offset(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Offset> {
        let byte_reader = self.next_byte_reader()?;
        OffsetsIOReader::new(byte_reader)
    }
}


impl <R: ByteReader, F: FnMut() -> anyhow::Result<R>> IOReaderFactory for F {
    type ByteReader = R;

    fn next_byte_reader(&mut self) -> anyhow::Result<Self::ByteReader> {
        self()
    }
}