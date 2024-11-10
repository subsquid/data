use crate::writer::{AnyWriter, Writer, WriterFactory};
use arrow_buffer::ArrowNativeType;
use std::io::Write;
use std::marker::PhantomData;


mod bitmask;
mod native;
mod nullmask;
mod offsets;


pub use bitmask::*;
pub use native::*;
pub use nullmask::*;
pub use offsets::*;


pub struct IOWriter<W> {
    phantom_data: PhantomData<W>
}


impl<W: Write> Writer for IOWriter<W> {
    type Bitmask = BitmaskIOWriter<W>;
    type Nullmask = NullmaskIOWriter<W>;
    type Native = NativeIOWriter<W>;
    type Offset = OffsetsIOWriter<W>;
}


pub trait IOWriterFactory {
    type Write: Write;
    
    fn next_write(&mut self) -> anyhow::Result<Self::Write>;
}


impl<F: IOWriterFactory> WriterFactory for F {
    type Writer = IOWriter<F::Write>;

    fn nullmask(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Nullmask> {
        let write = self.next_write()?;
        Ok(NullmaskIOWriter::new(write))
    }

    fn bitmask(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Bitmask> {
        let write = self.next_write()?;
        Ok(BitmaskIOWriter::new(write))
    }

    fn native<T: ArrowNativeType>(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Native> {
        let write = self.next_write()?;
        Ok(NativeIOWriter::new(write))
    }

    fn offset(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Offset> {
        let write = self.next_write()?;
        Ok(OffsetsIOWriter::new(write))
    }
}


impl <W: Write, F: FnMut() -> anyhow::Result<W>> IOWriterFactory for F {
    type Write = W;

    fn next_write(&mut self) -> anyhow::Result<Self::Write> {
        self()
    }
}


impl<W: Write> IOWriter<W> {
    pub fn finish_any_writer(writer: AnyWriter<IOWriter<W>>) -> anyhow::Result<W> {
        Ok(match writer {
            AnyWriter::Bitmask(w) => w.finish()?,
            AnyWriter::Nullmask(w) => w.finish()?,
            AnyWriter::Native(w) => w.into_write(),
            AnyWriter::Offsets(w) => w.finish()?,
        })
    }
}