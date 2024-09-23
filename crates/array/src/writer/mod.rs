mod any;


use arrow::array::Array;
use arrow_buffer::{ArrowNativeType, ToByteSlice};
pub use any::*;


pub trait BitmaskWriter {
    fn write_packed_bits(&mut self, data: &[u8], offset: usize, len: usize) -> anyhow::Result<()>;

    fn write_many(&mut self, val: bool, count: usize) -> anyhow::Result<()>;
}


pub trait NativeWriter {
    fn write_slice<T: ArrowNativeType>(&mut self, values: &[T]) -> anyhow::Result<()>;

    fn write<T: ToByteSlice>(&mut self, value: T) -> anyhow::Result<()>;
}


pub trait OffsetsWriter {
    fn write_slice(&mut self, offsets: &[i32]) -> anyhow::Result<()>;

    fn write_len(&mut self, len: usize) -> anyhow::Result<()>;
    
    fn write(&mut self, offset: i32) -> anyhow::Result<()>;
}


pub trait Writer {
    type Bitmask: BitmaskWriter;
    type Nullmask: BitmaskWriter;
    type Native: NativeWriter;
    type Offset: OffsetsWriter;
}


pub trait ArrayWriter: Sized {
    type Writer: Writer;

    fn bitmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Bitmask;
    
    fn nullmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Nullmask;

    fn native(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Native;

    fn offset(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Offset;

    #[inline]
    fn shift(&mut self, pos: usize) -> impl ArrayWriter<Writer = Self::Writer> + '_ {
        ArrayWriterView {
            builder: self,
            pos
        }
    }

    fn push_array(&mut self, _array: &dyn Array) {
        todo!()
    }
}


struct ArrayWriterView<'a, T> {
    builder: &'a mut T,
    pos: usize
}


impl <'a, T: ArrayWriter> ArrayWriter for ArrayWriterView<'a, T> {
    type Writer = T::Writer;

    #[inline]
    fn bitmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Bitmask {
        self.builder.bitmask(buf + self.pos)
    }

    #[inline]
    fn nullmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Nullmask {
        self.builder.nullmask(buf + self.pos)
    }

    #[inline]
    fn native(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Native {
        self.builder.native(buf + self.pos)
    }

    #[inline]
    fn offset(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Offset {
        self.builder.offset(buf + self.pos)
    }
    
    #[inline]
    fn shift(&mut self, pos: usize) -> impl ArrayWriter<Writer = Self::Writer> + '_ {
        ArrayWriterView {
            builder: self.builder,
            pos: self.pos + pos
        }
    }
}