mod any;
mod range_list;


pub use any::*;
use arrow::array::Array;
use arrow_buffer::{ArrowNativeType, ToByteSlice};
pub use range_list::*;


pub trait BitmaskWriter {
    fn write_slice(&mut self, data: &[u8], offset: usize, len: usize) -> anyhow::Result<()>;

    fn write_slice_indexes(
        &mut self,
        data: &[u8],
        indexes: impl Iterator<Item = usize> + Clone
    ) -> anyhow::Result<()>;

    fn write_slice_ranges(
        &mut self,
        data: &[u8],
        ranges: &mut impl RangeList
    ) -> anyhow::Result<()>;

    fn write_many(&mut self, val: bool, count: usize) -> anyhow::Result<()>;
}


pub trait NativeWriter {
    fn write<T: ToByteSlice>(&mut self, value: T) -> anyhow::Result<()>;
    
    fn write_slice<T: ArrowNativeType>(&mut self, values: &[T]) -> anyhow::Result<()>;

    fn write_slice_indexes<T: ArrowNativeType>(
        &mut self,
        values: &[T],
        indexes: impl Iterator<Item = usize>
    ) -> anyhow::Result<()>;

    fn write_slice_ranges<T: ArrowNativeType>(
        &mut self,
        values: &[T],
        ranges: &mut impl RangeList
    ) -> anyhow::Result<()>;
}


pub trait OffsetsWriter {
    fn write_slice(&mut self, offsets: &[i32]) -> anyhow::Result<()>;

    fn write_slice_indexes(
        &mut self,
        offsets: &[i32],
        indexes: impl Iterator<Item = usize>
    ) -> anyhow::Result<()>;

    fn write_slice_ranges(
        &mut self,
        offsets: &[i32],
        ranges: &mut impl RangeList
    ) -> anyhow::Result<()>;

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


pub trait WriterFactory {
    type Writer: Writer;

    fn nullmask(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Nullmask>;

    fn bitmask(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Bitmask>;

    fn native(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Native>;

    fn offset(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Offset>;
}