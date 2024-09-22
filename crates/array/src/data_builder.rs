use arrow::array::Array;
use arrow_buffer::{ArrowNativeType, ToByteSlice};


pub trait BitmaskWriter {
    fn write_packed_bits(&mut self, data: &[u8], offset: usize, len: usize);

    fn write_many(&mut self, val: bool, count: usize);
}


pub trait NativeWriter {
    fn write_slice<T: ArrowNativeType>(&mut self, values: &[T]);

    fn write<T: ToByteSlice>(&mut self, value: T);
}


pub trait OffsetsWriter {
    fn write_slice(&mut self, offsets: &[i32]);

    fn write_len(&mut self, len: usize);
    
    fn write(&mut self, offset: i32);
}


pub trait Writer {
    type Bitmask: BitmaskWriter;
    type Nullmask: BitmaskWriter;
    type Native: NativeWriter;
    type Offset: OffsetsWriter;
}


pub trait DataBuilder: Sized {
    type Writer: Writer;

    fn bitmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Bitmask;
    
    fn nullmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Nullmask;

    fn native(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Native;

    fn offset(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Offset;

    #[inline]
    fn shift(&mut self, pos: usize) -> impl DataBuilder<Writer = Self::Writer> + '_ {
        DataBuilderView {
            builder: self,
            pos
        }
    }

    fn push_array(&mut self, _array: &dyn Array) {
        todo!()
    }
}


struct DataBuilderView<'a, T> {
    builder: &'a mut T,
    pos: usize
}


impl <'a, T: DataBuilder> DataBuilder for DataBuilderView<'a, T> {
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
    fn shift(&mut self, pos: usize) -> impl DataBuilder<Writer = Self::Writer> + '_ {
        DataBuilderView {
            builder: self.builder,
            pos: self.pos + pos
        }
    }
}