use crate::reader::native::NativeReader;
use crate::reader::nullmask::NullmaskReader;
use crate::reader::{ArrowReader, ByteReader};
use crate::writer::ArrayWriter;


pub struct PrimitiveReader<R> {
    nulls: NullmaskReader<R>,
    values: NativeReader<R>
}


impl <R: ByteReader> ArrowReader for PrimitiveReader<R> {
    fn num_buffers(&self) -> usize {
        2
    }

    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn read_slice(&mut self, dst: &mut impl ArrayWriter, offset: usize, len: usize) -> anyhow::Result<()> {
        self.nulls.read_slice(dst.nullmask(0), offset, len)?;
        self.values.read_slice(dst.native(1), offset, len)
    }
}