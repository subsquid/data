use crate::reader::bitmask::BitmaskReader;
use crate::reader::{ArrowReader, ByteReader};
use crate::reader::nullmask::NullmaskReader;
use crate::writer::ArrayWriter;


pub struct BooleanReader<R> {
    nulls: NullmaskReader<R>,
    values: BitmaskReader<R>
}


impl <R: ByteReader> ArrowReader for BooleanReader<R> {
    fn num_buffers(&self) -> usize {
        2
    }

    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn read_slice(&mut self, dst: &mut impl ArrayWriter, offset: usize, len: usize) -> anyhow::Result<()> {
        self.nulls.read_slice(dst.nullmask(0), offset, len)?;
        self.values.read_slice(dst.bitmask(1), offset, len)
    }
}