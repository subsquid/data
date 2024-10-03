use crate::reader::{ArrowReader, ByteReader};
use crate::reader::nullmask::NullmaskReader;
use crate::reader::offsets::OffsetsReader;
use crate::writer::ArrayWriter;


pub struct ListReader<R, T> {
    nulls: NullmaskReader<R>,
    offsets: OffsetsReader<R>,
    values: T
}


impl <R: ByteReader, T: ArrowReader> ArrowReader for ListReader<R, T> {
    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn read_slice(&mut self, dst: &mut impl ArrayWriter, offset: usize, len: usize) -> anyhow::Result<()> {
        self.nulls.read_slice(dst.nullmask(0), offset, len)?;
        
        let value_range = self.offsets.read_slice(dst.offset(1), offset, len)?;
        
        self.values.read_slice(&mut dst.shift(2), value_range.start, value_range.len())
    }
}