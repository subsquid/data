use crate::reader::any::AnyReader;
use crate::reader::{ArrowReader, ByteReader};
use crate::reader::nullmask::NullmaskReader;
use crate::writer::ArrayWriter;


pub struct StructReader<R> {
    nulls: NullmaskReader<R>,
    columns: Vec<AnyReader<R>>
}


impl <R: ByteReader> ArrowReader for StructReader<R> {
    fn num_buffers(&self) -> usize {
        1 + self.columns.iter().map(|c| c.num_buffers()).sum::<usize>()
    }

    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn read_slice(&mut self, dst: &mut impl ArrayWriter, offset: usize, len: usize) -> anyhow::Result<()> {
        self.nulls.read_slice(dst.nullmask(0), offset, len)?;

        let mut shift = 1;
        for col in self.columns.iter_mut() {
            col.read_slice(&mut dst.shift(shift), offset, len)?;
            shift += col.num_buffers()
        }

        Ok(())
    }
}