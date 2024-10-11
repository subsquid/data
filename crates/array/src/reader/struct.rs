use crate::chunking::ChunkRange;
use crate::reader::any::AnyReader;
use crate::reader::{ArrayReader, BitmaskReader, Reader};
use crate::reader::chunked::ChunkedArrayReader;
use crate::writer::ArrayWriter;


pub struct StructReader<R: Reader> {
    nulls: R::Nullmask,
    columns: Vec<AnyReader<R>>
}


impl <R: Reader> StructReader<R> {
    pub fn new(nulls: R::Nullmask, columns: Vec<AnyReader<R>>) -> Self {
        Self {
            nulls,
            columns
        }
    }
}


impl <R: Reader> ArrayReader for StructReader<R> {
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

    fn read_chunk_ranges(
        chunks: &mut impl ChunkedArrayReader<ArrayReader=Self>,
        dst: &mut impl ArrayWriter,
        mut ranges: impl Iterator<Item=ChunkRange> + Clone
    ) -> anyhow::Result<()>
    {
        if chunks.num_chunks() == 0 {
            if ranges.next().is_some() {
                panic!("attempt to extract a range from an empty struct array")
            } else {
                return Ok(())
            }
        }
        
        let num_columns = chunks.chunk(0).columns.len();
        
        let nullmask_dst = dst.nullmask(0);
        for r in ranges.clone() {
            chunks.chunk(r.chunk).nulls.read_slice(nullmask_dst, r.offset, r.len)?
        }

        let mut shift = 1;
        for i in 0..num_columns {
            let mut dst = dst.shift(shift);
            
            AnyReader::read_chunk_ranges(
                &mut chunks.map(|ch| &mut ch.columns[i]),
                &mut dst,
                ranges.clone()
            )?;
            
            shift += chunks.chunk(0).columns[i].num_buffers();
        }

        Ok(())
    }
}