use arrow::datatypes::DataType;
use sqd_array::builder::{AnyBuilder, ArrayBuilder};
use sqd_array::item_index_cast::cast_item_index;
use sqd_array::reader::ArrayReader;
use sqd_array::slice::AsSlice;
use sqd_array::writer::ArrayWriter;


pub struct IndexCastReader<S> {
    src: S,
    src_buf: AnyBuilder,
    target_type: DataType
}


impl<S: ArrayReader> IndexCastReader<S> {
    pub fn new(src: S, src_type: &DataType, target_type: DataType) -> Self {
        Self {
            src,
            src_buf: AnyBuilder::new(src_type),
            target_type
        }
    }
    
    pub fn len(&self) -> usize {
        self.src.len()
    }
    
    pub fn read(&mut self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        self.read_slice(dst, 0, self.len())
    }

    pub fn read_slice(
        &mut self,
        dst: &mut impl ArrayWriter,
        mut offset: usize,
        mut len: usize
    ) -> anyhow::Result<()> {
        while len > 0 {
            let step = std::cmp::min(len, 1000);
            self.src_buf.clear();
            self.src.read_slice(&mut self.src_buf, offset, step)?;
            cast_item_index(&self.src_buf.as_slice(), &self.target_type, dst)?;
            offset += step;
            len -= step
        }
        Ok(())
    }
}


pub enum MaybeCastedReader<R> {
    Plain(R),
    Cast(IndexCastReader<R>)
}


impl<R: ArrayReader> MaybeCastedReader<R> {
    pub fn read(
        &mut self,
        dst: &mut impl ArrayWriter
    ) -> anyhow::Result<()>
    {
        match self {
            MaybeCastedReader::Plain(r) => r.read(dst),
            MaybeCastedReader::Cast(r) => r.read(dst),
        }
    }
    
    pub fn read_slice(
        &mut self, 
        dst: &mut impl ArrayWriter, 
        offset: usize, 
        len: usize
    ) -> anyhow::Result<()> 
    {
        match self {
            MaybeCastedReader::Plain(r) => r.read_slice(dst, offset, len),
            MaybeCastedReader::Cast(r) => r.read_slice(dst, offset, len),
        }    
    }
}