use crate::writer::ArrayWriter;


mod primitive;
mod bitmask;
mod nullmask;
mod native;
mod boolean;
mod offsets;
mod list;
mod any;
mod r#struct;


pub use any::*;
pub use boolean::*;
pub use list::*;
pub use primitive::*;
pub use r#struct::*;


pub trait ByteReader {
    fn len(&self) -> usize;
    
    fn read(&mut self, offset: usize, len: usize) -> anyhow::Result<&[u8]>;
    
    fn read_exact(
        &mut self, 
        mut offset: usize, 
        len: usize, 
        mut cb: impl FnMut(&[u8]) -> anyhow::Result<()>
    ) -> anyhow::Result<()> 
    {
        let end = offset + len;
        while offset < end {
            let bytes = self.read(offset, end - offset)?;
            cb(bytes)?;
            offset += bytes.len()
        }
        Ok(())
    }
}


pub trait ArrowReader {
    fn num_buffers(&self) -> usize;
    
    fn len(&self) -> usize;
    
    fn read_slice(
        &mut self, 
        dst: &mut impl ArrayWriter, 
        offset: usize, 
        len: usize
    ) -> anyhow::Result<()>;
}