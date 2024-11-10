use crate::io::reader::IOReaderFactory;
use anyhow::ensure;


const USIZE: usize = size_of::<u32>();


pub struct DenseReader<'a> {
    data: &'a [u8],
    buffer_lengths: Vec<usize>,
    pos: usize
}


impl<'a> DenseReader<'a> {
    pub fn new(data: &'a [u8]) -> anyhow::Result<Self> {
        ensure!(
            data.len() >= USIZE,
            "dense file must have at least {} bytes", 
            USIZE
        );
        
        let num_buffers = u32::from_le_bytes(
            data[data.len() - USIZE..].try_into()?
        ) as usize;
        
        let min_byte_size = num_buffers * USIZE + USIZE;
        ensure!(
            data.len() >= min_byte_size,
            "dense file of {} buffer(s) must have at least {} bytes",
            num_buffers, 
            min_byte_size
        );
        
        let buffer_lengths: Vec<usize> = (0..num_buffers).map(|i| {
            let beg = data.len() - min_byte_size + i * USIZE;
            let end = beg + USIZE;
            u32::from_le_bytes(
                data[beg..end].try_into().unwrap()
            ) as usize
        }).collect();
        
        let buffers_size: usize = buffer_lengths.iter().sum();
        ensure!(buffers_size + min_byte_size == data.len(), "invalid file length");
        
        Ok(Self {
            data,
            buffer_lengths,
            pos: 0
        })
    }
}


impl<'a> IOReaderFactory for DenseReader<'a>  {
    type ByteReader = &'a [u8];

    fn next_byte_reader(&mut self) -> anyhow::Result<Self::ByteReader> {
        ensure!(self.pos < self.buffer_lengths.len(), "no buffers left");
        let (buf, left) = self.data.split_at(self.buffer_lengths[self.pos]);
        self.data = left;
        self.pos += 1;
        Ok(buf)
    }
}