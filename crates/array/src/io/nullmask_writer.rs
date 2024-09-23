use std::io::Write;
use crate::io::bitmask_writer::BitmaskIOWriter;
use crate::writer::BitmaskWriter;


pub struct NullmaskIOWriter<W> {
    bitmask: BitmaskIOWriter<W>,
    len: usize,
    has_bitmask: bool
}


impl <W: Write> NullmaskIOWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            bitmask: BitmaskIOWriter::new(writer),
            len: 0,
            has_bitmask: false
        }
    }
}


impl<W: Write> BitmaskWriter for NullmaskIOWriter<W> {
    fn write_packed_bits(&mut self, data: &[u8], offset: usize, len: usize) -> anyhow::Result<()> {
        todo!()
    }

    fn write_many(&mut self, val: bool, count: usize) -> anyhow::Result<()> {
        todo!()
    }
}