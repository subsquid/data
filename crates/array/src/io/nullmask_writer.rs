use std::io::Write;
use crate::io::bitmask_writer::BitmaskIOWriter;
use crate::writer::{BitmaskWriter, RangeList};


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
    fn write_slice(&mut self, data: &[u8], offset: usize, len: usize) -> anyhow::Result<()> {
        todo!()
    }

    fn write_slice_indexes(&mut self, data: &[u8], indexes: impl Iterator<Item=usize>) -> anyhow::Result<()> {
        todo!()
    }

    fn write_slice_ranges(&mut self, data: &[u8], ranges: &mut impl RangeList) -> anyhow::Result<()> {
        todo!()
    }

    fn write_many(&mut self, val: bool, count: usize) -> anyhow::Result<()> {
        todo!()
    }
}