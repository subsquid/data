use crate::writer::NativeWriter;
use arrow_buffer::{ArrowNativeType, ToByteSlice};
use std::io::Write;
use crate::index::RangeList;


pub struct NativeIOWriter<W> {
    write: W
}


impl <W> NativeIOWriter<W> {
    pub fn new(write: W) -> Self {
        Self { write }
    }

    pub fn into_write(self) -> W {
        self.write
    }
}


impl <W: Write> NativeWriter for NativeIOWriter<W> {
    #[inline]
    fn write<T: ToByteSlice>(&mut self, value: T) -> anyhow::Result<()> {
        self.write.write_all(value.to_byte_slice())?;
        Ok(())
    }

    #[inline]
    fn write_slice<T: ArrowNativeType>(&mut self, values: &[T]) -> anyhow::Result<()> {
        self.write.write_all(values.to_byte_slice())?;
        Ok(())
    }

    #[inline]
    fn write_slice_indexes<T: ArrowNativeType>(
        &mut self, 
        values: &[T], 
        indexes: impl Iterator<Item=usize>
    ) -> anyhow::Result<()> {
        for i in indexes {
            self.write(values[i])?;
        }
        Ok(())
    }

    #[inline]
    fn write_slice_ranges<T: ArrowNativeType>(
        &mut self, 
        values: &[T], 
        ranges: &mut impl RangeList
    ) -> anyhow::Result<()> {
        for r in ranges.iter() {
            self.write_slice(&values[r])?;
        }
        Ok(())
    }
}