use std::io::Write;
use arrow_buffer::{ArrowNativeType, ToByteSlice};
use crate::writer::NativeWriter;


pub struct NativeIOWriter<W> {
    writer: W
}


impl <W: Write> NativeWriter for NativeIOWriter<W> {
    #[inline]
    fn write_slice<T: ArrowNativeType>(&mut self, values: &[T]) -> anyhow::Result<()> {
        self.writer.write_all(values.to_byte_slice())?;
        Ok(())
    }
    
    #[inline]
    fn write<T: ToByteSlice>(&mut self, value: T) -> anyhow::Result<()> {
        self.writer.write_all(value.to_byte_slice())?;
        Ok(())
    }
}