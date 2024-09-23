use arrow_buffer::{ArrowNativeType, MutableBuffer, ToByteSlice};
use crate::writer::NativeWriter;


impl NativeWriter for MutableBuffer {
    #[inline]
    fn write_slice<T: ArrowNativeType>(&mut self, values: &[T]) -> anyhow::Result<()> {
        self.extend_from_slice(values);
        Ok(())
    }

    #[inline]
    fn write<T: ToByteSlice>(&mut self, value: T) -> anyhow::Result<()> {
        self.push(value);
        Ok(())
    }
}