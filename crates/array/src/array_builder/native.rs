use crate::writer::{NativeWriter, RangeList};
use arrow_buffer::{ArrowNativeType, MutableBuffer, ToByteSlice};


impl NativeWriter for MutableBuffer {
    #[inline]
    fn write<T: ToByteSlice>(&mut self, value: T) -> anyhow::Result<()> {
        self.push(value);
        Ok(())
    }

    fn write_slice<T: ArrowNativeType>(&mut self, values: &[T]) -> anyhow::Result<()> {
        self.extend_from_slice(values);
        Ok(())
    }

    fn write_slice_indexes<T: ArrowNativeType>(
        &mut self,
        values: &[T],
        indexes: impl Iterator<Item=usize>
    ) -> anyhow::Result<()>
    {
        todo!()
    }

    fn write_slice_ranges<T: ArrowNativeType>(
        &mut self, 
        values: &[T], 
        ranges: &mut impl RangeList
    ) -> anyhow::Result<()> 
    {
        todo!()
    }
}