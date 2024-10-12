use crate::writer::{NativeWriter, RangeList};
use arrow_buffer::{ArrowNativeType, MutableBuffer, ToByteSlice};


impl NativeWriter for MutableBuffer {
    #[inline]
    fn write<T: ToByteSlice>(&mut self, value: T) -> anyhow::Result<()> {
        self.push(value);
        Ok(())
    }

    #[inline]
    fn write_slice<T: ArrowNativeType>(&mut self, values: &[T]) -> anyhow::Result<()> {
        self.extend_from_slice(values);
        Ok(())
    }

    fn write_slice_indexes<T: ArrowNativeType>(
        &mut self,
        values: &[T],
        mut indexes: impl Iterator<Item=usize>
    ) -> anyhow::Result<()>
    {
        let value_size = size_of::<T>();
        let min_byte_len = indexes.size_hint().0 * value_size;

        self.reserve(min_byte_len);

        let mut byte_len = self.len();

        while byte_len < min_byte_len {
            if let Some(i) = indexes.next() {
                unsafe {
                    let src = values[i].to_byte_slice().as_ptr();
                    let dst = self.as_mut_ptr().add(byte_len);
                    std::ptr::copy_nonoverlapping(src, dst, value_size);
                    byte_len += value_size;
                }
            } else {
                unsafe {
                    self.set_len(byte_len)
                }
                return Ok(())
            }
        }

        unsafe {
            self.set_len(byte_len)
        }

        while let Some(i) = indexes.next() {
            self.push(values[i])
        }

        Ok(())
    }

    fn write_slice_ranges<T: ArrowNativeType>(
        &mut self,
        values: &[T],
        ranges: &mut impl RangeList
    ) -> anyhow::Result<()>
    {
        self.reserve(ranges.size() * size_of::<T>());

        for r in ranges.iter() {
            self.extend_from_slice(&values[r])        
        }
        
        Ok(())
    }
}