use arrow_buffer::{ArrowNativeType, MutableBuffer, ToByteSlice};
use crate::data_builder::NativeWriter;


impl NativeWriter for MutableBuffer {
    fn write_slice<T: ArrowNativeType>(&mut self, values: &[T]) {
        self.extend_from_slice(values)
    }

    fn write<T: ToByteSlice>(&mut self, value: T) {
        self.push(value)
    }
}