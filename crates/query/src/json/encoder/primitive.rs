use arrow::buffer::ScalarBuffer;
use arrow::datatypes::ArrowNativeType;
use lexical_core::FormattedSize;
use crate::json::encoder::Encoder;


pub trait PrimitiveEncode: ArrowNativeType + Send {
    type Buffer: Send;

    // Workaround https://github.com/rust-lang/rust/issues/61415
    fn init_buffer() -> Self::Buffer;

    /// Encode the primitive value as bytes, returning a reference to that slice.
    ///
    /// `buf` is temporary space that may be used
    fn encode(self, buf: &mut Self::Buffer) -> &[u8];
}


macro_rules! integer_encode {
    ($($t:ty),*) => {
        $(
            impl PrimitiveEncode for $t {
                type Buffer = [u8; Self::FORMATTED_SIZE];

                fn init_buffer() -> Self::Buffer {
                    [0; Self::FORMATTED_SIZE]
                }

                fn encode(self, buf: &mut Self::Buffer) -> &[u8] {
                    lexical_core::write(self, buf)
                }
            }
        )*
    };
}
integer_encode!(i8, i16, i32, i64, u8, u16, u32, u64);


macro_rules! float_encode {
    ($($t:ty),*) => {
        $(
            impl PrimitiveEncode for $t {
                type Buffer = [u8; Self::FORMATTED_SIZE];

                fn init_buffer() -> Self::Buffer {
                    [0; Self::FORMATTED_SIZE]
                }

                fn encode(self, buf: &mut Self::Buffer) -> &[u8] {
                    if self.is_infinite() || self.is_nan() {
                        b"null"
                    } else {
                        lexical_core::write(self, buf)
                    }
                }
            }
        )*
    };
}
float_encode!(f32, f64);


pub struct PrimitiveEncoder<N: PrimitiveEncode> {
    values: ScalarBuffer<N>,
    buffer: N::Buffer,
}


impl <N: PrimitiveEncode> PrimitiveEncoder<N> {
    pub fn new(values: ScalarBuffer<N>) -> Self {
        Self {
            values,
            buffer: N::init_buffer()
        }
    }
}


impl<N: PrimitiveEncode> Encoder for PrimitiveEncoder<N> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.extend_from_slice(self.values[idx].encode(&mut self.buffer));
    }
}


pub struct TimestampEncoder {
    values: ScalarBuffer<i64>,
    buffer: [u8; i64::FORMATTED_SIZE],
    scale_multiplier: i64,
    scale_divisor: i64
}


impl TimestampEncoder {
    pub fn new(values: ScalarBuffer<i64>, scale_multiplier: i64, scale_divisor: i64) -> Self {
        Self {
            values,
            buffer: i64::init_buffer(),
            scale_multiplier,
            scale_divisor
        }
    }
}


impl Encoder for TimestampEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let value = self.values[idx] * self.scale_multiplier / self.scale_divisor;
        out.extend_from_slice(value.encode(&mut self.buffer));
    }
}