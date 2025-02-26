use arrow::buffer::ScalarBuffer;
use arrow::datatypes::ArrowNativeType;
use crate::json::encoder::Encoder;
use std::io::Write;


pub trait HexEncode: ArrowNativeType + Send {
    type Buffer: Send;

    // Workaround https://github.com/rust-lang/rust/issues/61415
    fn init_buffer() -> Self::Buffer;

    /// Encode the primitive value as bytes, returning a reference to that slice.
    ///
    /// `buf` is temporary space that may be used
    fn encode(self, buf: &mut Self::Buffer) -> &[u8];
}


macro_rules! hex_encode {
    ($($t:ty),*) => {
        $(
            impl HexEncode for $t {
                type Buffer = [u8; 2 + std::mem::size_of::<Self>() * 2];

                fn init_buffer() -> Self::Buffer {
                    [0; 2 + std::mem::size_of::<Self>() * 2]
                }

                fn encode(self, buf: &mut Self::Buffer) -> &[u8] {
                    let mut buf = buf.as_mut();
                    write_hex(&mut buf, &self.to_be_bytes());
                    buf
                }
            }
        )*
    };
}
// Implementation for signed types depends on the implementation of `to_be_bytes`
hex_encode!(u8, u16, u32, u64, u128, i128);


fn write_hex(mut buf: impl Write, bytes: &[u8]) {
    write!(buf, "0x").unwrap();
    for b in bytes {
        write!(buf, "{:02x}", b).unwrap();
    }
}


pub struct HexEncoder<N: HexEncode> {
    values: ScalarBuffer<N>,
    buffer: N::Buffer,
}


impl <N: HexEncode> HexEncoder<N> {
    pub fn new(values: ScalarBuffer<N>) -> Self {
        Self {
            values,
            buffer: N::init_buffer()
        }
    }
}


impl<N: HexEncode> Encoder for HexEncoder<N> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.extend_from_slice(self.values[idx].encode(&mut self.buffer));
    }
}


#[cfg(test)]
mod tests {
    use super::HexEncode;

    #[test]
    fn test_hex_write() {
        let mut buf = u16::init_buffer();
        1600u16.encode(&mut buf);
        assert_eq!(&buf, b"0x0640");

        let mut buf = u64::init_buffer();
        1600u64.encode(&mut buf);
        assert_eq!(&buf, b"0x0000000000000640");
    }
}
