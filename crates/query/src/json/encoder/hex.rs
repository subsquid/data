use crate::json::encoder::Encoder;
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::ArrowNativeType;


static TABLE: &[u8] = b"0123456789abcdef";


pub trait HexEncode: ArrowNativeType + Send {
    type Buffer: Send + AsRef<[u8]>;

    fn init_buffer() -> Self::Buffer;

    fn encode(self, buf: &mut Self::Buffer);
}


macro_rules! hex_encode {
    ($($t:ty),*) => {
        $(
            impl HexEncode for $t {
                type Buffer = [u8; 4 + std::mem::size_of::<Self>() * 2];

                fn init_buffer() -> Self::Buffer {
                    let mut buf = [0; 4 + std::mem::size_of::<Self>() * 2];
                    buf[0] = b'"';
                    buf[1] = b'0';
                    buf[2] = b'x';
                    buf[buf.len() - 1] = b'"';
                    buf
                }

                fn encode(self, buf: &mut Self::Buffer) {
                    let bytes = self.to_le_bytes();
                    let size = bytes.len();
                    for i in 0..size {
                        let b = bytes[size - 1 - i];
                        let pos = 3 + i * 2;
                        buf[pos] = TABLE[((b >> 4) & 0xf) as usize];
                        buf[pos + 1] = TABLE[(b & 0xf) as usize];
                    }
                }
            }
        )*
    };
}
hex_encode!(u8, u16, u32, u64, u128);


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
        self.values[idx].encode(&mut self.buffer);
        out.extend_from_slice(self.buffer.as_ref());
    }
}


#[cfg(test)]
mod tests {
    use super::HexEncode;


    #[test]
    fn test_hex_write() {
        let mut buf = u16::init_buffer();
        1600u16.encode(&mut buf);
        assert_eq!(&buf, b"\"0x0640\"");

        let mut buf = u64::init_buffer();
        1600u64.encode(&mut buf);
        assert_eq!(&buf, b"\"0x0000000000000640\"");
    }
}
