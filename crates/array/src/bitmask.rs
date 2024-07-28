use arrow_buffer::{bit_mask, bit_util, BooleanBuffer, BooleanBufferBuilder};

use crate::types::{Builder, Slice};
use crate::util::encode_len;


#[derive(Clone)]
pub struct BitSlice<'a> {
    buf: &'a [u8],
    offset: usize,
    len: usize
}


impl <'a> Slice<'a> for BitSlice<'a> {
    fn read_page(_bytes: &'a [u8]) -> anyhow::Result<Self> {
        todo!()
    }

    fn write_page(&self, buf: &mut Vec<u8>) {
        let size = encode_len(self.len);
        let data_len = bit_util::ceil(self.len, 8);

        buf.reserve(size.len() + data_len);
        buf.extend_from_slice(&size);

        let offset = buf.len();
        buf.extend(std::iter::repeat(0).take(data_len));
        bit_mask::set_bits(
            buf.as_mut_slice(),
            &self.buf,
            offset * 8,
            self.offset,
            self.len
        );
    }

    fn len(&self) -> usize {
        self.len
    }

    fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(offset + len <= self.len);
        Self {
            buf: self.buf,
            offset: self.offset + offset,
            len
        }
    }
}


impl Builder for BooleanBufferBuilder {
    type Slice<'a> = BitSlice<'a>;

    fn push_slice(&mut self, slice: &Self::Slice<'_>) {
        let beg = slice.offset;
        let end = slice.offset + slice.len;
        self.append_packed_range(beg..end, slice.buf)
    }

    fn as_slice(&self) -> Self::Slice<'_> {
        BitSlice {
            buf: self.as_slice(),
            offset: 0,
            len: self.len()
        }
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn capacity(&self) -> usize {
        self.capacity()
    }
}


pub fn push_null_mask<'a>(
    mask_len: usize,
    mask: &'a Option<BitSlice<'a>>,
    builder_cap: usize,
    builder: &mut Option<BooleanBufferBuilder>
) {
    match (mask.as_ref(), builder.as_mut()) {
        (Some(m), Some(b)) => {
            b.push_slice(m)
        },
        (Some(m), None) => {
            let mut b = BooleanBufferBuilder::new(std::cmp::max(builder_cap, mask_len));
            b.append_n(mask_len, true);
            b.push_slice(m);
            *builder = Some(b)
        },
        (None, Some(b)) => {
            b.append_n(mask_len, true)
        },
        (None, None) => {}
    }
}


pub fn write_null_mask(
    nulls: &Option<BitSlice<'_>>,
    buf: &mut Vec<u8>
) {
    if let Some(mask) = nulls.as_ref() {
        mask.write_page(buf)
    } else {
        buf.extend_from_slice(&encode_len(0))
    }
}


impl BitSlice<'static> {
    pub fn empty() -> Self {
        Self {
            buf: &[],
            offset: 0,
            len: 0
        }
    }
}


impl<'a> From<&'a BooleanBuffer> for BitSlice<'a> {
    fn from(value: &'a BooleanBuffer) -> Self {
        Self {
            buf: value.values(),
            offset: value.offset(),
            len: value.len()
        }
    }
}