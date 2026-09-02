use arrow_buffer::MutableBuffer;

use crate::{
    builder::{bitmask::BitmaskBuilder, nullmask::NullmaskBuilder, offsets::OffsetsBuilder},
    writer::Writer
};

pub struct MemoryWriter;

impl Writer for MemoryWriter {
    type Bitmask = BitmaskBuilder;
    type Nullmask = NullmaskBuilder;
    type Native = MutableBuffer;
    type Offset = OffsetsBuilder;
}
