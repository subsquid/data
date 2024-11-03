use arrow_buffer::MutableBuffer;
use crate::builder::bitmask::BitmaskBuilder;
use crate::builder::nullmask::NullmaskBuilder;
use crate::builder::offsets::OffsetsBuilder;
use crate::writer::Writer;


pub struct MemoryWriter;


impl Writer for MemoryWriter {
    type Bitmask = BitmaskBuilder;
    type Nullmask = NullmaskBuilder;
    type Native = MutableBuffer;
    type Offset = OffsetsBuilder;
}