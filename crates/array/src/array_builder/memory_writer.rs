use arrow_buffer::MutableBuffer;
use crate::array_builder::bitmask::BitmaskBuilder;
use crate::array_builder::nullmask::NullmaskBuilder;
use crate::array_builder::offsets::OffsetsBuilder;
use crate::writer::Writer;


pub struct MemoryWriter;


impl Writer for MemoryWriter {
    type Bitmask = BitmaskBuilder;
    type Nullmask = NullmaskBuilder;
    type Native = MutableBuffer;
    type Offset = OffsetsBuilder;
}