use crate::writer::Writer;


enum AnyBuffer<W: Writer> {
    Bitmask(W::Bitmask),
    Nullmask(W::Nullmask),
    Native(W::Native),
    Offsets(W::Offset),
}


pub struct AnyArrayWriter<W: Writer> {
    buffers: Vec<AnyBuffer<W>>
}