use crate::util::invalid_buffer_access;
use crate::writer::{ArrayWriter, Writer};


enum AnyBuffer<W: Writer> {
    Bitmask(W::Bitmask),
    Nullmask(W::Nullmask),
    Native(W::Native),
    Offsets(W::Offset),
}


pub struct AnyArrayWriter<W: Writer> {
    buffers: Vec<AnyBuffer<W>>
}


impl <W: Writer> ArrayWriter for AnyArrayWriter<W> {
    type Writer = W;

    fn bitmask(&mut self, buf: usize) -> &mut W::Bitmask {
        match self.buffers.get_mut(buf) {
            Some(AnyBuffer::Bitmask(w)) => w,
            _ => invalid_buffer_access!()
        }
    }

    fn nullmask(&mut self, buf: usize) -> &mut W::Nullmask {
        match self.buffers.get_mut(buf) {
            Some(AnyBuffer::Nullmask(w)) => w,
            _ => invalid_buffer_access!()
        }
    }

    fn native(&mut self, buf: usize) -> &mut W::Native {
        match self.buffers.get_mut(buf) {
            Some(AnyBuffer::Native(w)) => w,
            _ => invalid_buffer_access!()
        }
    }

    fn offset(&mut self, buf: usize) -> &mut W::Offset {
        match self.buffers.get_mut(buf) {
            Some(AnyBuffer::Offsets(w)) => w,
            _ => invalid_buffer_access!()
        }
    }
}