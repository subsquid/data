use super::bitmask::BitmaskPageWriter;
use super::native::NativePageWriter;
use super::nullmask::NullmaskPageWriter;
use super::offsets::OffsetPageWriter;
use super::page::BufferPageWriter;
use crate::kv::KvWrite;
use crate::table::key::TableKeyFactory;
use arrow_buffer::ArrowNativeType;
use sqd_array::writer::{Writer, WriterFactory};
use std::marker::PhantomData;


pub struct StorageWriter<S> {
    phantom_data: PhantomData<S>
}


impl<S: KvWrite> Writer for StorageWriter<S> {
    type Bitmask = BitmaskPageWriter<BufferPageWriter<S>>;
    type Nullmask = NullmaskPageWriter<BufferPageWriter<S>>;
    type Native = NativePageWriter<BufferPageWriter<S>>;
    type Offset = OffsetPageWriter<BufferPageWriter<S>>;
}


pub(super) struct StorageWriterFactory<S> {
    storage: S,
    key: TableKeyFactory,
    pos: usize
}


impl<S: KvWrite + Clone> StorageWriterFactory<S> {
    pub fn new(storage: S, key: TableKeyFactory) -> Self {
        Self {
            storage,
            key,
            pos: 0
        }
    }

    fn next_buffer(&mut self) -> BufferPageWriter<S> {
        let buf = BufferPageWriter::new(self.storage.clone(), self.key.clone(), self.pos);
        self.pos += 1;
        buf
    }
}


impl <S: KvWrite + Clone> WriterFactory for StorageWriterFactory<S>{
    type Writer = StorageWriter<S>;

    fn nullmask(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Nullmask> {
        let buf = self.next_buffer();
        Ok(NullmaskPageWriter::new(buf))
    }

    fn bitmask(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Bitmask> {
        let buf = self.next_buffer();
        Ok(BitmaskPageWriter::new(buf))
    }

    fn native<T: ArrowNativeType>(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Native> {
        let buf = self.next_buffer();
        Ok(NativePageWriter::new(buf, T::get_byte_width()))
    }

    fn offset(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Offset> {
        let buf = self.next_buffer();
        Ok(OffsetPageWriter::new(buf))
    }
}