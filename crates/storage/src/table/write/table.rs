use super::storage_writer::{StorageWriter, StorageWriterFactory};
use crate::kv::KvWrite;
use crate::table::key::TableKeyFactory;
use arrow::datatypes::SchemaRef;
use arrow::ipc::convert::IpcSchemaEncoder;
use sqd_array::writer::{AnyArrayWriter, AnyWriter, ArrayWriter, Writer};


pub struct TableWriter<S: KvWrite> {
    storage: S,
    schema: SchemaRef,
    key: TableKeyFactory,
    writer: AnyArrayWriter<StorageWriter<S>>
}


impl<S: KvWrite + Clone> TableWriter<S> {
    pub fn new(storage: S, table_name: &[u8], schema: SchemaRef) -> Self {
        let key = TableKeyFactory::new(table_name);
        let mut factory = StorageWriterFactory::new(storage.clone(), key.clone());
        let writer = AnyArrayWriter::table_writer_from_factory(&mut factory, &schema).unwrap();
        Self {
            storage,
            schema,
            key,
            writer
        }
    }
}


impl<S: KvWrite> TableWriter<S>  {
    pub fn finish(mut self) -> anyhow::Result<S> {
        for buf in self.writer.into_inner() {
            match buf {
                AnyWriter::Bitmask(writer) => writer.finish(),
                AnyWriter::Nullmask(writer) => writer.finish(),
                AnyWriter::Native(writer) => writer.finish(),
                AnyWriter::Offsets(writer) => writer.finish(),
            }?.finish()?;
        }

        self.storage.put(
            self.key.schema(),
            IpcSchemaEncoder::new().schema_to_fb(&self.schema).finished_data()
        )?;
        
        Ok(self.storage)
    }
}


impl<S: KvWrite> ArrayWriter for TableWriter<S> {
    type Writer = StorageWriter<S>;

    #[inline]
    fn bitmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Bitmask {
        self.writer.bitmask(buf)
    }

    #[inline]
    fn nullmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Nullmask {
        self.writer.nullmask(buf)
    }

    #[inline]
    fn native(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Native {
        self.writer.native(buf)
    }

    #[inline]
    fn offset(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Offset {
        self.writer.offset(buf)
    }
}