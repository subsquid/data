use crate::io::file::shared_file::SharedFileRef;
use crate::io::file::ArrayFile;
use crate::io::writer::{BitmaskIOWriter, NativeIOWriter, NullmaskIOWriter, OffsetsIOWriter};
use crate::writer::{AnyArrayWriter, AnyWriter, ArrayWriter, Writer, WriterFactory};
use arrow::datatypes::DataType;
use std::fs::File;
use std::io::{BufWriter, Write};


pub struct FileWriter;


impl Writer for FileWriter {
    type Bitmask = BitmaskIOWriter<BufWriter<File>>;
    type Nullmask = NullmaskIOWriter<BufWriter<File>>;
    type Native = NativeIOWriter<BufWriter<File>>;
    type Offset = OffsetsIOWriter<BufWriter<File>>;
}


pub struct ArrayFileWriter {
    data_type: DataType,
    inner: AnyArrayWriter<FileWriter>
}


impl ArrayFileWriter {
    pub(super) fn new(data_type: DataType, buffers: Vec<SharedFileRef>) -> anyhow::Result<Self> {
        let mut factory = FileWriterFactory {
            buffers: buffers.into_iter()
        };
        
        let inner = AnyArrayWriter::from_factory(&mut factory, &data_type)?;
        
        Ok(Self {
            data_type,
            inner
        })
    }
    
    pub fn finish(self) -> anyhow::Result<ArrayFile> {
        let buffers = self.inner.into_inner().into_iter().map(|buf| {
            let mut buf_writer = match buf {
                AnyWriter::Bitmask(w) => w.finish()?,
                AnyWriter::Nullmask(w) => w.finish()?,
                AnyWriter::Native(w) => w.into_write(),
                AnyWriter::Offsets(w) => w.finish()?,
            };
            buf_writer.flush()?;
            let file = buf_writer.into_inner().expect("buffer was already flushed");
            Ok(SharedFileRef::new(file))
        }).collect::<anyhow::Result<Vec<_>>>()?;
        
        Ok(ArrayFile::new(self.data_type, buffers))
    }
}


struct FileWriterFactory<I> {
    buffers: I
}


impl<I: Iterator<Item=SharedFileRef>> FileWriterFactory<I> {
    fn next_file(&mut self) -> anyhow::Result<BufWriter<File>> {
        let shared = self.buffers.next().expect("no more buffers left");
        let file = shared.into_file();
        file.set_len(0)?;
        Ok(BufWriter::new(file))
    }
}


impl<I: Iterator<Item=SharedFileRef>> WriterFactory for FileWriterFactory<I> {
    type Writer = FileWriter;

    fn nullmask(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Nullmask> {
        let file = self.next_file()?;
        Ok(NullmaskIOWriter::new(file))
    }

    fn bitmask(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Bitmask> {
        let file = self.next_file()?;
        Ok(BitmaskIOWriter::new(file))
    }

    fn native(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Native> {
        let file = self.next_file()?;
        Ok(NativeIOWriter::new(file))
    }

    fn offset(&mut self) -> anyhow::Result<<Self::Writer as Writer>::Offset> {
        let file = self.next_file()?;
        Ok(OffsetsIOWriter::new(file))
    }
}


impl ArrayWriter for ArrayFileWriter {
    type Writer = FileWriter;

    #[inline]
    fn bitmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Bitmask {
        self.inner.bitmask(buf)
    }

    #[inline]
    fn nullmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Nullmask {
        self.inner.nullmask(buf)
    }

    #[inline]
    fn native(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Native {
        self.inner.native(buf)
    }

    #[inline]
    fn offset(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Offset {
        self.inner.offset(buf)
    }
}