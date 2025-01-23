use crate::io::file::shared_file::SharedFileRef;
use crate::io::file::ArrayFile;
use crate::io::writer::IOWriter;
use crate::writer::{AnyArrayWriter, ArrayWriter, Writer};
use arrow::datatypes::DataType;
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};


pub type FileWriter = IOWriter<BufWriter<File>>;


pub struct ArrayFileWriter {
    data_type: DataType,
    inner: AnyArrayWriter<FileWriter>
}


impl ArrayFileWriter {
    pub(super) fn new(data_type: DataType, buffers: Vec<SharedFileRef>) -> anyhow::Result<Self> {
        let mut buffers = buffers.into_iter();
        
        let mut factory = move || {
            let shared = buffers.next().expect("no more buffers left");
            let mut file = shared.into_file();
            file.seek(SeekFrom::Start(0))?;
            file.set_len(0)?;
            Ok(BufWriter::new(file))
        };
        
        let inner = AnyArrayWriter::from_factory(&mut factory, &data_type)?;
        
        Ok(Self {
            data_type,
            inner
        })
    }
    
    pub fn finish(self) -> anyhow::Result<ArrayFile> {
        let buffers = self.inner.into_inner().into_iter().map(|buf| {
            let mut buf_writer = IOWriter::finish_any_writer(buf)?;
            buf_writer.flush()?;
            let file = buf_writer.into_inner().expect("buffer was already flushed");
            Ok(SharedFileRef::new(file))
        }).collect::<anyhow::Result<Vec<_>>>()?;
        
        Ok(ArrayFile::new(self.data_type, buffers))
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