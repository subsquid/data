use crate::io::file::ArrayFile;
use crate::io::writer::{BitmaskIOWriter, NativeIOWriter, NullmaskIOWriter, OffsetsIOWriter};
use crate::writer::{AnyArrayWriter, AnyWriter, ArrayWriter, Writer, WriterFactory};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;


pub struct FileWriter;


impl Writer for FileWriter {
    type Bitmask = BitmaskIOWriter<BufWriter<File>>;
    type Nullmask = NullmaskIOWriter<BufWriter<File>>;
    type Native = NativeIOWriter<BufWriter<File>>;
    type Offset = OffsetsIOWriter<BufWriter<File>>;
}


pub struct ArrayFileWriter<F> {
    file: ArrayFile<F>,
    writer: AnyArrayWriter<FileWriter>
}


impl <F: AsRef<Path>> ArrayFileWriter<F> {
    pub fn new(file: ArrayFile<F>) -> anyhow::Result<Self> {
        let mut factory = FileWriterFactory {
            buffers: &file.buffers,
            pos: 0
        };
        let writer = AnyArrayWriter::from_factory(&mut factory, &file.data_type)?;
        Ok(Self {
            file,
            writer
        })
    }
    
    pub fn finish(self) -> anyhow::Result<ArrayFile<F>> {
        for buf in self.writer.into_inner() {
            let mut file = match buf {
                AnyWriter::Bitmask(w) => w.finish()?,
                AnyWriter::Nullmask(w) => w.finish()?,
                AnyWriter::Native(w) => w.into_write(),
                AnyWriter::Offsets(w) => w.finish()?,
            };
            file.flush()?;
        }
        Ok(self.file)
    }
}


struct FileWriterFactory<'a, F> {
    buffers: &'a [F],
    pos: usize
}


impl <'a, F: AsRef<Path>> FileWriterFactory<'a, F> {
    fn next_file(&mut self) -> anyhow::Result<BufWriter<File>> {
        let file = File::options().write(true).open(&self.buffers[self.pos])?;
        self.pos += 1;
        Ok(BufWriter::new(file))
    }
}


impl <'a, F: AsRef<Path>> WriterFactory for FileWriterFactory<'a, F> {
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


impl <F> ArrayWriter for ArrayFileWriter<F> {
    type Writer = FileWriter;

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