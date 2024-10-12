use crate::io::writer::{BitmaskIOWriter, NativeIOWriter, NullmaskIOWriter, OffsetsIOWriter};
use crate::writer::{AnyArrayWriter, AnyWriter, Writer, WriterFactory};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::Path;


pub struct FileWriter<'a> {
    phantom_data: PhantomData<&'a ()>
}


impl <'a> Writer for FileWriter<'a> {
    type Bitmask = BitmaskIOWriter<BufWriter<File>>;
    type Nullmask = NullmaskIOWriter<BufWriter<File>>;
    type Native = NativeIOWriter<BufWriter<File>>;
    type Offset = OffsetsIOWriter<BufWriter<File>>;
}


pub type ArrayFileWriter<'a> = AnyArrayWriter<FileWriter<'a>>;


pub(super) struct FileWriterFactory<'a, F> {
    buffers: &'a [F],
    pos: usize
}


impl <'a, F: AsRef<Path>> FileWriterFactory<'a, F> {
    pub fn new(buffers: &'a [F]) -> Self {
        Self { 
            buffers, 
            pos: 0 
        }
    }
    
    fn next_file(&mut self) -> anyhow::Result<BufWriter<File>> {
        let file = File::options().write(true).open(&self.buffers[self.pos])?;
        self.pos += 1;
        Ok(BufWriter::new(file))
    }
}


impl <'a, F: AsRef<Path>> WriterFactory for FileWriterFactory<'a, F> {
    type Writer = FileWriter<'a>;

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


impl<'a> ArrayFileWriter<'a> {
    pub fn finish(self) -> anyhow::Result<()> {
        for buf in self.into_inner() {
            let mut file = match buf {
                AnyWriter::Bitmask(w) => w.finish()?,
                AnyWriter::Nullmask(w) => w.finish()?,
                AnyWriter::Native(w) => w.into_inner(),
                AnyWriter::Offsets(w) => w.finish()?,
            };
            file.flush()?;
        }
        Ok(())
    }
}