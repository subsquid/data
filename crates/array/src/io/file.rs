use crate::io::{BitmaskIOWriter, NativeIOWriter, NullmaskIOWriter, OffsetsIOWriter};
use crate::writer::{AnyArrayWriter, AnyWriter, Writer, WriterFactory};
use arrow::datatypes::DataType;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::Path;
use crate::util::get_num_buffers;


pub struct FileWriter<'a> {
    phantom_data: PhantomData<&'a ()>
}


impl <'a> Writer for FileWriter<'a> {
    type Bitmask = BitmaskIOWriter<BufWriter<File>>;
    type Nullmask = NullmaskIOWriter<BufWriter<File>>;
    type Native = NativeIOWriter<BufWriter<File>>;
    type Offset = OffsetsIOWriter<BufWriter<File>>;
}


pub type ArrowFileWriter<'a> = AnyArrayWriter<FileWriter<'a>>;


pub struct ArrowFile<F> {
    data_type: DataType,
    buffers: Vec<F>
}


impl <F: AsRef<Path>> ArrowFile<F> {
    pub fn write(&mut self) -> anyhow::Result<ArrowFileWriter<'_>> {
        let mut factory = FileWriterFactory {
            buffers: &self.buffers,
            pos: 0
        };
        AnyArrayWriter::from_factory(&mut factory, &self.data_type)
    }
}


struct FileWriterFactory<'a, F> {
    buffers: &'a [F],
    pos: usize
}


impl <'a, F: AsRef<Path>> FileWriterFactory<'a, F> {
    fn next_file(&mut self) -> anyhow::Result<BufWriter<File>> {
        let file = File::open(&self.buffers[self.pos])?;
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


impl<'a> ArrowFileWriter<'a> {
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


#[cfg(feature = "tempfile")]
impl ArrowFile<tempfile::NamedTempFile> {
    pub fn new_temporary(data_type: DataType) -> anyhow::Result<Self> {
        let buffers = std::iter::repeat_with(tempfile::NamedTempFile::new)
            .take(get_num_buffers(&data_type))
            .collect::<Result<Vec<_>, _>>()?;
        
        Ok(Self {
            data_type,
            buffers
        })
    }
}