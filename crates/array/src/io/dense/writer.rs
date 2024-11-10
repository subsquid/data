use crate::io::writer::IOWriterFactory;
use arrow_buffer::ToByteSlice;
use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;


pub struct DenseWriter<W> {
    inner: Rc<RefCell<WriterInner<W>>>
}


impl<W: Write> DenseWriter<W> {
    pub fn new(write: W) -> Self {
        let inner = WriterInner {
            write,
            buffers: Vec::new()
        };
        Self {
            inner: Rc::new(RefCell::new(inner))
        }
    }

    pub fn new_buffer(&self) -> BufferWriter<W> {
        let mut inner = self.inner.borrow_mut();
        let buffer_index = inner.buffers.len();
        inner.buffers.push(Vec::new());
        BufferWriter {
            write: Rc::clone(&self.inner),
            buf: Vec::new(),
            buffer_index
        }
    }

    pub fn finish(self) -> std::io::Result<W> {
        Rc::into_inner(self.inner)
            .expect("buffer writers still exist")
            .into_inner()
            .finish()
    }
}


struct WriterInner<W> {
    write: W,
    buffers: Vec<Vec<u8>>,
}


impl<W: Write> WriterInner<W> {
    fn finish(mut self) -> std::io::Result<W> {
        let buffer_lengths: Vec<u32> = self.buffers.iter()
            .map(|b| b.len() as u32)
            .collect();

        for buf in self.buffers.into_iter() {
            self.write.write_all(&buf)?;
        }

        self.write.write_all(buffer_lengths.to_byte_slice())?;
        self.write.write_all((buffer_lengths.len() as u32).to_byte_slice())?;
        Ok(self.write)
    }
}


pub struct BufferWriter<W> {
    write: Rc<RefCell<WriterInner<W>>>,
    buf: Vec<u8>,
    buffer_index: usize
}


impl<W> BufferWriter<W> {
    pub fn finish(self) {
        let mut file = self.write.borrow_mut();
        file.buffers[self.buffer_index] = self.buf
    }
}


impl<W: Write> Write for BufferWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.buf.write_all(buf)
    }
}


impl<W: Write> IOWriterFactory for DenseWriter<W> {
    type Write = BufferWriter<W>;

    fn next_write(&mut self) -> anyhow::Result<Self::Write> {
        Ok(self.new_buffer())
    }
} 