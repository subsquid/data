use arrow::array::ArrowPrimitiveType;
use arrow::datatypes::{DataType, Fields};
use crate::util::{get_num_buffers, invalid_buffer_access};
use crate::visitor::DataTypeVisitor;
use crate::writer::{ArrayWriter, Writer, WriterFactory};


pub enum AnyBufferWriter<W: Writer> {
    Bitmask(W::Bitmask),
    Nullmask(W::Nullmask),
    Native(W::Native),
    Offsets(W::Offset),
}


pub struct AnyArrayWriter<W: Writer> {
    buffers: Vec<AnyBufferWriter<W>>
}


impl <W: Writer> ArrayWriter for AnyArrayWriter<W> {
    type Writer = W;

    fn bitmask(&mut self, buf: usize) -> &mut W::Bitmask {
        match self.buffers.get_mut(buf) {
            Some(AnyBufferWriter::Bitmask(w)) => w,
            _ => invalid_buffer_access!()
        }
    }

    fn nullmask(&mut self, buf: usize) -> &mut W::Nullmask {
        match self.buffers.get_mut(buf) {
            Some(AnyBufferWriter::Nullmask(w)) => w,
            _ => invalid_buffer_access!()
        }
    }

    fn native(&mut self, buf: usize) -> &mut W::Native {
        match self.buffers.get_mut(buf) {
            Some(AnyBufferWriter::Native(w)) => w,
            _ => invalid_buffer_access!()
        }
    }

    fn offset(&mut self, buf: usize) -> &mut W::Offset {
        match self.buffers.get_mut(buf) {
            Some(AnyBufferWriter::Offsets(w)) => w,
            _ => invalid_buffer_access!()
        }
    }
}


impl <W: Writer> AnyArrayWriter<W> {
    pub fn from_factory(
        factory: &mut impl WriterFactory<Writer=W>,
        data_type: &DataType
    ) -> anyhow::Result<Self>
    {
        let mut buffers = Vec::with_capacity(get_num_buffers(data_type));
        
        AnyArrayFactory {
            buffers: &mut buffers,
            writer_factory: factory
        }.visit(data_type)?;
        
        Ok(Self {
            buffers
        })
    }
}


struct AnyArrayFactory<'a, W: Writer, F> {
    buffers: &'a mut Vec<AnyBufferWriter<W>>,
    writer_factory: &'a mut F,
}


impl <'a, W: Writer, F: WriterFactory> DataTypeVisitor for AnyArrayFactory<'a, W, F> {
    type Result = anyhow::Result<()>;

    fn boolean(&mut self) -> Self::Result {
        todo!()
    }

    fn primitive<T: ArrowPrimitiveType>(&mut self) -> Self::Result {
        todo!()
    }

    fn binary(&mut self) -> Self::Result {
        todo!()
    }

    fn list(&mut self, item: &DataType) -> Self::Result {
        todo!()
    }

    fn r#struct(&mut self, fields: &Fields) -> Self::Result {
        todo!()
    }
}