use crate::reader::list::ListReader;
use crate::reader::native::NativeReader;
use crate::reader::primitive::PrimitiveReader;
use crate::reader::r#struct::StructReader;
use crate::reader::{ArrowReader, BooleanReader, ByteReader};
use crate::visitor::DataTypeVisitor;
use crate::writer::ArrayWriter;
use arrow::array::ArrowPrimitiveType;
use arrow::datatypes::{DataType, FieldRef};


pub enum AnyReader<R> {
    Boolean(BooleanReader<R>),
    Primitive(PrimitiveReader<R>),
    Binary(ListReader<R, NativeReader<R>>),
    List(Box<ListReader<R, AnyReader<R>>>),
    Struct(StructReader<R>)
}


impl <R: ByteReader> ArrowReader for AnyReader<R> {
    fn num_buffers(&self) -> usize {
        match self {
            AnyReader::Boolean(r) => r.num_buffers(),
            AnyReader::Primitive(r) => r.num_buffers(),
            AnyReader::Binary(r) => r.num_buffers(),
            AnyReader::List(r) => r.num_buffers(),
            AnyReader::Struct(r) => r.num_buffers(),
        }
    }

    fn len(&self) -> usize {
        match self {
            AnyReader::Boolean(r) => r.len(),
            AnyReader::Primitive(r) => r.len(),
            AnyReader::Binary(r) => r.len(),
            AnyReader::List(r) => r.len(),
            AnyReader::Struct(r) => r.len(),
        }
    }

    fn read_slice(&mut self, dst: &mut impl ArrayWriter, offset: usize, len: usize) -> anyhow::Result<()> {
        match self {
            AnyReader::Boolean(r) => r.read_slice(dst, offset, len),
            AnyReader::Primitive(r) => r.read_slice(dst, offset, len),
            AnyReader::Binary(r) => r.read_slice(dst, offset, len),
            AnyReader::List(r) => r.read_slice(dst, offset, len),
            AnyReader::Struct(r) => r.read_slice(dst, offset, len),
        }
    }
}


impl <R: ByteReader> AnyReader<R> {
    pub fn from_byte_reader_factory(
        data_type: &DataType,
        next_reader: impl FnMut() -> anyhow::Result<R>,
    ) -> anyhow::Result<Self> 
    {
        AnyReaderFactory { next_reader }.visit(data_type)
    }
}


struct AnyReaderFactory<F> {
    next_reader: F
}


impl <'a, B: ByteReader, F: FnMut() -> anyhow::Result<B>> DataTypeVisitor for AnyReaderFactory<F> {
    type Result = anyhow::Result<AnyReader<B>>;

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

    fn r#struct(&mut self, fields: &[FieldRef]) -> Self::Result {
        todo!()
    }
}