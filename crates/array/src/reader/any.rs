use crate::chunking::ChunkRange;
use crate::reader::chunked::ChunkedArrayReader;
use crate::reader::list::ListReader;
use crate::reader::primitive::PrimitiveReader;
use crate::reader::r#struct::StructReader;
use crate::reader::{ArrayReader, BinaryReader, BitmaskReader, BooleanReader, NativeReader, Reader, ReaderFactory};
use crate::visitor::DataTypeVisitor;
use crate::writer::ArrayWriter;
use anyhow::ensure;
use arrow::array::ArrowPrimitiveType;
use arrow::datatypes::{DataType, FieldRef};


pub type AnyList<R> = ListReader<R, AnyReader<R>>;


pub enum AnyReader<R: Reader> {
    Boolean(BooleanReader<R>),
    Primitive(PrimitiveReader<R>),
    Binary(BinaryReader<R>),
    List(Box<AnyList<R>>),
    Struct(StructReader<R>)
}


impl <R: Reader> AnyReader<R> {
    pub fn from_factory(
        factory: &mut impl ReaderFactory<Reader=R>,
        data_type: &DataType
    ) -> anyhow::Result<Self>
    {
        AnyReaderFactory {
            factory
        }.visit(data_type)
    }
    
    #[inline]
    pub fn as_boolean(&mut self) -> &mut BooleanReader<R> {
        match self {
            AnyReader::Boolean(r) => r,
            _ => panic!("not a BooleanReader")
        }
    }
}


impl <R: Reader> ArrayReader for AnyReader<R> {
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

    fn read_chunk_ranges(
        chunks: &mut impl ChunkedArrayReader<ArrayReader=Self>,
        dst: &mut impl ArrayWriter,
        mut ranges: impl Iterator<Item=ChunkRange> + Clone
    ) -> anyhow::Result<()>
    {
        if chunks.num_chunks() == 0 {
            if ranges.next().is_some() {
                panic!("attempt to extract a range from an empty chunked array")
            } else {
                return Ok(())
            }
        }

        match chunks.chunk(0) {
            AnyReader::Boolean(_) => {
                BooleanReader::read_chunk_ranges(
                    &mut chunks.map(|c| c.as_boolean()),
                    dst,
                    ranges
                )?;
            },
            AnyReader::Primitive(_) => {
                todo!()
            },
            AnyReader::Binary(_) => {
                todo!()
            },
            AnyReader::List(_) => {
                todo!()
            },
            AnyReader::Struct(_) => {
                todo!()
            }
        }

        Ok(())
    }
}


impl <R: Reader> From<BooleanReader<R>> for AnyReader<R> {
    fn from(value: BooleanReader<R>) -> Self {
        AnyReader::Boolean(value)
    }
}


impl <R: Reader> From<PrimitiveReader<R>> for AnyReader<R> {
    fn from(value: PrimitiveReader<R>) -> Self {
        AnyReader::Primitive(value)
    }
}


impl <R: Reader> From<BinaryReader<R>> for AnyReader<R> {
    fn from(value: BinaryReader<R>) -> Self {
        AnyReader::Binary(value)
    }
}


impl <R: Reader> From<AnyList<R>> for AnyReader<R> {
    fn from(value: AnyList<R>) -> Self {
        AnyReader::List(Box::new(value))
    }
}


impl <R: Reader> From<StructReader<R>> for AnyReader<R> {
    fn from(value: StructReader<R>) -> Self {
        AnyReader::Struct(value)
    }
}


struct AnyReaderFactory<'a, F> {
    factory: &'a mut F,
}


impl <'a, F: ReaderFactory> DataTypeVisitor for AnyReaderFactory<'a, F> {
    type Result = anyhow::Result<AnyReader<F::Reader>>;

    fn boolean(&mut self) -> Self::Result {
        let nulls = self.factory.nullmask()?;
        let values = self.factory.bitmask()?;
        ensure!(
            nulls.len() == values.len(), 
            "nulls and values of a boolean array have different lengths"
        );
        Ok(
            BooleanReader::new(nulls, values).into()
        )
    }

    fn primitive<T: ArrowPrimitiveType>(&mut self) -> Self::Result {
        let nulls = self.factory.nullmask()?;
        let values = self.factory.native::<T::Native>()?;
        ensure!(
            nulls.len() == values.len(), 
            "nulls and values of a primitive array have different lengths"
        );
        Ok(
            PrimitiveReader::new(nulls, values).into()
        )
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