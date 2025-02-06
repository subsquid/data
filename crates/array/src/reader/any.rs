use crate::chunking::ChunkRange;
use crate::reader::native::{ChunkedNativeArrayReader, NativeArrayReader};
use crate::reader::{ArrayReader, BinaryReader, BooleanReader, ChunkedArrayReader, ChunkedBinaryReader, ChunkedBooleanReader, ChunkedListReader, ChunkedPrimitiveReader, ChunkedStructReader, ListReader, PrimitiveReader, Reader, ReaderFactory, StructReader};
use crate::visitor::DataTypeVisitor;
use crate::writer::ArrayWriter;
use arrow::array::ArrowPrimitiveType;
use arrow::datatypes::{DataType, FieldRef};
use std::marker::PhantomData;


pub type AnyListReader<R> = ListReader<R, AnyReader<R>>;


pub enum AnyReader<R: Reader> {
    Boolean(BooleanReader<R>),
    Primitive(PrimitiveReader<R>),
    Binary(BinaryReader<R>),
    List(Box<AnyListReader<R>>),
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

    #[inline]
    pub fn as_primitive(&mut self) -> &mut PrimitiveReader<R> {
        match self {
            AnyReader::Primitive(r) => r,
            _ => panic!("not a PrimitiveReader")
        }
    }

    #[inline]
    pub fn as_binary(&mut self) -> &mut BinaryReader<R> {
        match self {
            AnyReader::Binary(r) => r,
            _ => panic!("not a BinaryReader")
        }
    }

    #[inline]
    pub fn as_list(&mut self) -> &mut AnyListReader<R> {
        match self {
            AnyReader::List(r) => r,
            _ => panic!("not a AnyListReader")
        }
    }

    #[inline]
    pub fn as_struct(&mut self) -> &mut StructReader<R> {
        match self {
            AnyReader::Struct(r) => r,
            _ => panic!("not a StructReader")
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


impl <R: Reader> From<AnyListReader<R>> for AnyReader<R> {
    fn from(value: AnyListReader<R>) -> Self {
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
        let reader = BooleanReader::try_new(nulls, values)?;
        Ok(reader.into())
    }

    fn primitive<T: ArrowPrimitiveType>(&mut self) -> Self::Result {
        let nulls = self.factory.nullmask()?;
        let values = self.factory.native::<T::Native>()?;
        let reader = PrimitiveReader::try_new(nulls, values)?;
        Ok(reader.into())
    }

    fn binary(&mut self) -> Self::Result {
        let nulls = self.factory.nullmask()?;
        let offsets = self.factory.offset()?;
        let values = self.factory.native::<u8>()?;
        let reader = BinaryReader::try_new(
            nulls, 
            offsets, 
            NativeArrayReader::new(values)
        )?;
        Ok(reader.into())
    }

    fn list(&mut self, item: &DataType) -> Self::Result {
        let nulls = self.factory.nullmask()?;
        let offsets = self.factory.offset()?;
        let values = self.visit(item)?;
        let reader = ListReader::try_new(
            nulls,
            offsets,
            values
        )?;
        Ok(reader.into())
    }

    fn r#struct(&mut self, fields: &[FieldRef]) -> Self::Result {
        let nulls = self.factory.nullmask()?;
        
        let columns = fields.iter()
            .map(|f| self.visit(f.data_type()))
            .collect::<anyhow::Result<Vec<_>>>()?;
        
        let reader = StructReader::try_new(nulls, columns)?;
        
        Ok(reader.into())
    }
}


pub type AnyChunkedListReader<R> = ChunkedListReader<R, AnyChunkedReader<R>>;


pub enum AnyChunkedReader<R: Reader> {
    Boolean(ChunkedBooleanReader<R>),
    Primitive(ChunkedPrimitiveReader<R>),
    Binary(ChunkedBinaryReader<R>),
    List(Box<AnyChunkedListReader<R>>),
    Struct(ChunkedStructReader<R>)
}


impl<R: Reader> AnyChunkedReader<R> {
    pub fn new(data_type: &DataType) -> Self {
        Self::with_capacity(0, data_type)
    }
    
    pub fn with_capacity(cap: usize, data_type: &DataType) -> Self {
        AnyChunkedReaderFactory {
            cap,
            phantom_data: PhantomData::<R>::default()
        }.visit(data_type)
    }
}


impl<R: Reader> ChunkedArrayReader for AnyChunkedReader<R> {
    type Chunk = AnyReader<R>;

    fn num_buffers(&self) -> usize {
        match self {
            AnyChunkedReader::Boolean(r) => r.num_buffers(),
            AnyChunkedReader::Primitive(r) => r.num_buffers(),
            AnyChunkedReader::Binary(r) => r.num_buffers(),
            AnyChunkedReader::List(r) => r.num_buffers(),
            AnyChunkedReader::Struct(r) => r.num_buffers(),
        }
    }

    fn push(&mut self, chunk: Self::Chunk) {
        match (self, chunk) {
            (AnyChunkedReader::Boolean(c), AnyReader::Boolean(r)) => c.push(r),
            (AnyChunkedReader::Primitive(c), AnyReader::Primitive(r)) => c.push(r),
            (AnyChunkedReader::Binary(c), AnyReader::Binary(r)) => c.push(r),
            (AnyChunkedReader::List(c), AnyReader::List(r)) => c.push(*r),
            (AnyChunkedReader::Struct(c), AnyReader::Struct(r)) => c.push(r),
            _ => panic!("array type mismatch")
        }
    }

    fn read_chunked_ranges(
        &mut self,
        dst: &mut impl ArrayWriter,
        ranges: impl Iterator<Item=ChunkRange> + Clone
    ) -> anyhow::Result<()>
    {
        match self {
            AnyChunkedReader::Boolean(r) => r.read_chunked_ranges(dst, ranges),
            AnyChunkedReader::Primitive(r) => r.read_chunked_ranges(dst, ranges),
            AnyChunkedReader::Binary(r) => r.read_chunked_ranges(dst, ranges),
            AnyChunkedReader::List(r) => r.read_chunked_ranges(dst, ranges),
            AnyChunkedReader::Struct(r) => r.read_chunked_ranges(dst, ranges),
        }
    }
}


struct AnyChunkedReaderFactory<R> {
    cap: usize,
    phantom_data: PhantomData<R>
}


impl<R: Reader> DataTypeVisitor for AnyChunkedReaderFactory<R> {
    type Result = AnyChunkedReader<R>;

    fn boolean(&mut self) -> Self::Result {
        AnyChunkedReader::Boolean(ChunkedBooleanReader::with_capacity(self.cap))
    }

    fn primitive<T: ArrowPrimitiveType>(&mut self) -> Self::Result {
        AnyChunkedReader::Primitive(ChunkedPrimitiveReader::with_capacity(self.cap))
    }

    fn binary(&mut self) -> Self::Result {
        AnyChunkedReader::Binary(ChunkedBinaryReader::new(
            self.cap,
            ChunkedNativeArrayReader::with_capacity(self.cap)
        ))
    }

    fn list(&mut self, item: &DataType) -> Self::Result {
        AnyChunkedReader::List(Box::new(
            AnyChunkedListReader::new(self.cap, self.visit(item))
        ))
    }

    fn r#struct(&mut self, fields: &[FieldRef]) -> Self::Result {
        let columns = fields.iter().map(|f| self.visit(f.data_type())).collect();
        let reader = ChunkedStructReader::new(self.cap, columns);
        AnyChunkedReader::Struct(reader)
    }
}