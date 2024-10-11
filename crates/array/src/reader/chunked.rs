use crate::reader::ArrayReader;


pub trait ChunkedArrayReader {
    type ArrayReader: ArrayReader;
    
    fn num_chunks(&self) -> usize;
    
    fn chunk(&mut self, i: usize) -> &mut Self::ArrayReader;
    
    fn map<'a, R, F>(&'a mut self, f: F) -> impl ChunkedArrayReader<ArrayReader=R> + 'a
    where 
        R: 'a + ArrayReader,
        F: 'a + FnMut(&mut Self::ArrayReader) -> &mut R
    {
        MappedChunkedArrayReader {
            reader: self,
            map: f
        }
    }
}


struct MappedChunkedArrayReader<'a, R: ?Sized, F> {
    reader: &'a mut R,
    map: F
}


impl <'a, T: ?Sized, R, F> ChunkedArrayReader for MappedChunkedArrayReader<'a, T, F> 
where
    T: ChunkedArrayReader,
    R: ArrayReader + 'a,
    F: FnMut(&mut T::ArrayReader) -> &mut R
{
    type ArrayReader = R;

    #[inline]
    fn num_chunks(&self) -> usize {
        self.reader.num_chunks()
    }

    #[inline]
    fn chunk(&mut self, i: usize) -> &mut Self::ArrayReader {
        (self.map)(self.reader.chunk(i))
    }
}


impl <T: ArrayReader> ChunkedArrayReader for [T] {
    type ArrayReader = T;

    #[inline]
    fn num_chunks(&self) -> usize {
        self.len()
    }

    #[inline]
    fn chunk(&mut self, i: usize) -> &mut Self::ArrayReader {
        &mut self[i]
    }
}