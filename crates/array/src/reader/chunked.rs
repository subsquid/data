use std::marker::PhantomData;
use crate::reader::ArrayReader;


pub trait ChunkedArrayReader {
    type ArrayReader: ArrayReader;
    
    fn num_chunks(&self) -> usize;
    
    fn chunk(&mut self, i: usize) -> &mut Self::ArrayReader;

    #[inline]
    fn map<'a, R, F>(&'a mut self, f: F) -> impl ChunkedArrayReader<ArrayReader=R> + 'a
    where 
        R: 'a + ArrayReader,
        F: 'a + FnMut(&mut Self::ArrayReader) -> &mut R
    {
        MappedChunkedArrayReader::<'a, Self, F, 1> {
            reader: self,
            map: f
        }
    }
}


struct MappedChunkedArrayReader<'a, R: ?Sized, F, const N: usize> {
    reader: &'a mut R,
    map: F
}


macro_rules! implement_map_level {
    ($level:literal) => {
        impl <'a, T: ?Sized, R, F> ChunkedArrayReader for MappedChunkedArrayReader<'a, T, F, $level>
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

            #[inline]
            fn map<'b, R2, F2>(&'b mut self, f: F2) -> impl ChunkedArrayReader<ArrayReader=R2> + 'b
            where
                R2: 'b + ArrayReader,
                F2: 'b + FnMut(&mut Self::ArrayReader) -> &mut R2,
            {
                MappedChunkedArrayReader::<'b, Self, F2, { $level + 1usize }> {
                    reader: self,
                    map: f
                }
            }
        }
    };
}
implement_map_level!(1);
implement_map_level!(2);
implement_map_level!(3);
implement_map_level!(4);
implement_map_level!(5);
implement_map_level!(6);


impl<'a, T: ?Sized, R, F> ChunkedArrayReader for MappedChunkedArrayReader<'a, T, F, 7>
where
    T: ChunkedArrayReader,
    R: ArrayReader + 'a,
    F: FnMut(&mut T::ArrayReader) -> &mut R,
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

    #[inline]
    fn map<'b, R2, F2>(&'b mut self, _f: F2) -> impl ChunkedArrayReader<ArrayReader=R2> + 'b
    where
        R2: 'b + ArrayReader,
        F2: 'b + FnMut(&mut Self::ArrayReader) -> &mut R2,
    {
        FinalReader::<R2> {
            phantom_data: PhantomData::default()
        }
    }
}


struct FinalReader<T> {
    phantom_data: PhantomData<T>
}


impl <T: ArrayReader> ChunkedArrayReader for FinalReader<T> {
    type ArrayReader = T;

    fn num_chunks(&self) -> usize {
        panic!("array structure is too deep")
    }

    fn chunk(&mut self, _i: usize) -> &mut Self::ArrayReader {
        panic!("array structure is too deep")
    }
    
    fn map<'b, R2, F2>(&'b mut self, _f: F2) -> impl ChunkedArrayReader<ArrayReader=R2> + 'b
    where
        R2: 'b + ArrayReader,
        F2: 'b + FnMut(&mut Self::ArrayReader) -> &mut R2,
    {
        FinalReader::<R2> {
            phantom_data: PhantomData::default()
        }
    }
}


impl<'a, T: ArrayReader> ChunkedArrayReader for Vec<&'a mut T> {
    type ArrayReader = T;

    #[inline]
    fn num_chunks(&self) -> usize {
        self.len()
    }

    #[inline]
    fn chunk(&mut self, i: usize) -> &mut Self::ArrayReader {
        self[i]
    }

    fn map<'b, R2, F2>(&'b mut self, mut f: F2) -> impl ChunkedArrayReader<ArrayReader=R2> + 'b
    where
        R2: 'b + ArrayReader,
        F2: 'b + FnMut(&mut Self::ArrayReader) -> &mut R2,
    {
        self.iter_mut().map(|r| f(*r)).collect::<Vec<_>>()
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