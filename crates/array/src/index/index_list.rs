pub trait IndexList {
    fn index_iter(&self) -> impl Iterator<Item=usize> + Clone;
    
    fn len_hint(&self) -> usize;
    
    fn shift(&self, offset: usize, len: usize) -> impl IndexList {
        ShiftedIndexList {
            inner: self,
            offset,
            len
        }
    }
}


struct ShiftedIndexList<'a, T: ?Sized> {
    inner: &'a T,
    offset: usize,
    len: usize
}


impl <'a, T: IndexList + ?Sized> IndexList for ShiftedIndexList<'a, T> {
    fn index_iter(&self) -> impl Iterator<Item=usize> + Clone {
        self.inner.index_iter().map(|i| {
            assert!(i < self.len);
            self.offset + i
        })
    }

    #[inline]
    fn len_hint(&self) -> usize {
        self.inner.len_hint()
    }

    fn shift(&self, offset: usize, len: usize) -> impl IndexList {
        assert!(offset + len <= self.len);
        ShiftedIndexList {
            inner: self.inner,
            offset: self.offset + offset,
            len
        }
    }
}


impl <T: Iterator<Item=usize> + Clone> IndexList for T {
    #[inline]
    fn index_iter(&self) -> impl Iterator<Item=usize> + Clone {
        self.clone()
    }

    #[inline]
    fn len_hint(&self) -> usize {
        Iterator::size_hint(self).0
    }
}


impl IndexList for [usize] {
    #[inline]
    fn index_iter(&self) -> impl Iterator<Item=usize> + Clone {
        self.iter().copied()
    }

    #[inline]
    fn len_hint(&self) -> usize {
        self.len()
    }
}


impl IndexList for [u32] {
    #[inline]
    fn index_iter(&self) -> impl Iterator<Item=usize> + Clone {
        self.iter().map(|i| *i as usize)
    }

    #[inline]
    fn len_hint(&self) -> usize {
        self.len()
    }
}