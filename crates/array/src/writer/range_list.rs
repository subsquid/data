use std::ops::Range;


pub struct RangesIterable<T> {
    inner: T,
    size: Option<usize>
}


impl <T: Iterator<Item = Range<usize>> + Clone> RangesIterable<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            size: None
        }
    }

    pub fn with_size(inner: T, size: impl Into<Option<usize>>) -> Self {
        Self {
            inner,
            size: size.into()
        }
    }

    #[inline]
    pub fn iter(&self) -> T {
        self.inner.clone()
    }

    pub fn size(&mut self) -> usize {
        if let Some(size) = self.size {
            size
        } else {
            let size = self.iter().map(|r| r.len()).sum();
            self.size = Some(size);
            size
        }
    }
}


pub trait RangeList: Sized {
    fn iter(&self) -> impl Iterator<Item = Range<usize>> + Clone;

    fn size(&mut self) -> usize;

    fn shift(&mut self, offset: usize, len: usize) -> ShiftedRangeList<'_, Self> {
        ShiftedRangeList {
            src: self,
            offset,
            len
        }
    }
}


pub struct ShiftedRangeList<'a, S> {
    src: &'a mut S,
    offset: usize,
    len: usize
}


impl <'a, S: RangeList> RangeList for ShiftedRangeList<'a, S> {
    #[inline]
    fn iter(&self) -> impl Iterator<Item = Range<usize>> + Clone {
        self.src.iter().map(move |r| {
            let beg = self.offset + r.start;
            let end = self.offset + r.end;
            assert!(end <= self.len);
            beg..end
        })
    }

    #[inline]
    fn size(&mut self) -> usize {
        self.src.size()
    }
}


impl <T: Iterator<Item = Range<usize>> + Clone> RangeList for RangesIterable<T> {
    #[inline]
    fn iter(&self) -> impl Iterator<Item = Range<usize>> + Clone {
        self.iter()
    }

    #[inline]
    fn size(&mut self) -> usize {
        self.size()
    }
}