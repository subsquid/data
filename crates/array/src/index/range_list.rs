use crate::offsets::Offsets;
use std::ops::Range;


pub trait RangeList {
    fn iter(&self) -> impl Iterator<Item=Range<usize>> + Clone;

    fn span(&mut self) -> usize;

    #[inline]
    fn shift(&mut self, offset: usize, len: usize) -> impl RangeList {
        Shifted::<_, false> {
            src: self,
            offset,
            len
        }
    }

    #[inline]
    fn list_items<'a>(&'a self, offsets: Offsets<'a>) -> impl RangeList + 'a {
        ListItems {
            inner: self.iter(),
            offsets,
            span: None
        }
    }
}


struct Shifted<'a, S: ?Sized, const IS_LIST: bool> {
    src: &'a mut S,
    offset: usize,
    len: usize
}


impl <'a, S: RangeList + ?Sized, const IS_LIST: bool> Shifted<'a, S, IS_LIST> {
    fn iter(&self) -> impl Iterator<Item=Range<usize>> + Clone + '_ {
        self.src.iter().map(|r| {
            assert!(r.start + r.len() <= self.len);
            let beg = self.offset + r.start;
            let end = self.offset + r.end;
            beg..end
        })
    }
}


impl <'a, S: RangeList + ?Sized> RangeList for Shifted<'a, S, false> {
    #[inline]
    fn iter(&self) -> impl Iterator<Item=Range<usize>> + Clone {
        self.iter()
    }

    #[inline]
    fn span(&mut self) -> usize {
        self.src.span()
    }

    #[inline]
    fn shift(&mut self, offset: usize, len: usize) -> impl RangeList {
        assert!(offset + len <= self.len);
        Shifted::<_, false> {
            src: self.src,
            offset: self.offset + offset,
            len
        }
    }
}


impl <'a, S: RangeList + ?Sized> RangeList for Shifted<'a, S, true> {
    #[inline]
    fn iter(&self) -> impl Iterator<Item=Range<usize>> + Clone {
        self.iter()
    }

    #[inline]
    fn span(&mut self) -> usize {
        self.src.span()
    }

    #[inline]
    fn shift(&mut self, offset: usize, len: usize) -> impl RangeList {
        assert!(offset + len <= self.len);
        Shifted::<_, true> {
            src: self.src,
            offset: self.offset + offset,
            len
        }
    }

    fn list_items<'b>(&'b self, offsets: Offsets<'b>) -> impl RangeList + 'b {
        FinalListItems::new(self.iter(), offsets)
    }
}


macro_rules! compute_span {
    ($this:ident) => {
        if let Some(span) = $this.span {
            span
        } else {
            let span = $this.iter().map(|r| r.len()).sum();
            $this.span = Some(span);
            span
        }
    };
}


struct ListItems<'a, I> {
    inner: I,
    offsets: Offsets<'a>,
    span: Option<usize>
}


impl <'a, I: Iterator<Item=Range<usize>> + Clone>  RangeList for ListItems<'a, I> {
    fn iter(&self) -> impl Iterator<Item=Range<usize>> + Clone {
        self.inner.clone().map(|r| {
            let beg = self.offsets.index(r.start);
            let end = self.offsets.index(r.end);
            beg..end
        })
    }

    fn span(&mut self) -> usize {
        compute_span!(self)
    }

    #[inline]
    fn shift(&mut self, offset: usize, len: usize) -> impl RangeList {
        Shifted::<_, true> {
            src: self,
            offset,
            len
        }
    }

    #[inline]
    fn list_items<'b>(&'b self, offsets: Offsets<'b>) -> impl RangeList {
        FinalListItems::new(self.iter(), offsets)
    }
}


struct FinalListItems {
    list: Vec<Range<u32>>,
    span: usize
}


impl FinalListItems {
    fn new(ranges: impl Iterator<Item=Range<usize>>, offsets: Offsets<'_>) -> Self {
        let mut span = 0;

        let list: Vec<_> = ranges.map(|r| {
            let beg = offsets.value(r.start) as u32;
            let end = offsets.value(r.end) as u32;
            span += (end - beg) as usize;
            beg..end
        }).collect();

        Self {
            list,
            span
        }
    }
}


impl RangeList for FinalListItems {
    #[inline]
    fn iter(&self) -> impl Iterator<Item=Range<usize>> + Clone {
        self.list.iter().map(|r| r.start as usize..r.end as usize)
    }

    fn span(&mut self) -> usize {
        self.span
    }

    #[inline]
    fn shift(&mut self, offset: usize, len: usize) -> impl RangeList {
        Shifted::<_, true> {
            src: self,
            offset,
            len
        }
    }

    fn list_items<'b>(&'b self, offsets: Offsets<'b>) -> impl RangeList {
        Self::new(self.iter(), offsets)
    }
}


pub struct RangeListFromIterator<I> {
    inner: I,
    span: Option<usize>
}


impl <I> RangeListFromIterator<I> {
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            span: None
        }
    }

    pub fn with_size(inner: I, span: impl Into<Option<usize>>) -> Self {
        Self {
            inner,
            span: span.into()
        }
    }
}


impl <I: Iterator<Item=Range<usize>> + Clone> RangeList for RangeListFromIterator<I> {
    #[inline]
    fn iter(&self) -> impl Iterator<Item=Range<usize>> + Clone {
        self.inner.clone()
    }

    fn span(&mut self) -> usize {
        compute_span!(self)
    }
}