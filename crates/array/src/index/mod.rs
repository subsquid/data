use std::ops::Range;


pub trait RangeList {
    fn iter(&self) -> impl Iterator<Item=Range<usize>> + Clone;

    fn span(&mut self) -> usize;

    #[inline]
    fn shift(&mut self, offset: usize, len: usize) -> impl RangeList {
        ShiftedRangeList {
            src: self,
            offset,
            len
        }
    }
}


struct ShiftedRangeList<'a, S: ?Sized> {
    src: &'a mut S,
    offset: usize,
    len: usize
}


impl <'a, S: RangeList + ?Sized> RangeList for ShiftedRangeList<'a, S> {
    fn iter(&self) -> impl Iterator<Item=Range<usize>> + Clone {
        self.src.iter().map(|r| {
            assert!(
                r.start <= self.len && r.end <= self.len, 
                "{:?} is out of upper bound {}",
                r, self.len
            );
            let beg = self.offset + r.start;
            let end = self.offset + r.end;
            beg..end
        })
    }

    #[inline]
    fn span(&mut self) -> usize {
        self.src.span()
    }

    #[inline]
    fn shift(&mut self, offset: usize, len: usize) -> impl RangeList {
        assert!(offset + len <= self.len);
        ShiftedRangeList {
            src: self.src,
            offset: self.offset + offset,
            len
        }
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


pub struct MaterializedRangeList {
    ranges: Vec<Range<u32>>,
    span: Option<usize>
}


impl MaterializedRangeList {
    pub fn from_iter(ranges: impl Iterator<Item=Range<usize>>) -> Self {
        let mut span = 0;
        
        let ranges = ranges.map(|r| {
            span += r.len();
            r.start as u32..r.end as u32
        }).collect();

        Self {
            ranges,
            span: Some(span)
        }
    }
}


impl RangeList for MaterializedRangeList {
    #[inline]
    fn iter(&self) -> impl Iterator<Item=Range<usize>> + Clone {
        self.ranges.iter().map(|r| r.start as usize..r.end as usize)
    }
    
    fn span(&mut self) -> usize {
        compute_span!(self)
    }
}