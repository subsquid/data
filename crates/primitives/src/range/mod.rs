use std::cmp::{max, min, Ordering};
use std::fmt::Debug;
use std::ops::{Range, Sub};

use crate::ItemIndex;
use crate::range::arith::{intersection, seal, union};


mod arith;


#[derive(Debug)]
pub struct RangeList<Idx> {
    ranges: Vec<Range<Idx>>
}


impl <Idx: Ord> TryFrom<Vec<Range<Idx>>> for RangeList<Idx> {
    type Error = &'static str;

    fn try_from(ranges: Vec<Range<Idx>>) -> Result<Self, Self::Error> {
        if ranges.iter().any(|r| r.is_empty()) {
            return Err("range list can only contain non-empty ranges")
        }
        for i in 1..ranges.len() {
            let current = &ranges[i];
            let prev = &ranges[i-1];
            if prev.end > current.start {
                return Err("found unordered or overlapping ranges")
            }
        }
        Ok(Self {
            ranges
        })
    }
}


impl <Idx: Ord + Copy + Default> RangeList<Idx> {
    pub fn new(ranges: Vec<Range<Idx>>) -> Self {
        Self::try_from(ranges).unwrap()
    }
    
    pub unsafe fn new_unchecked(ranges: Vec<Range<Idx>>) -> Self {
        Self {
            ranges
        }
    }

    pub fn seal<I: IntoIterator<Item = Range<Idx>>>(list: I) -> Self {
        Self {
            ranges: seal(list).collect()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item=Range<Idx>> + '_ {
        self.ranges.iter().cloned()
    }
    
    pub fn as_slice(&self) -> &[Range<Idx>] {
        &self.ranges
    }

    pub fn union(&self, other: &Self) -> Self {
        Self {
            ranges: union(self.iter(), other.iter()).collect()
        }
    }

    pub fn intersection(&self, other: &Self) -> Self {
        Self {
            ranges: intersection(self.iter(), other.iter()).collect()
        }
    }
    
    pub fn end(&self) -> Idx {
        self.ranges.last().map(|r| r.end).unwrap_or_default()
    }
    
    pub fn len(&self) -> usize {
        self.ranges.len()
    }
}


impl <Idx: Ord + Copy + Default + Sub<Output = Idx> + Debug> RangeList<Idx> {
    pub fn paginate<'a>(&'a self, page_offsets: &'a [Idx]) -> impl Iterator<Item = (usize, Option<Self>)> + 'a {
        let mut ranges = self.iter().peekable();
        let mut i = 0;
        std::iter::from_fn(move || {
            while i < page_offsets.len() - 1 {
                let pix = i;
                let page = page_offsets[i]..page_offsets[i + 1];
                assert!(!page.is_empty());

                let mut included_ranges = Vec::new();

                loop {
                    if let Some(range) = ranges.peek().cloned() {
                        let intersection = max(page.start, range.start)..min(page.end, range.end);

                        if intersection == page {
                            i += 1;
                            return Some((pix, None))
                        }
                        
                        if intersection.end > intersection.start {
                            included_ranges.push(
                                intersection.start - page.start..intersection.end - page.start
                            );
                        }
                        
                        if intersection.end == range.end {
                            ranges.next();
                        }

                        if page.end == intersection.end {
                            break
                        }
                    } else {
                        return if included_ranges.len() > 0 {
                            Some((pix, Some(RangeList {
                                ranges: included_ranges
                            })))
                        } else {
                            None
                        }
                    }
                }

                i += 1;

                if included_ranges.len() > 0 {
                    return Some((pix, Some(RangeList {
                        ranges: std::mem::take(&mut included_ranges)
                    })))
                }
            }
            None
        })
    }
}


impl RangeList<ItemIndex> {
    pub fn from_sorted_indexes<I: IntoIterator<Item = ItemIndex>>(indexes: I) -> Self {
        let mut iter = indexes.into_iter();
        let mut ranges = Vec::new();
        if let Some(idx) = iter.next() {
            let mut beg = idx;
            let mut end = idx + 1;
            while let Some(idx) = iter.next() {
                match end.cmp(&idx) {
                    Ordering::Less => {
                        ranges.push(beg..end);
                        beg = idx;
                        end = idx + 1;
                    },
                    Ordering::Equal => {
                        end = idx + 1;
                    },
                    Ordering::Greater => {
                        panic!("index list was unsorted")
                    }
                }
            }
            ranges.push(beg..end)
        }
        Self {
            ranges
        }
    }
}