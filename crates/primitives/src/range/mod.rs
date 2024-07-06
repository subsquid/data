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


impl <Idx: Ord + Copy> RangeList<Idx> {
    pub fn new(ranges: Vec<Range<Idx>>) -> Self {
        Self::try_from(ranges).unwrap()
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
}


impl <Idx: Ord + Copy + Sub<Output = Idx> + Debug> RangeList<Idx> {
    pub fn filter_groups<'a>(&'a self, offsets: &'a [Idx]) -> impl Iterator<Item = (usize, Option<Self>)> + 'a {
        let mut ranges = self.iter().peekable();
        let mut i = 0;
        std::iter::from_fn(move || {
            while i < offsets.len() - 1 {
                let gix = i;
                let group = offsets[i]..offsets[i + 1];
                assert!(!group.is_empty());

                let mut included_ranges = Vec::new();

                loop {
                    if let Some(range) = ranges.peek().cloned() {
                        let intersection = max(group.start, range.start)..min(group.end, range.end);

                        if intersection == group {
                            i += 1;
                            return Some((gix, None))
                        }
                        
                        if intersection.end > intersection.start {
                            included_ranges.push(
                                intersection.start - group.start..intersection.end-group.start
                            );
                        }
                        
                        if intersection.end == range.end {
                            ranges.next();
                        }

                        if group.end == intersection.end {
                            break
                        }
                    } else {
                        return if included_ranges.len() > 0 {
                            Some((gix, Some(RangeList {
                                ranges: included_ranges
                            })))
                        } else {
                            None
                        }
                    }
                }

                i += 1;

                if included_ranges.len() > 0 {
                    return Some((gix, Some(RangeList {
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