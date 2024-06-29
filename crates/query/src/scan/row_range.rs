use std::cmp::{max, min, Ordering};
use std::iter::Peekable;
use std::ops::Range;


pub type RowRange = Range<i64>;
pub type RowRangeList = Vec<RowRange>;


pub fn seal<L>(ranges: L) -> impl Iterator<Item=RowRange>
    where L: IntoIterator<Item=RowRange>
{
    Seal {
        list: ranges.into_iter().peekable()
    }
}


struct Seal<L: Iterator> {
    list: Peekable<L>
}


impl <L: Iterator<Item = RowRange>> Iterator for Seal<L> {
    type Item = RowRange;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match (self.list.next(), self.list.peek_mut()) {
                (Some(head), Some(next)) => {
                    match head.end.cmp(&next.start) {
                        Ordering::Less => {
                            return Some(head)
                        }
                        Ordering::Equal => {
                            next.start = head.start
                        }
                        Ordering::Greater => {
                            panic!("unordered or intersecting ranges found in range list")
                        }
                    }
                }
                (head, _) => return head
            }
        }
    }
}


pub fn intersection<L1, L2>(a: L1, b: L2) -> impl Iterator<Item = RowRange>
    where L1: IntoIterator<Item = RowRange>,
          L2: IntoIterator<Item = RowRange>
{
    Intersection {
        list1: a.into_iter().peekable(),
        list2: b.into_iter().peekable()
    }
}


struct Intersection<L1: Iterator, L2: Iterator> {
    list1: Peekable<L1>,
    list2: Peekable<L2>
}


impl <L1: Iterator<Item = RowRange>, L2: Iterator<Item = RowRange>> Iterator for Intersection<L1, L2> {
    type Item = RowRange;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match (self.list1.peek().cloned(), self.list2.peek().cloned()) {
                (Some(h1), Some(h2)) => {
                    let start = max(h1.start, h2.start);
                    let end = match h1.end.cmp(&h2.end) {
                        Ordering::Less => {
                            self.list1.next();
                            h1.end
                        }
                        Ordering::Equal => {
                            self.list1.next();
                            self.list2.next();
                            h1.end
                        }
                        Ordering::Greater => {
                            self.list2.next();
                            h2.end
                        }
                    };
                    if start < end {
                        return Some(start..end)
                    }
                },
                (_, None) | (None, _) => return None
            }
        }
    }
}


pub fn union<L1, L2>(a: L1, b: L2) -> impl Iterator<Item = RowRange>
    where L1: IntoIterator<Item = RowRange>,
          L2: IntoIterator<Item = RowRange>
{
    seal(Union {
        list1: a.into_iter().peekable(),
        list2: b.into_iter().peekable()
    })
}


struct Union<L1: Iterator, L2: Iterator> {
    list1: Peekable<L1>,
    list2: Peekable<L2>
}


impl <L1: Iterator<Item = RowRange>, L2: Iterator<Item = RowRange>> Iterator for Union<L1, L2> {
    type Item = RowRange;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.list1.peek().cloned(), self.list2.peek().cloned()) {
            (Some(h1), Some(h2)) => {
                let start = min(h1.start, h2.start);
                let end = min(h1.end, h2.end);
                if h1.end == end {
                    self.list1.next();
                } else if h1.start < end {
                    self.list1.peek_mut().unwrap().start = end;
                }
                if h2.end == end {
                    self.list2.next();
                } else {
                    self.list2.peek_mut().unwrap().start = end;
                }
                Some(start..end)
            },
            (_, None) => self.list1.next(),
            (None, _) => self.list2.next()
        }
    }
}