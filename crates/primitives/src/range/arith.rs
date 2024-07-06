use std::cmp::{max, min, Ordering};
use std::ops::Range;


pub fn seal<I, L>(ranges: L) -> impl Iterator<Item=Range<I>>
where
    I: Ord,
    L: IntoIterator<Item=Range<I>>,
{
    let mut list = ranges.into_iter().peekable();
    std::iter::from_fn(move || {
        loop {
            match (list.next(), list.peek_mut()) {
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
    })
}


pub fn intersection<I, L1, L2>(a: L1, b: L2) -> impl Iterator<Item=Range<I>>
where
    L1: IntoIterator<Item=Range<I>>,
    L2: IntoIterator<Item=Range<I>>,
    I: Ord + Clone,
{
    let mut list1 = a.into_iter().peekable();
    let mut list2 = b.into_iter().peekable();
    std::iter::from_fn(move || {
        loop {
            match (list1.peek().cloned(), list2.peek().cloned()) {
                (Some(h1), Some(h2)) => {
                    let start = max(h1.start, h2.start);
                    let end = match h1.end.cmp(&h2.end) {
                        Ordering::Less => {
                            list1.next();
                            h1.end
                        }
                        Ordering::Equal => {
                            list1.next();
                            list2.next();
                            h1.end
                        }
                        Ordering::Greater => {
                            list2.next();
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
    })
}


pub fn union<I, L1, L2>(a: L1, b: L2) -> impl Iterator<Item=Range<I>>
where
    L1: IntoIterator<Item=Range<I>>,
    L2: IntoIterator<Item=Range<I>>,
    I: Ord + Copy
{
    let mut list1 = a.into_iter().peekable();
    let mut list2 = b.into_iter().peekable();
    seal(std::iter::from_fn(move || {
        match (list1.peek().cloned(), list2.peek().cloned()) {
            (Some(h1), Some(h2)) => {
                let start = min(h1.start, h2.start);
                let end = min(h1.end, h2.end);
                if h1.end == end {
                    list1.next();
                } else if h1.start < end {
                    list1.peek_mut().unwrap().start = end;
                }
                if h2.end == end {
                    list2.next();
                } else {
                    list2.peek_mut().unwrap().start = end;
                }
                Some(start..end)
            },
            (_, None) => list1.next(),
            (None, _) => list2.next()
        }
    }))
}