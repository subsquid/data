use std::cmp::Ordering;
use crate::access::Access;


pub trait Order<Idx> {
    fn compare(&self, a: Idx, b: Idx) -> Ordering;
}


pub struct OrderPair<A, B>(pub A, pub B);


impl <Idx: Copy, A: Order<Idx>, B: Order<Idx>> Order<Idx> for OrderPair<A, B> {
    fn compare(&self, a: Idx, b: Idx) -> Ordering {
        match self.0.compare(a, b) {
            Ordering::Equal => self.1.compare(a, b),
            res => res
        }
    }
}


pub struct OrderList<T> {
    list: Vec<T>
}


impl <T> OrderList<T> {
    pub fn new(list: Vec<T>) -> Self {
        assert!(list.len() > 0);
        Self { list }
    }
}


impl <Idx: Copy, T: Order<Idx>> Order<Idx> for OrderList<T> {
    fn compare(&self, a: Idx, b: Idx) -> Ordering {
        for o in self.list.iter() {
            match o.compare(a, b) {
                Ordering::Equal => {}
                res => return res
            }
        }
        Ordering::Equal
    }
}


impl <T: Access<Value: Ord>> Order<usize> for T {
    #[inline]
    fn compare(&self, a: usize, b: usize) -> Ordering {
        match (self.is_valid(a), self.is_valid(b)) {
            (true, true) => self.get(a).cmp(&self.get(b)),
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => Ordering::Equal
        }
    }
}


pub struct IgnoreNulls<T>(pub T);


impl <T: Access<Value: Ord>> Order<usize> for IgnoreNulls<T> {
    #[inline]
    fn compare(&self, a: usize, b: usize) -> Ordering {
        self.0.get(a).cmp(&self.0.get(b))
    }
}


impl <'a> Order<usize> for &'a dyn Order<usize> {
    #[inline]
    fn compare(&self, a: usize, b: usize) -> Ordering {
        (*self).compare(a, b)
    }
}


impl <'a> Order<usize> for Box<dyn Order<usize> + 'a> {
    #[inline]
    fn compare(&self, a: usize, b: usize) -> Ordering {
        self.as_ref().compare(a, b)
    }
}