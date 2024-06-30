use arrow::array::{ArrayBuilder, ArrayRef};

use super::row::Row;
use super::sorter::{SortedRows, Sorter};


pub trait RowProcessor: Sized {
    type Row: Row;
    type Builder: ArrayBuilder + Default;

    fn map(&mut self, builder: &mut Self::Builder, row: &Self::Row);

    fn pre(&mut self, _row: &Self::Row) {}

    fn post(&mut self, array: ArrayRef) -> ArrayRef {
        array
    }

    fn capacity(&self) -> usize {
        300_000
    }

    fn memory_threshold(&self) -> usize {
        10 * 1024 * 1024
    }

    fn into_builder(self) -> RowArrayBuilder<Self> {
        let sorter = Sorter::new(self.capacity(), self.memory_threshold());
        RowArrayBuilder::new(sorter, Self::Builder::default(), self)
    }
}


pub struct RowArrayBuilder<P: RowProcessor> {
    sorter: Sorter<P::Row>,
    builder: P::Builder,
    processor: P
}


impl <P: RowProcessor> RowArrayBuilder<P> {
    pub fn new(sorter: Sorter<P::Row>, builder: P::Builder, processor: P) -> Self {
        Self {
            sorter,
            builder,
            processor
        }
    }

    #[inline]
    pub fn push(&mut self, row: &P::Row) {
        self.processor.pre(row);
        self.sorter.push(row)
    }

    pub fn push_from_iter<'a, I: IntoIterator<Item = &'a P::Row>>(&'a mut self, rows: I) {
        for row in rows.into_iter() {
            self.push(row)
        }
    }

    pub fn n_rows(&self) -> usize {
        self.sorter.n_rows()
    }

    pub fn n_bytes(&self) -> usize {
        self.sorter.n_bytes()
    }

    pub fn finish(&mut self) -> impl Iterator<Item=ArrayRef> + '_ {
        RowBatchIter {
            rows: self.sorter.take_sorted(),
            writer: self,
            pos: 0
        }
    }
}


struct RowBatchIter<'a, P: RowProcessor> {
    rows: SortedRows<P::Row>,
    writer: &'a mut RowArrayBuilder<P>,
    pos: usize
}


impl<'a, P: RowProcessor> Iterator for RowBatchIter<'a, P> {
    type Item = ArrayRef;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(row) = self.rows.next() {
            self.writer.processor.map(&mut self.writer.builder, &row);
            if self.rows.offset() - self.pos > 1024 * 1024 {
                self.pos = self.rows.offset();
                break
            }
        }
        if self.writer.builder.len() == 0 {
            return None
        }
        let array = self.writer.builder.finish();
        let array = self.writer.processor.post(array);
        Some(array)
    }
}