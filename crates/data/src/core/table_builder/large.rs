use arrow::array::{ArrayBuilder, ArrayRef};

use crate::core::RowProcessor;
use crate::core::sorter::Sorter;

use super::TableBuilder;


pub struct LargeTableBuilder<P: RowProcessor> {
    sorter: Sorter<P::Row>,
    builder: P::Builder,
    processor: P
}


impl <P: RowProcessor + Default> LargeTableBuilder<P> {
    pub fn new(
        builder: P::Builder,
    ) -> Self
    {
        Self {
            sorter: Sorter::default(),
            builder,
            processor: P::default()
        }
    }
}


impl <P: RowProcessor + Default> TableBuilder for LargeTableBuilder<P> {
    type Row = P::Row;

    fn push(&mut self, row: &Self::Row) {
        self.processor.pre(row);
        self.sorter.push(row)
    }

    fn num_rows(&self) -> usize {
        self.sorter.num_rows()
    }

    fn num_bytes(&self) -> usize {
        self.sorter.num_bytes()
    }

    fn finish(&mut self) -> impl Iterator<Item=ArrayRef> + '_ {
        let mut rows = self.sorter.take_sorted();
        let mut processor = std::mem::take(&mut self.processor);
        let mut pos = 0;

        std::iter::from_fn(move || {
            while let Some(row) = rows.next() {
                processor.map(&mut self.builder, &row);
                if rows.offset() - pos > 1024 * 1024 {
                    pos = rows.offset();
                    break
                }
            }
            if self.builder.len() == 0 {
                return None
            }
            let array = self.builder.finish();
            let array = processor.post(array);
            Some(array)
        })
    }
}