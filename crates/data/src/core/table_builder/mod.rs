use arrow::array::ArrayRef;

pub use large::*;


mod large;


pub trait TableBuilder {
    type Row;

    fn push(&mut self, row: &Self::Row);

    fn push_from_iter<'a, I: IntoIterator<Item = &'a Self::Row>>(&'a mut self, rows: I) {
        for row in rows.into_iter() {
            self.push(row)
        }
    }

    fn num_rows(&self) -> usize;

    fn num_bytes(&self) -> usize;

    fn finish(&mut self) -> impl Iterator<Item=ArrayRef> + '_;
}