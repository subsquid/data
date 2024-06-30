use arrow::array::{ArrayBuilder, ArrayRef};

use super::row::Row;


pub trait RowProcessor: Sized {
    type Row: Row;
    type Builder: ArrayBuilder;

    fn map(&mut self, builder: &mut Self::Builder, row: &Self::Row);

    fn pre(&mut self, _row: &Self::Row) {}

    fn post(&self, array: ArrayRef) -> ArrayRef;
}