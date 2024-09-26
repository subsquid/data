mod bitmask;


use crate::writer::ArrayWriter;
pub use bitmask::*;
use std::ops::Range;


pub trait Slice<'a>: Sized + Clone {
    fn len(&self) -> usize;

    // fn slice(&self, offset: usize, len: usize) -> Self;

    fn write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()>;

    fn write_range(&self, dst: &mut impl ArrayWriter, range: Range<usize>) -> anyhow::Result<()>;

    fn write_indexes(
        &self,
        dst: &mut impl ArrayWriter,
        indexes: impl IntoIterator<Item = usize>
    ) -> anyhow::Result<()>;
}