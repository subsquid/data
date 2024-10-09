mod any;
pub mod bitmask;
mod boolean;
mod list;
mod native;
pub mod nullmask;
mod primitive;
mod r#struct;


use crate::writer::{ArrayWriter, RangeList};
pub use any::*;
pub use boolean::*;
pub use list::*;
pub use primitive::*;
pub use r#struct::*;
use std::ops::Range;


pub trait Slice: Clone {
    fn num_buffers(&self) -> usize;
    
    fn len(&self) -> usize;
    
    fn slice(&self, offset: usize, len: usize) -> Self;
    
    fn write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()>;
    
    #[inline]
    fn write_range(&self, dst: &mut impl ArrayWriter, range: Range<usize>) -> anyhow::Result<()> {
        self.slice(range.start, range.len()).write(dst)
    }

    fn write_ranges(&self, dst: &mut impl ArrayWriter, ranges: &mut impl RangeList) -> anyhow::Result<()>;

    fn write_indexes(
        &self, 
        dst: &mut impl ArrayWriter, 
        indexes: impl IntoIterator<Item = usize, IntoIter: Clone>
    ) -> anyhow::Result<()>;
}


pub trait AsSlice {
    type Slice<'a>: Slice where Self: 'a;

    fn as_slice(&self) -> Self::Slice<'_>;
}