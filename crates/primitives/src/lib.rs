use std::ops::Range;

use crate::range::RangeList;
pub use sid::SID;


pub mod range;
mod sid;


pub type Name = &'static str;

pub type BlockNumber = u64;

pub type ItemIndex = u32;

pub type ShortHash = SID<8>;

pub type RowRange = Range<ItemIndex>;

pub type RowRangeList = RangeList<ItemIndex>;