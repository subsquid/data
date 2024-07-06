use std::ops::Range;

use crate::range::RangeList;


pub mod range;

pub type Name = &'static str;

pub type BlockNumber = u64;

pub type ItemIndex = u32;

pub type ShortHash = [u8; 8];

pub type RowRange = Range<ItemIndex>;

pub type RowRangeList = RangeList<ItemIndex>;