use std::ops::Deref;

use arrow::array::{Array, ArrowPrimitiveType, AsArray, OffsetSizeTrait, StringArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};


pub trait Key {
    type Item: ?Sized;

    fn get(&self, idx: usize) -> &Self::Item;
}


pub struct PrimitiveKey<A: ArrowPrimitiveType> {
    values: ScalarBuffer<A::Native>
}


impl <A: ArrowPrimitiveType> Key for PrimitiveKey<A> {
    type Item = A::Native;

    #[inline]
    fn get(&self, idx: usize) -> &Self::Item {
        &self.values[idx]
    }
}


impl <A: ArrowPrimitiveType> From<&dyn Array> for PrimitiveKey<A> {
    fn from(value: &dyn Array) -> Self {
        let array = value.as_primitive::<A>();
        let values = array.values().clone();
        Self {
            values
        }
    }
}


impl <'a> Key for StringArray {
    type Item = str;

    #[inline]
    fn get(&self, idx: usize) -> &Self::Item {
        self.value(idx)
    }
}


pub type PrimitiveListKey<A> = PrimitiveGenericListKey<A, i32>;
pub type PrimitiveLargeListKey<A> = PrimitiveGenericListKey<A, i64>;


pub struct PrimitiveGenericListKey<A: ArrowPrimitiveType, O: OffsetSizeTrait> {
    offsets: OffsetBuffer<O>,
    values: ScalarBuffer<A::Native>
}


impl <A: ArrowPrimitiveType, O: OffsetSizeTrait> Key for PrimitiveGenericListKey<A, O> {
    type Item = [A::Native];

    fn get(&self, idx: usize) -> &Self::Item {
        let start = self.offsets[idx].as_usize();
        let end = self.offsets[idx + 1].as_usize();
        &self.values.deref()[start..end]
    }
}


impl <A: ArrowPrimitiveType, O: OffsetSizeTrait> From<&dyn Array> for PrimitiveGenericListKey<A, O> {
    fn from(value: &dyn Array) -> Self {
        let array = value.as_list();
        let offsets = array.offsets().clone();
        let values = array.values()
            .as_primitive::<A>()
            .values()
            .clone();
        Self {
            offsets,
            values
        }
    }
}


pub type ListKey<K> = GenericListKey<K, i32>;
pub type LargeListKey<K> = GenericListKey<K, i64>;


pub struct GenericListKey<K, O: OffsetSizeTrait> {
    offsets: OffsetBuffer<O>,
    item: K
}


impl <K, O: OffsetSizeTrait> GenericListKey<K, O> {
    pub fn get(&self, idx: usize) -> OffsetKey<'_, K> {
        let offset = self.offsets[idx].as_usize();
        let len = self.offsets[idx + 1].as_usize() - offset;
        OffsetKey {
            item: &self.item,
            offset,
            len
        }
    }
}


pub struct OffsetKey<'a, K> {
    item: &'a K,
    offset: usize,
    len: usize
}


impl <'a, K> OffsetKey<'a, K> {
    pub fn len(&self) -> usize {
        self.len
    }
}


impl <K: Key> Key for OffsetKey<'_, K> {
    type Item = K::Item;

    fn get(&self, idx: usize) -> &Self::Item {
        self.item.get(self.offset + idx)
    }
}


impl <K, O: OffsetSizeTrait> From<&dyn Array> for GenericListKey<K, O>
    where K: for<'a> From<&'a dyn Array>
{
    fn from(value: &dyn Array) -> Self {
        let array = value.as_list();
        let offsets = array.offsets().clone();
        let item = K::from(array.values());
        Self {
            offsets,
            item
        }
    }
}