use arrow::array::{Array, ArrowPrimitiveType, BooleanArray, GenericByteArray, PrimitiveArray};
use arrow::datatypes::ByteArrayType;
use arrow_buffer::ArrowNativeType;


pub trait Access {
    type Value;

    fn get(&self, i: usize) -> Self::Value;

    fn is_valid(&self, i: usize) -> bool;

    #[inline]
    fn is_null(&self, i: usize) -> bool {
        !self.is_valid(i)
    }

    fn has_nulls(&self) -> bool;
}


impl <'a, T: Access> Access for &'a T {
    type Value = T::Value;

    #[inline]
    fn get(&self, i: usize) -> Self::Value {
        (*self).get(i)
    }

    #[inline]
    fn is_valid(&self, i: usize) -> bool {
        (*self).is_valid(i)
    }

    #[inline]
    fn has_nulls(&self) -> bool {
        (*self).has_nulls()
    }
}


impl Access for BooleanArray {
    type Value = bool;

    #[inline]
    fn get(&self, i: usize) -> bool {
        self.value(i)
    }

    #[inline]
    fn is_valid(&self, i: usize) -> bool {
        Array::is_valid(self, i)
    }

    fn has_nulls(&self) -> bool {
        self.null_count() > 0
    }
}


impl <T: ArrowPrimitiveType> Access for PrimitiveArray<T> {
    type Value = T::Native;

    #[inline]
    fn get(&self, i: usize) -> Self::Value {
        self.value(i)
    }

    #[inline]
    fn is_valid(&self, i: usize) -> bool {
        Array::is_valid(self, i)
    }

    fn has_nulls(&self) -> bool {
        self.null_count() > 0
    }
}


impl <'a, T: ByteArrayType> Access for &'a GenericByteArray<T> {
    type Value = &'a [u8];

    fn get(&self, i: usize) -> Self::Value {
        let offsets = self.offsets();
        let beg = offsets[i].as_usize();
        let end = offsets[i + 1].as_usize();
        &self.values()[beg..end]
    }

    fn is_valid(&self, i: usize) -> bool {
        Array::is_valid(self, i)
    }

    fn has_nulls(&self) -> bool {
        self.null_count() > 0
    }
}
