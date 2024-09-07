use crate::table::util::{BufferBag, BufferCallback};
use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray, BinaryArray, PrimitiveArray, StringArray};
use arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type};
use arrow_buffer::{BooleanBuffer, ScalarBuffer};
use std::sync::Arc;


#[derive(Default)]
struct Bag<T> {
    validity: Vec<bool>,
    min: Vec<T>,
    max: Vec<T>
}


impl <T: PartialOrd> Bag<T> {
    fn push(&mut self, is_valid: bool, min: T, max: T) {
        self.validity.push(is_valid);
        self.min.push(min);
        self.max.push(max)
    }

    fn merge_last(&mut self) {
        let validity = self.validity.pop().unwrap();
        let min = self.min.pop().unwrap();
        let max = self.max.pop().unwrap();

        let t_validity = self.validity.last_mut().unwrap();
        let t_min = self.min.last_mut().unwrap();
        let t_max = self.max.last_mut().unwrap();

        match (*t_validity, validity) {
            (true, true) => {
                if min.lt(t_min) {
                    *t_min = min;
                }
                if max.gt(t_max) {
                    *t_max = max;
                }
            },
            (false, true) => {
                *t_validity = true;
                *t_min = min;
                *t_max = max;
            },
            _ => {}
        }
    }
}


enum AnyBag {
    Int8(Bag<i8>),
    Int16(Bag<i16>),
    Int32(Bag<i32>),
    Int64(Bag<i64>),
    UInt8(Bag<u8>),
    UInt16(Bag<u16>),
    UInt32(Bag<u32>),
    UInt64(Bag<u64>),
    Binary(Bag<Vec<u8>>),
    String(Bag<String>)
}


impl AnyBag {
    fn can_have_stats(data_type: &DataType) -> bool {
        match data_type {
            DataType::Int8 => true,
            DataType::Int16 => true,
            DataType::Int32 => true,
            DataType::Int64 => true,
            DataType::UInt8 => true,
            DataType::UInt16 => true,
            DataType::UInt32 => true,
            DataType::UInt64 => true,
            DataType::Binary => true,
            DataType::Utf8 => true,
            _ => false
        }
    }

    fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Int8 => AnyBag::Int8(Bag::default()),
            DataType::Int16 => AnyBag::Int16(Bag::default()),
            DataType::Int32 => AnyBag::Int32(Bag::default()),
            DataType::Int64 => AnyBag::Int64(Bag::default()),
            DataType::UInt8 => AnyBag::UInt8(Bag::default()),
            DataType::UInt16 => AnyBag::UInt16(Bag::default()),
            DataType::UInt32 => AnyBag::UInt32(Bag::default()),
            DataType::UInt64 => AnyBag::UInt64(Bag::default()),
            DataType::Binary => AnyBag::Binary(Bag::default()),
            DataType::Utf8 => AnyBag::String(Bag::default()),
            ty => panic!("unsupported data type - {}", ty)
        }
    }

    fn push(&mut self, array: &dyn Array) {
        match self {
            AnyBag::Int8(bag) => push_primitive::<Int8Type>(bag, array),
            AnyBag::Int16(bag) => push_primitive::<Int16Type>(bag, array),
            AnyBag::Int32(bag) => push_primitive::<Int32Type>(bag, array),
            AnyBag::Int64(bag) => push_primitive::<Int64Type>(bag, array),
            AnyBag::UInt8(bag) => push_primitive::<UInt8Type>(bag, array),
            AnyBag::UInt16(bag) => push_primitive::<UInt16Type>(bag, array),
            AnyBag::UInt32(bag) => push_primitive::<UInt32Type>(bag, array),
            AnyBag::UInt64(bag) => push_primitive::<UInt64Type>(bag, array),
            AnyBag::Binary(bag) => push_binary(bag, array),
            AnyBag::String(bag) => push_string(bag, array)
        }
    }

    fn merge_last(&mut self) {
        match self {
            AnyBag::Int8(bag) => bag.merge_last(),
            AnyBag::Int16(bag) => bag.merge_last(),
            AnyBag::Int32(bag) => bag.merge_last(),
            AnyBag::Int64(bag) => bag.merge_last(),
            AnyBag::UInt8(bag) => bag.merge_last(),
            AnyBag::UInt16(bag) => bag.merge_last(),
            AnyBag::UInt32(bag) => bag.merge_last(),
            AnyBag::UInt64(bag) => bag.merge_last(),
            AnyBag::Binary(bag) => bag.merge_last(),
            AnyBag::String(bag) => bag.merge_last(),
        }
    }

    fn finish(self) -> (Option<BooleanBuffer>, ArrayRef, ArrayRef) {
        macro_rules! prim {
            ($bag:ident, $t:ty) => {{
                let nulls = $bag.validity.iter().any(|x| !x).then(|| {
                    BooleanBuffer::from($bag.validity)
                });
                let min = Arc::new(PrimitiveArray::<$t>::from($bag.min));
                let max = Arc::new(PrimitiveArray::<$t>::from($bag.max));
                (nulls, min, max)
            }};
        }
        match self {
            AnyBag::Int8(bag) => prim!(bag, Int8Type),
            AnyBag::Int16(bag) => prim!(bag, Int16Type),
            AnyBag::Int32(bag) => prim!(bag, Int32Type),
            AnyBag::Int64(bag) => prim!(bag, Int64Type),
            AnyBag::UInt8(bag) => prim!(bag, UInt8Type),
            AnyBag::UInt16(bag) => prim!(bag, UInt16Type),
            AnyBag::UInt32(bag) => prim!(bag, UInt32Type),
            AnyBag::UInt64(bag) => prim!(bag, UInt64Type),
            AnyBag::Binary(bag) => {
                let nulls = bag.validity.iter().any(|x| !x).then(|| {
                    BooleanBuffer::from(bag.validity)
                });
                let min: Vec<_> = bag.min.iter().map(|v| v.as_slice()).collect();
                let max: Vec<_> = bag.max.iter().map(|v| v.as_slice()).collect();
                let min = Arc::new(BinaryArray::from(min));
                let max = Arc::new(BinaryArray::from(max));
                (nulls, min, max)
            },
            AnyBag::String(bag) => {
                let nulls = bag.validity.iter().any(|x| !x).then(|| {
                    BooleanBuffer::from(bag.validity)
                });
                let min: Vec<_> = bag.min.iter().map(|v| v.as_str()).collect();
                let max: Vec<_> = bag.max.iter().map(|v| v.as_str()).collect();
                let min = Arc::new(StringArray::from(min));
                let max = Arc::new(StringArray::from(max));
                (nulls, min, max)
            },
        }
    }
}


fn push_primitive<T: ArrowPrimitiveType>(bag: &mut Bag<T::Native>, array: &dyn Array) {
    let array = array.as_primitive::<T>();
    let min = arrow::compute::min(array);
    if let Some(min) = min {
        let max = arrow::compute::max(array).unwrap();
        bag.push(true, min, max)
    } else {
        bag.push(false, T::default_value(), T::default_value())
    }
}


fn push_binary(bag: &mut Bag<Vec<u8>>, array: &dyn Array) {
    let array = array.as_binary::<i32>();
    let min = arrow::compute::min_binary(array);
    if let Some(min) = min {
        let max = arrow::compute::max_binary(array).unwrap();
        bag.push(true, min.to_vec(), max.to_vec())
    } else {
        bag.push(false, vec![], vec![])
    }
}


fn push_string(bag: &mut Bag<String>, array: &dyn Array) {
    let array = array.as_string::<i32>();
    let min = arrow::compute::min_string(array);
    if let Some(min) = min {
        let max = arrow::compute::max_string(array).unwrap();
        bag.push(true, min.to_string(), max.to_string())
    } else {
        bag.push(false, String::new(), String::new())
    }
}


pub struct Stats {
    pub offsets: ScalarBuffer<u32>,
    pub nulls: Option<BooleanBuffer>,
    pub min: ArrayRef,
    pub max: ArrayRef
}


impl BufferBag for Stats {
    fn for_each_buffer(&self, cb: BufferCallback<'_>) {
        self.offsets.for_each_buffer(cb);
        
        if let Some(buf) = self.nulls.as_ref() {
            buf.for_each_buffer(cb)
        } else {
            cb(0, &[])
        }
        
        self.min.as_ref().for_each_buffer(cb);
        self.max.as_ref().for_each_buffer(cb)
    }
}


pub struct StatsBuilder {
    offsets: Vec<u32>,
    bag: AnyBag,
    partition: usize
}


impl StatsBuilder {
    pub fn can_have_stats(data_type: &DataType) -> bool {
        AnyBag::can_have_stats(data_type)
    }

    pub fn new(data_type: &DataType, partition: usize) -> Self {
        Self {
            offsets: vec![0],
            bag: AnyBag::new(data_type),
            partition
        }
    }

    pub fn push_array(&mut self, array: &dyn Array) {
        let mut offset = if self.last_partition_len() < self.partition {
            let len = std::cmp::min(self.partition - self.last_partition_len(), array.len());
            let merge = self.last_partition_len() > 0;
            self.push_array_values(&array.slice(0, len));
            if merge {
                self.merge_last();
            }
            len
        } else {
            0
        };

        while offset < array.len() {
            let len = std::cmp::min(self.partition, array.len() - offset);
            self.push_array_values(&array.slice(offset, len));
            offset += len
        }
    }

    fn push_array_values(&mut self, array: &dyn Array) {
        let offset = self.offsets.last().unwrap();
        self.offsets.push(offset + array.len() as u32);
        self.bag.push(array)
    }

    fn merge_last(&mut self) {
        let offset = self.offsets.pop().unwrap();
        *(self.offsets.last_mut().unwrap()) = offset;
        self.bag.merge_last()
    }

    fn last_partition_len(&self) -> usize {
        let last = self.offsets.len() - 1;
        let len = if last > 0 {
            self.offsets[last] - self.offsets[last - 1]
        } else {
            0
        };
        len as usize
    }

    pub fn num_partitions(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn finish(mut self) -> Option<Stats> {
        if self.num_partitions() < 2 && self.last_partition_len() < self.partition {
            return None
        }
        if self.last_partition_len() < self.partition / 2 {
            self.merge_last()
        }
        let offsets = ScalarBuffer::from(self.offsets);
        let (nulls, min, max) = self.bag.finish();
        Some(Stats {
            offsets,
            nulls,
            min,
            max
        })
    }
}