use arrow::array::{Array, ArrayBuilder, ArrayRef, AsArray, BinaryBuilder, BooleanBuilder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, PrimitiveBuilder, StringBuilder, UInt16Builder, UInt32Array, UInt32Builder, UInt64Builder, UInt8Builder};
use arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type};


pub struct Summary {
    pub offsets: UInt32Array,
    pub null_count: UInt32Array,
    pub stats: Option<Stats>
}


pub struct Stats {
    pub min: ArrayRef,
    pub max: ArrayRef
}


impl Summary {
    pub fn item_count(&self) -> u32 {
        self.offsets.values().last().cloned().unwrap()
    }
}


struct StatsBuilder {
    data_type: DataType,
    min: Box<dyn ArrayBuilder>,
    max: Box<dyn ArrayBuilder>
}


impl StatsBuilder {
    fn can_have_stats(data_type: &DataType) -> bool {
        match data_type {
            DataType::Boolean => true,
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

    fn new(data_type: DataType) -> Self {
        macro_rules! min_max {
            ($b:expr) => {
                (Box::new($b) as Box<dyn ArrayBuilder>, Box::new($b) as Box<dyn ArrayBuilder>)
            }
        }

        let (min, max) = match &data_type {
            DataType::Boolean => min_max!(BooleanBuilder::with_capacity(200)),
            DataType::Int8 => min_max!(Int8Builder::with_capacity(200)),
            DataType::Int16 => min_max!(Int16Builder::with_capacity(200)),
            DataType::Int32 => min_max!(Int32Builder::with_capacity(200)),
            DataType::Int64 => min_max!(Int64Builder::with_capacity(200)),
            DataType::UInt8 => min_max!(UInt8Builder::with_capacity(200)),
            DataType::UInt16 => min_max!(UInt16Builder::with_capacity(200)),
            DataType::UInt32 => min_max!(UInt32Builder::with_capacity(200)),
            DataType::UInt64 => min_max!(UInt64Builder::with_capacity(200)),
            DataType::Binary => min_max!(BinaryBuilder::with_capacity(200, 200 * 32)),
            DataType::Utf8 => min_max!(StringBuilder::with_capacity(200, 200 * 68)),
            _ => panic!("can't build stats for {}", data_type)
        };
        Self {
            data_type,
            min,
            max
        }
    }

    fn push(&mut self, array: &dyn Array) {
        assert_eq!(&self.data_type, array.data_type());

        macro_rules! prim {
            ($t:ty) => {{
                let array = array.as_primitive::<$t>();
                let min = arrow::compute::min(array);
                let max = arrow::compute::max(array);
                let min_builder = self.min.as_any_mut().downcast_mut::<PrimitiveBuilder<$t>>().unwrap();
                let max_builder = self.max.as_any_mut().downcast_mut::<PrimitiveBuilder<$t>>().unwrap();
                min_builder.append_option(min);
                max_builder.append_option(max);
            }};
        }

        match array.data_type() {
            DataType::Boolean => {
                let array = array.as_boolean();
                let min = arrow::compute::bool_and(array);
                let max = arrow::compute::bool_or(array);
                let min_builder = self.min.as_any_mut().downcast_mut::<BooleanBuilder>().unwrap();
                let max_builder = self.max.as_any_mut().downcast_mut::<BooleanBuilder>().unwrap();
                min_builder.append_option(min);
                max_builder.append_option(max);
            },
            DataType::Int8 => prim!(Int8Type),
            DataType::Int16 => prim!(Int16Type),
            DataType::Int32 => prim!(Int32Type),
            DataType::Int64 => prim!(Int64Type),
            DataType::UInt8 => prim!(UInt8Type),
            DataType::UInt16 => prim!(UInt16Type),
            DataType::UInt32 => prim!(UInt32Type),
            DataType::UInt64 => prim!(UInt64Type),
            DataType::Binary => {
                let array = array.as_binary::<i32>();
                let min = arrow::compute::min_binary(array);
                let max = arrow::compute::max_binary(array);
                let min_builder = self.min.as_any_mut().downcast_mut::<BinaryBuilder>().unwrap();
                let max_builder = self.max.as_any_mut().downcast_mut::<BinaryBuilder>().unwrap();
                min_builder.append_option(min);
                max_builder.append_option(max);
            },
            DataType::Utf8 => {
                let array = array.as_string::<i32>();
                let min = arrow::compute::min_string(array);
                let max = arrow::compute::max_string(array);
                let min_builder = self.min.as_any_mut().downcast_mut::<StringBuilder>().unwrap();
                let max_builder = self.max.as_any_mut().downcast_mut::<StringBuilder>().unwrap();
                min_builder.append_option(min);
                max_builder.append_option(max);
            },
            _ => unreachable!()
        }
    }

    fn acc(&mut self, stats: &Stats) {
        macro_rules! prim {
            ($t:ty) => {{
                let min = stats.min.as_primitive::<$t>();
                let max = stats.max.as_primitive::<$t>();
                let min = arrow::compute::min(min);
                let max = arrow::compute::max(max);
                let min_builder = self.min.as_any_mut().downcast_mut::<PrimitiveBuilder<$t>>().unwrap();
                let max_builder = self.max.as_any_mut().downcast_mut::<PrimitiveBuilder<$t>>().unwrap();
                min_builder.append_option(min);
                max_builder.append_option(max);
            }};
        }
        
        match &self.data_type {
            DataType::Boolean => {
                let min = stats.min.as_boolean();
                let max = stats.max.as_boolean();
                let min = arrow::compute::bool_and(min);
                let max = arrow::compute::bool_or(max);
                let min_builder = self.min.as_any_mut().downcast_mut::<BooleanBuilder>().unwrap();
                let max_builder = self.max.as_any_mut().downcast_mut::<BooleanBuilder>().unwrap();
                min_builder.append_option(min);
                max_builder.append_option(max);
            },
            DataType::Int8 => prim!(Int8Type),
            DataType::Int16 => prim!(Int16Type),
            DataType::Int32 => prim!(Int32Type),
            DataType::Int64 => prim!(Int64Type),
            DataType::UInt8 => prim!(UInt8Type),
            DataType::UInt16 => prim!(UInt16Type),
            DataType::UInt32 => prim!(UInt32Type),
            DataType::UInt64 => prim!(UInt64Type),
            DataType::Binary => {
                let min = stats.min.as_binary::<i32>();
                let max = stats.max.as_binary::<i32>();
                let min = arrow::compute::min_binary(min);
                let max = arrow::compute::max_binary(max);
                let min_builder = self.min.as_any_mut().downcast_mut::<BinaryBuilder>().unwrap();
                let max_builder = self.max.as_any_mut().downcast_mut::<BinaryBuilder>().unwrap();
                min_builder.append_option(min);
                max_builder.append_option(max);
            },
            DataType::Utf8 => {
                let min = stats.min.as_string::<i32>();
                let max = stats.max.as_string::<i32>();
                let min = arrow::compute::min_string(min);
                let max = arrow::compute::max_string(max);
                let min_builder = self.min.as_any_mut().downcast_mut::<StringBuilder>().unwrap();
                let max_builder = self.max.as_any_mut().downcast_mut::<StringBuilder>().unwrap();
                min_builder.append_option(min);
                max_builder.append_option(max);
            },
            _ => unreachable!()
        }
    }

    fn finish(&mut self) -> Stats {
        Stats {
            min: self.min.finish(),
            max: self.max.finish()
        }
    }
}


pub struct SummaryBuilder {
    offsets: UInt32Builder,
    null_count: UInt32Builder,
    stats: Option<StatsBuilder>
}


impl SummaryBuilder {
    pub fn can_have_stats(data_type: &DataType) -> bool {
        StatsBuilder::can_have_stats(data_type)
    }

    pub fn new(with_stats: Option<DataType>) -> Self {
        Self {
            offsets: {
                let mut offsets = UInt32Builder::with_capacity(200);
                offsets.append_value(0);
                offsets
            },
            null_count: UInt32Builder::with_capacity(200),
            stats: with_stats.map(StatsBuilder::new)
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn item_count(&self) -> u32 {
        self.offsets.values_slice().last().cloned().unwrap()
    }

    pub fn null_count(&self) -> u32 {
        self.null_count.values_slice().iter().sum()
    }

    pub fn push(&mut self, array: &dyn Array) {
        self.offsets.append_value(array.len() as u32 + self.item_count());
        self.null_count.append_value(array.null_count() as u32);
        if let Some(stats) = self.stats.as_mut() {
            stats.push(array)
        }
    }

    pub fn acc(&mut self, summary: &Summary) {
        assert_eq!(
            self.stats.as_ref().map(|s| &s.data_type),
            summary.stats.as_ref().map(|s| s.max.data_type())
        );
        self.offsets.append_value(self.item_count() + summary.item_count());
        self.null_count.append_value(summary.null_count.values().iter().sum());
        if let Some(self_stats) = self.stats.as_mut() {
            let other_stats = summary.stats.as_ref().unwrap();
            self_stats.acc(other_stats)
        }
    }

    pub fn finish(&mut self) -> Summary {
        Summary {
            offsets: {
                let offsets = self.offsets.finish();
                self.offsets.append_value(0);
                offsets
            },
            null_count: self.null_count.finish(),
            stats: self.stats.as_mut().map(|s| s.finish())
        }
    }
}