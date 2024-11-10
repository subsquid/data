use super::{can_have_stats, Stats};
use arrow::datatypes::DataType;
use arrow_buffer::{ArrowNativeType, OffsetBuffer};
use sqd_array::builder::{AnyBuilder, ArrayBuilder};
use sqd_array::slice::{AnySlice, ListSlice, PrimitiveSlice, Slice};
use sqd_array::writer::ArrayWriter;


pub struct StatsBuilder {
    data_type: DataType,
    offsets: Vec<u32>,
    last_offset: u32,
    min: AnyBuilder,
    max: AnyBuilder
}


impl StatsBuilder {
    pub fn new(data_type: DataType) -> Self {
        assert!(can_have_stats(&data_type), "data type {} can't have stats", data_type);
        Self {
            offsets: vec![0],
            last_offset: 0,
            min: AnyBuilder::new(&data_type),
            max: AnyBuilder::new(&data_type),
            data_type
        }
    }

    pub fn finish(self) -> Stats {
        let offsets = unsafe {
            OffsetBuffer::new_unchecked(self.offsets.into())
        };
        Stats {
            offsets,
            min: self.min.finish(),
            max: self.max.finish()
        }
    }

    pub fn push_entry(&mut self, values: &AnySlice<'_>) {
        self.last_offset += values.len() as u32;
        self.offsets.push(self.last_offset);
        match &self.data_type {
            DataType::Int8 => self.push_primitive(values.as_i8()),
            DataType::Int16 => self.push_primitive(values.as_i16()),

            DataType::Int32 |
            DataType::Date32 |
            DataType::Time32(_) => self.push_primitive(values.as_i32()),

            DataType::Int64 |
            DataType::Timestamp(_, _) |
            DataType::Date64 |
            DataType::Time64(_) |
            DataType::Duration(_) |
            DataType::Interval(_) => self.push_primitive(values.as_i64()),

            DataType::UInt8 => self.push_primitive(values.as_u8()),
            DataType::UInt16 => self.push_primitive(values.as_u16()),
            DataType::UInt32 => self.push_primitive(values.as_u32()),
            DataType::UInt64 => self.push_primitive(values.as_u64()),

            DataType::Binary |
            DataType::Utf8 => self.push_binary(values.as_binary()),
            ty => unreachable!("unexpected arrow type - {}", ty)
        };
    }

    fn push_primitive<T: ArrowNativeType + Ord>(&mut self, values: PrimitiveSlice<'_, T>) {
        let min_max = values.min().and_then(|min| {
            let max = values.max().unwrap();
            Some((min, max))
        });

        let is_valid = min_max.is_some();
        self.min.nullmask(0).append(is_valid);
        self.max.nullmask(0).append(is_valid);

        let (min, max) = min_max.unwrap_or_default();
        self.min.native(1).push(min);
        self.max.native(1).push(max);
    }

    fn push_binary(&mut self, values: ListSlice<'_, &'_ [u8]>) {
        let min_max = values.min().and_then(|min| {
            let max = values.max().unwrap();
            Some((min, max))
        });

        match (&mut self.min, &mut self.max) {
            (AnyBuilder::Binary(min_builder), AnyBuilder::Binary(max_builder)) => {
                if let Some((min, max)) = min_max {
                    min_builder.append(min);
                    max_builder.append(max)
                } else {
                    min_builder.append_null();
                    max_builder.append_null()
                }
            },
            (AnyBuilder::String(min_builder), AnyBuilder::String(max_builder)) => {
                if let Some((min, max)) = min_max {
                    // FIXME: we should not crush here
                    min_builder.append(std::str::from_utf8(min).unwrap());
                    max_builder.append(std::str::from_utf8(max).unwrap())
                } else {
                    min_builder.append_null();
                    max_builder.append_null()
                }
            },
            _ => unreachable!()
        };


    }
}