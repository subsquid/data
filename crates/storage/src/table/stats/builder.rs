use super::{can_have_stats, Stats};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use arrow_buffer::{ArrowNativeType, NullBuffer, OffsetBuffer};
use sqd_array::builder::nullmask::NullmaskBuilder;
use sqd_array::builder::{AnyBuilder, ArrayBuilder};
use sqd_array::slice::{AnySlice, ListSlice, PrimitiveSlice, Slice};


pub struct StatsBuilder {
    data_type: DataType,
    offsets: Vec<u32>,
    nulls: NullmaskBuilder,
    min: AnyBuilder,
    max: AnyBuilder
}


impl StatsBuilder {
    pub fn new(data_type: DataType) -> Self {
        assert!(can_have_stats(&data_type), "data type {} can't have stats", data_type);
        Self {
            offsets: vec![0],
            nulls: NullmaskBuilder::new(0),
            min: AnyBuilder::new(&data_type),
            max: AnyBuilder::new(&data_type),
            data_type
        }
    }

    pub fn finish(self) -> Stats {
        let offsets = unsafe {
            OffsetBuffer::new_unchecked(self.offsets.into())
        };
        let nulls = self.nulls.finish();
        Stats {
            offsets,
            min: set_null_mask(self.min.finish(), nulls.clone()),
            max: set_null_mask(self.max.finish(), nulls)
        }
    }

    pub fn push_entry(&mut self, values: &AnySlice<'_>) {
        self.offsets.push(values.len() as u32);
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

        self.nulls.append(min_max.is_some());

        let (min, max) = min_max.unwrap_or_default();
        PrimitiveSlice::new(&[min], None).write(&mut self.min).unwrap();
        PrimitiveSlice::new(&[max], None).write(&mut self.max).unwrap()
    }

    fn push_binary(&mut self, values: ListSlice<'_, &'_ [u8]>) {
        let min_max = values.min().and_then(|min| {
            let max = values.max().unwrap();
            Some((min, max))
        });

        self.nulls.append(min_max.is_some());

        let (min, max) = min_max.unwrap_or_default();

        let (min_builder, max_builder) = match (&mut self.min, &mut self.max) {
            (AnyBuilder::Binary(min), AnyBuilder::Binary(max)) => (min, max),
            _ => unreachable!()
        };

        min_builder.append(min);
        max_builder.append(max)
    }
}


fn set_null_mask(array: ArrayRef, nulls: Option<NullBuffer>) -> ArrayRef {
    if let Some(nulls) = nulls {
        let data = array.to_data()
            .into_builder()
            .nulls(Some(nulls))
            .build()
            .unwrap();
        arrow::array::make_array(data)
    } else {
        array
    }
}