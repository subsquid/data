use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, UInt16Type, UInt32Type, UInt64Type, UInt8Type};
use arrow_buffer::ArrowNativeType;


#[derive(Copy, Clone)]
pub struct Downcast {
    block_number: u64,
    item_index: u64
}


impl Default for Downcast {
    fn default() -> Self {
        Self {
            block_number: u32::MAX as u64,
            item_index: u16::MAX as u64
        }
    }
}


impl Downcast {
    pub fn reg_block_number(&mut self, i: u64) {
        self.block_number = std::cmp::max(self.block_number, i)
    }

    pub fn reg_item_index(&mut self, i: u64) {
        self.item_index = std::cmp::max(self.item_index, i)
    }

    pub fn downcast_record_batch(
        &self,
        record_batch: RecordBatch,
        block_number_columns: &[usize],
        item_index_columns: &[usize]
    ) -> RecordBatch
    {
        let mut patch: Option<NewBatch> = None;
        let block_number_type = get_minimal_type(self.block_number);
        let item_type = get_minimal_type(self.item_index);

        for (columns, ty) in [
            (block_number_columns, block_number_type),
            (item_index_columns, item_type)
        ] {
            for idx in columns.iter().copied() {
                let array = record_batch.column(idx);
                let target_type = get_target_index_type(&array, ty.clone());
                if record_batch.schema().fields()[idx].data_type() != &target_type {
                    let new_array = cast(&array, target_type);
                    NewBatch::set_at_place(&mut patch, &record_batch, idx, new_array);
                }
            }
        }

        patch.map(|b| b.take()).unwrap_or(record_batch)
    }
}


fn get_target_index_type(array: &dyn Array, index_type: DataType) -> DataType {
    match array.data_type() {
        DataType::List(f) => DataType::List(
            Arc::new(Field::new_list_field(index_type, f.is_nullable()))
        ),
        _ => index_type
    }
}


pub fn get_max(array: &dyn Array) -> u64 {
    macro_rules! max {
        ($t:ty) => {
            arrow::compute::max(array.as_primitive::<$t>()).map_or(0, |val| val.to_i64().unwrap()) as u64
        };
    }
    
    match array.data_type() {
        DataType::UInt8 => max!(UInt8Type),
        DataType::UInt16 => max!(UInt16Type),
        DataType::UInt32 => max!(UInt32Type),
        DataType::UInt64 => max!(UInt64Type),
        DataType::List(f) if f.data_type().is_integer() => {
            let list_array = array.as_list::<i32>();
            get_max(list_array.values())
        },
        ty => panic!("invalid index array - {}", ty)
    }
}


fn get_minimal_type(max: u64) -> DataType {
    if u8::try_from(max).is_ok() {
        return DataType::UInt8
    }
    if u16::try_from(max).is_ok() {
        return DataType::UInt16
    }
    if u32::try_from(max).is_ok() {
        return DataType::UInt32
    }
    DataType::UInt64
}


fn cast(array: &dyn Array, to: DataType) -> ArrayRef {
    arrow::compute::cast_with_options(
        array,
        &to,
        &arrow::compute::CastOptions {
            safe: false,
            ..arrow::compute::CastOptions::default()
        }
    ).unwrap()
}


struct NewBatch {
    columns: Vec<ArrayRef>,
    fields: Vec<FieldRef>,
    schema: SchemaRef
}


impl NewBatch {
    fn set(&mut self, i: usize, new_array: ArrayRef) {
        self.fields[i] = Arc::new(
            Field::new(
                self.fields[i].name(),
                new_array.data_type().clone(),
                self.fields[i].is_nullable()
            )
        );
        self.columns[i] = new_array
    }

    fn set_at_place(place: &mut Option<Self>, record_batch: &RecordBatch, i: usize, array: ArrayRef) {
        if place.is_none() {
            let _ = std::mem::replace(place, Some(
                Self {
                    columns: record_batch.columns().to_vec(),
                    fields: record_batch.schema().fields().to_vec(),
                    schema: record_batch.schema()
                }
            ));
        }
        let new_batch = place.as_mut().unwrap();
        new_batch.set(i, array)
    }

    fn take(self) -> RecordBatch {
        let schema = Schema::new_with_metadata(
            self.fields,
            self.schema.metadata().clone()
        );
        RecordBatch::try_new(Arc::new(schema), self.columns).unwrap()
    }
}
