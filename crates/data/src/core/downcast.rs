use std::cmp::max;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields};
use sqd_primitives::Name;


pub struct IndexDowncast {
    max: u64,
    limit: u64
}


impl IndexDowncast {
    pub fn new(limit: u64) -> Self {
        Self {
            max: limit,
            limit
        }
    }

    pub fn for_block_number() -> Self {
        Self::new(u16::MAX as u64 + 1)
    }

    pub fn for_item_index() -> Self {
        Self::new(256)
    }

    pub fn reg<T: Into<u64>>(&mut self, val: T) {
        self.max = max(self.max, val.into())
    }

    pub fn target_type(&self) -> DataType {
        if u8::try_from(self.max).is_ok() {
            return DataType::UInt8
        }
        if u16::try_from(self.max).is_ok() {
            return DataType::UInt16
        }
        if u32::try_from(self.max).is_ok() {
            return DataType::UInt32
        }
        DataType::UInt64
    }

    pub fn downcast_columns(&self, array: ArrayRef, column_names: &[Name]) -> ArrayRef {
        let target_type = self.target_type();
        let (fields, mut columns, nulls) = array.as_struct().clone().into_parts();
        let mut fields_vec: Vec<_> = fields.iter().cloned().collect();

        for name in column_names.iter().copied() {
            let pos = fields.iter()
                .position(|f| f.name().as_str() == name)
                .unwrap();

            if unwrap_list_type(fields[pos].data_type()) == target_type {
                continue
            }

            let new_column = cast_index(columns[pos].as_ref(), target_type.clone());

            fields_vec[pos] = Arc::new(
                Field::new(
                    fields[pos].name().clone(),
                    new_column.data_type().clone(),
                    fields[pos].is_nullable()
                )
            );

            columns[pos] = new_column;
        }

        Arc::new(
            StructArray::new(Fields::from(fields_vec), columns, nulls)
        )
    }

    pub fn reset(&mut self) {
        self.max = self.limit
    }
}


fn unwrap_list_type(ty: &DataType) -> DataType {
    match ty {
        DataType::List(t) => t.data_type().clone(),
       _ => ty.clone()
    }
}


fn cast_index(array: &dyn Array, to: DataType) -> ArrayRef {
    let target_type = match array.data_type() {
        DataType::List(_) => DataType::List(Arc::new(Field::new_list_field(to.clone(), true))),
        _ => to
    };
    arrow::compute::cast_with_options(
        array,
        &target_type,
        &arrow::compute::CastOptions {
            safe: false,
            ..arrow::compute::CastOptions::default()
        }
    ).unwrap()
}


pub struct Downcast {
    pub block_number: IndexDowncast,
    pub item: IndexDowncast
}


impl Downcast {
    pub fn reset(&mut self) {
        self.block_number.reset();
        self.item.reset()
    }
}


impl Default for Downcast {
    fn default() -> Self {
        Downcast {
            block_number: IndexDowncast::for_block_number(),
            item: IndexDowncast::for_item_index()
        }
    }
}