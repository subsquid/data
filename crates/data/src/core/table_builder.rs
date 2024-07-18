use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;


pub struct PreparedTable<'a> {
    pub schema: SchemaRef,
    pub max_block_number: u64,
    pub max_item_index: u64,
    pub num_rows: usize,
    pub rows: Box<dyn Iterator<Item=RecordBatch> + 'a>
}


#[macro_export]
macro_rules! table_builder {
    (
        $name:ident {
            $($field:ident : $builder:ident,)*
        }

        description($desc:ident) $desc_cb:expr
    ) => {
        pub struct $name {
            $(
                $field: $builder,
            )*
        }

        impl $name {
            pub fn table_description() -> &'static sqd_dataset::TableDescription {
                lazy_static::lazy_static! {
                    static ref DESC: sqd_dataset::TableDescription = {
                        let mut $desc = sqd_dataset::TableDescription::default();
                        {
                            $desc_cb
                        };
                        $desc
                    };
                }
                &DESC
            }

            pub fn schema() -> arrow::datatypes::SchemaRef {
                use std::sync::Arc;
                use arrow::datatypes::*;
                use crate::core::ArrowDataType;

                lazy_static::lazy_static! {
                    static ref SCHEMA: SchemaRef = {
                        Arc::new(Schema::new(vec![
                            $(
                                Arc::new(
                                    Field::new(stringify!($field), $builder::data_type(), true)
                                ),
                            )*
                        ]))
                    };
                }

                SCHEMA.clone()
            }

            pub fn downcast_columns() -> &'static (Vec<usize>, Vec<usize>) {
                lazy_static::lazy_static! {
                    static ref COLS: (Vec<usize>, Vec<usize>) = {
                        let desc = $name::table_description();
                        let schema = $name::schema();

                        let block_number = desc.downcast.block_number.iter().map(|&name| {
                            schema.index_of(name).unwrap()
                        }).collect();

                        let item_index = desc.downcast.item_index.iter().map(|&name| {
                            schema.index_of(name).unwrap()
                        }).collect();

                        (block_number, item_index)
                    };
                }
                &COLS
            }

            pub fn finish(&mut self) -> crate::core::PreparedTable<'_> {
                use arrow::array::*;

                let columns = vec![
                    $(
                    arrow::array::ArrayBuilder::finish(&mut self.$field),
                    )*
                ];

                let schema = Self::schema();

                let mut record_batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
                
                let sort_key = &Self::table_description().sort_key; 
                if sort_key.len() > 0 {
                    let by = sort_key.iter().map(|name| name.to_string()).collect();
                    record_batch = sqd_polars::arrow::sort_record_batch(
                        &record_batch,
                        by
                    ).unwrap()
                }

                let downcast_columns = Self::downcast_columns();
                let mut max_block_number = 0u64;
                let mut max_item_index = 0u64;

                for col in downcast_columns.0.iter().copied() {
                    let max = crate::core::downcast::get_max(record_batch.column(col));
                    max_block_number = std::cmp::max(max_block_number, max)
                }

                for col in downcast_columns.1.iter().copied() {
                    let max = crate::core::downcast::get_max(record_batch.column(col));
                    max_item_index = std::cmp::max(max_item_index, max)
                }

                crate::core::PreparedTable {
                    schema,
                    max_block_number,
                    max_item_index,
                    num_rows: record_batch.num_rows(),
                    rows: Box::new(std::iter::once(record_batch))
                }
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    $(
                        $field: $builder::default(),
                    )*
                }
            }
        }
    };
}