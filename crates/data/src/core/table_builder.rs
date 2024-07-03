use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;


pub struct PreparedTable<'a> {
    pub schema: SchemaRef,
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

            fn normal_schema() -> arrow::datatypes::SchemaRef {
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

            fn downcast_columns() -> &'static (Vec<usize>, Vec<usize>) {
                lazy_static::lazy_static! {
                    static ref COLS: (Vec<usize>, Vec<usize>) = {
                        let desc = $name::table_description();
                        let schema = $name::normal_schema();

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

                let desc = Self::table_description();

                let columns = vec![
                    $(
                    arrow::array::ArrayBuilder::finish(&mut self.$field),
                    )*
                ];

                let schema = Self::normal_schema();

                let record_batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

                let downcast_columns = Self::downcast_columns();
                let mut downcast = crate::core::downcast::Downcast::default();

                for col in downcast_columns.0.iter().copied() {
                    downcast.reg_block_number(record_batch.column(col))
                }

                for col in downcast_columns.1.iter().copied() {
                    downcast.reg_item_index(record_batch.column(col))
                }

                let record_batch = downcast.downcast_record_batch(
                    record_batch,
                    &downcast_columns.0,
                    &downcast_columns.1
                );

                crate::core::PreparedTable {
                    schema,
                    rows: Box::new(std::iter::once(record_batch))
                }
            }
        }
    };
}