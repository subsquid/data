
#[macro_export]
macro_rules! chunk_builder {
    (
        $name:ident {
            $($field:ident : $table_builder:ident,)*
        }
    ) => {
        pub struct $name {
            $(
                $field: $table_builder,
            )*
        }

        impl $name {
            pub fn dataset_description() -> sqd_dataset::DatasetDescriptionRef {
                use sqd_dataset::*;

                lazy_static::lazy_static! {
                    static ref DESC: DatasetDescriptionRef = {
                        let mut desc = DatasetDescription::default();
                        $(
                        desc.tables.insert(
                            stringify!($field),
                            $table_builder::table_description().clone()
                        );
                        )*
                        std::sync::Arc::new(desc)
                    };
                }

                DESC.clone()
            }

            pub fn finish(&mut self) -> std::collections::HashMap<sqd_primitives::Name, crate::core::PreparedTable<'_>> {
                let mut dowcast = crate::core::downcast::Downcast::default();

                let tables = [
                    $(
                        (
                            stringify!($field),
                            $table_builder::downcast_columns(),
                            self.$field.finish()
                        ),
                    )*
                ];

                tables.iter().for_each(|rec| {
                    dowcast.reg_block_number(rec.2.max_block_number);
                    dowcast.reg_item_index(rec.2.max_item_index);
                });

                tables.into_iter().map(move |(name, downcast_columns, mut table)| {
                    let dc = dowcast.clone();
                    
                    table.schema = dc.downcast_schema(
                        table.schema,         
                        &downcast_columns.0,
                        &downcast_columns.1
                    );
                    
                    table.rows = Box::new(
                        table.rows.map(move |record_batch| {
                            dc.downcast_record_batch(
                                record_batch,
                                &downcast_columns.0,
                                &downcast_columns.1
                            )
                        })
                    );
                    (name, table)
                }).collect()
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    $(
                        $field: $table_builder::default(),
                    )*
                }
            }
        }
    };
}