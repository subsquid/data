#[macro_export]
macro_rules! chunk_builder {
    (
        $name:ident {
            $($table:ident : $builder:ident,)*
        }
    ) => {
        pub struct $name {
            $(
                $table: $builder,
            )*
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    $(
                    $table: $builder::default(),
                    )*
                }
            }
        }

        impl sqd_data_core::BaseBuilder for $name {
            fn clear(&mut self) {
                $(
                self.$table.clear();
                )*
            }

            fn byte_size(&self) -> usize {
                0 $(+ self.$table.byte_size())*
            }

            fn dataset_description(&self) -> sqd_dataset::DatasetDescriptionRef {
                use sqd_dataset::*;
                use std::sync::{Arc, LazyLock};

                static DESC: LazyLock<DatasetDescriptionRef> = LazyLock::new(|| {
                    let mut d = DatasetDescription::default();
                    $(
                    d.tables.insert(
                        stringify!($table),
                        $builder::table_description().clone()
                    );
                    )*
                    Arc::new(d)
                });

                DESC.clone()
            }

            fn chunk_processor(&self) -> sqd_data_core::ChunkProcessor {
                let mut tables = std::collections::BTreeMap::new();
                $(
                tables.insert(
                    stringify!($table),
                    self.$table.table_processor()
                );
                )*
                sqd_data_core::ChunkProcessor::new(tables)
            }

            fn as_slice(&self) -> std::collections::BTreeMap<&'static str, sqd_array::slice::AnyTableSlice<'_>> {
                use sqd_array::slice::*;
                let mut slice = std::collections::BTreeMap::new();
                $(
                slice.insert(
                    stringify!($table),
                    self.$table.as_slice()
                );
                )*
                slice
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}