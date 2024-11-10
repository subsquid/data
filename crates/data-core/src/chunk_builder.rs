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

            pub fn clear(&mut self) {
                $(
                self.$table.clear();
                )*
            }

            pub fn byte_size(&self) -> usize {
                0 $(+ self.$table.byte_size())*
            }

            pub fn dataset_description() -> sqd_dataset::DatasetDescriptionRef {
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
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}