use crate::ChunkProcessor;
use sqd_array::slice::AnyTableSlice;
use std::collections::BTreeMap;


pub trait BlockChunkBuilder: ChunkBuilder {
    type Block;

    fn push(&mut self, block: &Self::Block) -> anyhow::Result<()>;
}


pub trait ChunkBuilder {
    fn dataset_description(&self) -> sqd_dataset::DatasetDescriptionRef;

    fn clear(&mut self);

    fn byte_size(&self) -> usize;

    fn max_num_rows(&self) -> usize;

    fn as_slice_map(&self) -> BTreeMap<&'static str, AnyTableSlice<'_>>;

    fn new_chunk_processor(&self) -> ChunkProcessor;

    fn submit_to_processor(&self, processor: &mut ChunkProcessor) -> anyhow::Result<()>;
}


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

            pub fn max_num_rows(&self) -> usize {
                let mut len = 0;
                $(
                len = std::cmp::max(len, self.$table.len());
                )*
                len
            }

            pub fn as_slice_map(&self) -> std::collections::BTreeMap<&'static str, sqd_array::slice::AnyTableSlice<'_>> {
                use sqd_array::slice::*;
                let mut map = std::collections::BTreeMap::new();
                $(
                map.insert(
                    stringify!($table),
                    self.$table.as_slice()
                );
                )*
                map
            }

            pub fn new_chunk_processor(&self) -> sqd_data_core::ChunkProcessor {
                let mut tables = std::collections::BTreeMap::new();
                $(
                tables.insert(
                    stringify!($table),
                    self.$table.new_table_processor()
                );
                )*
                sqd_data_core::ChunkProcessor::new(tables)
            }

            pub fn submit_to_processor(&self, processor: &mut sqd_data_core::ChunkProcessor) -> anyhow::Result<()> {
                use sqd_array::slice::*;
                $(
                processor.push_table(stringify!($table), &self.$table.as_slice())?;
                )*
                Ok(())
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

        impl sqd_data_core::ChunkBuilder for $name {
            fn dataset_description(&self) -> sqd_dataset::DatasetDescriptionRef {
                Self::dataset_description()
            }

            fn clear(&mut self) {
                self.clear()
            }

            fn byte_size(&self) -> usize {
                self.byte_size()
            }

            fn max_num_rows(&self) -> usize {
                self.max_num_rows()
            }

            fn as_slice_map(&self) -> std::collections::BTreeMap<&'static str, sqd_array::slice::AnyTableSlice<'_>> {
                self.as_slice_map()
            }

            fn new_chunk_processor(&self) -> sqd_data_core::ChunkProcessor {
                self.new_chunk_processor()
            }

            fn submit_to_processor(&self, processor: &mut sqd_data_core::ChunkProcessor) -> anyhow::Result<()> {
                self.submit_to_processor(processor)
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}
