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
            _schema: arrow::datatypes::SchemaRef
        }
        
        impl $name {
            pub fn new() -> Self {
                use arrow::datatypes::{Schema, Field, FieldRef};
                use std::sync::Arc;
                use sqd_array::builder::*;
                
                $(
                let $field = $builder::default();
                )*
                
                let schema_fields: Vec<FieldRef> = vec![
                    $(
                        Arc::new(
                            Field::new(stringify!($field), $field.data_type(), true)
                        ),
                    )*
                ];
                
                let schema = Schema::new(schema_fields);
                
                Self {
                    $($field,)*
                    _schema: Arc::new(schema)
                }
            }
            
            pub fn table_description() -> &'static sqd_dataset::TableDescription {
                use std::sync::LazyLock;
                use sqd_dataset::TableDescription;
                
                static DESC: LazyLock<TableDescription> = LazyLock::new(|| {
                    let mut $desc = TableDescription::default();
                    {
                        $desc_cb
                    };
                    $desc
                });
                
                &DESC
            }
            
            #[inline]
            pub fn schema(&self) -> arrow::datatypes::SchemaRef {
                self._schema.clone()
            }
            
            pub fn len(&self) -> usize {
                _table_builder_len_impl!(self, $($field,)*)
            }
            
            pub fn byte_size(&self) -> usize {
                use sqd_array::builder::ArrayBuilder;
                0 $(+ self.$field.byte_size())*
            }
            
            pub fn clear(&mut self) {
                use sqd_array::builder::ArrayBuilder;
                $(
                self.$field.clear();
                )*
            }
            
            pub fn finish(self) -> arrow::array::RecordBatch {
                arrow::array::RecordBatch::try_new(
                    self._schema,
                    vec![
                        $(sqd_array::builder::ArrayBuilder::finish(self.$field),)*
                    ]
                ).unwrap()
            }
        }
        
        impl sqd_array::slice::AsSlice for $name {
            type Slice<'a> = sqd_array::slice::AnyTableSlice<'a>;
            
            fn as_slice(&self) -> Self::Slice<'_> {
                use sqd_array::slice::*;
                AnyTableSlice::new(
                    [
                        $(
                        AnySlice::from(self.$field.as_slice()),
                        )*
                    ].into()        
                )
            }
        }
        
        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}


#[macro_export]
macro_rules! _table_builder_len_impl {
    ($this:ident,) => { 0 };
    ($this:ident, $field:ident, $($rest:ident,)*) => {{
        use sqd_array::builder::ArrayBuilder;
        let len = $this.$field.len();
        $(
            assert_eq!(
                len, $this.$rest.len(), 
                "columns {} and {} have different lengths",
                stringify!($field), stringify!($rest)
            );
        )*
        len
    }};
}

// 
// table_builder! {
//     Transactions {
//         hash: StringBuilder,
//     }
//     
//     description(d) {}
// }