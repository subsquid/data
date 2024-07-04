#[macro_export]
macro_rules! struct_builder {
    ($name:ident {
        $($field:ident : $builder:ident,)*
    }) => {
        pub struct $name {
            $(
                pub $field: $builder,
            )*
            _nulls: arrow_buffer::NullBufferBuilder
        }

        impl crate::core::ArrowDataType for $name {
            fn data_type() -> arrow::datatypes::DataType {
                use arrow::datatypes::*;
                DataType::Struct(Self::data_fields())
            }
        }

        impl $name {
            pub fn data_fields() -> arrow::datatypes::Fields {
                use arrow::datatypes::*;
                use crate::core::ArrowDataType;

                lazy_static::lazy_static! {
                    static ref FIELDS: Fields = {
                        Fields::from(vec![
                            $(
                            Field::new(stringify!($field), $builder::data_type(), true),
                            )*
                        ])
                    };
                }

                FIELDS.clone()
            }

            pub fn append(&mut self, is_valid: bool) {
                self._nulls.append(is_valid)
            }

            pub fn append_null(&mut self) {
                self._nulls.append(false)
            }
        }

        impl arrow::array::ArrayBuilder for $name {
            fn len(&self) -> usize {
                self._nulls.len()
            }

            fn finish(&mut self) -> arrow::array::ArrayRef {
                let nulls = self._nulls.finish();
                let array = arrow::array::StructArray::new(
                    Self::data_fields(),
                    vec![
                        $(
                        arrow::array::ArrayBuilder::finish(&mut self.$field),
                        )*
                    ],
                    nulls
                );
                std::sync::Arc::new(array)
            }

            fn finish_cloned(&self) -> arrow::array::ArrayRef {
                let nulls = self._nulls.finish_cloned();
                let array = arrow::array::StructArray::new(
                    Self::data_fields(),
                    vec![
                        $(
                        arrow::array::ArrayBuilder::finish_cloned(&self.$field),
                        )*
                    ],
                    nulls
                );
                std::sync::Arc::new(array)
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                self
            }

            fn into_box_any(self: Box<Self>) -> Box<dyn std::any::Any> {
                self
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    _nulls: arrow_buffer::NullBufferBuilder::new(1024),
                    $(
                        $field: $builder::default(),
                    )*
                }
            }
        }
    };
}