use std::sync::Arc;

use arrow::array::{ArrayBuilder, ArrowPrimitiveType, BinaryBuilder, ListBuilder, PrimitiveBuilder, StringBuilder, BooleanBuilder};
use arrow::datatypes::{DataType, Field};
use arrow_buffer::ArrowNativeType;

macro_rules! struct_builder {
    ($name:ident {
        $($field:ident : $builder:ident,)*
    }) => {
        pub struct $name {
            $(
                pub $field: $builder,
            )*
            _nulls: arrow_buffer::NullBufferBuilder,
            _fields: arrow::datatypes::Fields
        }

        impl Builder for $name {
            fn byte_size(&self) -> usize {
                let mut size = 0;
                $(
                size += self.$field.byte_size();
                )*
                size
            }

            fn data_type() -> arrow::datatypes::DataType {
                use arrow::datatypes::*;
                DataType::Struct(Self::data_fields())
            }
        }

        impl $name {
            pub fn data_fields() -> arrow::datatypes::Fields {
                use arrow::datatypes::*;
                Fields::from(vec![
                    $(
                    Field::new(stringify!($field), $builder::data_type(), true),
                    )*
                ])
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
                    self._fields.clone(),
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
                    self._fields.clone(),
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
                    _fields: Self::data_fields(),
                    $(
                        $field: $builder::default(),
                    )*
                }
            }
        }
    };
}
pub(crate) use struct_builder;


pub trait Builder {
    fn byte_size(&self) -> usize;

    fn data_type() -> DataType;
}


impl <T: ArrowPrimitiveType> Builder for PrimitiveBuilder<T> {
    fn byte_size(&self) -> usize {
        self.len() * T::Native::get_byte_width()
    }

    fn data_type() -> DataType {
        T::DATA_TYPE
    }
}


impl Builder for StringBuilder {
    fn byte_size(&self) -> usize {
        self.values_slice().len()
    }

    fn data_type() -> DataType {
        DataType::Utf8
    }
}


impl Builder for BinaryBuilder {
    fn byte_size(&self) -> usize {
        self.values_slice().len()
    }

    fn data_type() -> DataType {
        DataType::Binary
    }
}


impl Builder for BooleanBuilder {
    fn byte_size(&self) -> usize {
        1
    }

    fn data_type() -> DataType {
        DataType::Boolean
    }
}


impl <T: ArrayBuilder + Builder> Builder for ListBuilder<T> {
    fn byte_size(&self) -> usize {
        self.values_ref().byte_size()
    }

    fn data_type() -> DataType {
        let field = Field::new_list_field(T::data_type(), true);
        DataType::List(Arc::new(field))
    }
}