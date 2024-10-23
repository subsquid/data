#[macro_export]
macro_rules! struct_builder {
    ($name:ident {
        $($field:ident : $builder:ident,)*
    }) => {
        pub struct $name {
            $(
                pub $field: $builder,
            )*
            _nulls: sqd_array::builder::nullmask::NullmaskBuilder,
            _fields: arrow::datatypes::Fields,
        }

        impl $name {
            pub fn new() -> Self {
                $(
                let $field = $builder::default();
                )*

                let fields = arrow::datatypes::Fields::from([
                    $(
                    std::sync::Arc::new(arrow::datatypes::Field::new(
                        stringify!($field),
                        sqd_array::builder::ArrayBuilder::data_type(&$field),
                        true
                    )),
                    )*
                ]);
                
                Self {
                    $( $field, )*
                    _nulls: sqd_array::builder::nullmask::NullmaskBuilder::new(0),
                    _fields: fields
                }
            }
            
            #[inline]
            pub fn append(&mut self, is_valid: bool) {
                self._nulls.append(is_valid)
            }

            #[inline]
            pub fn append_valid(&mut self) {
                self.append(true)
            }

            #[inline]
            pub fn append_null(&mut self) {
                self.append(false)
            }
            
            pub fn finish(self) -> arrow::array::StructArray {
                arrow::array::StructArray::new(
                    self._fields,
                    vec![
                        $(
                        sqd_array::builder::ArrayBuilder::finish(self.$field) ,
                        )*
                    ],
                    self._nulls.finish()
                )
            }
        }
        
        impl sqd_array::builder::ArrayBuilder for $name {
            fn data_type(&self) -> arrow::datatypes::DataType {
                arrow::datatypes::DataType::Struct(self._fields.clone())
            }
            
            fn len(&self) -> usize {
                self._nulls.len()
            }
            
            fn byte_size(&self) -> usize {
                use sqd_array::builder::ArrayBuilder;
                self._nulls.byte_size() $(+ self.$field.byte_size())*
            }
            
            fn clear(&mut self) {
                use sqd_array::builder::ArrayBuilder;
                self._nulls.clear();
                $(
                self.$field.clear();
                )*
            }
            
            fn finish(self) -> arrow::array::ArrayRef {
                std::sync::Arc::new(self.finish())
            }
        }
        
        impl sqd_array::slice::AsSlice for $name {
            type Slice<'a> = sqd_array::slice::AnyStructSlice<'a>;

            fn as_slice(&self) -> Self::Slice<'_> {
                use sqd_array::slice::*;
                AnyStructSlice::new(
                    self._nulls.as_slice(),
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


// struct_builder! {
//     Hello {
//         msg: StringBuilder,
//     }
// }