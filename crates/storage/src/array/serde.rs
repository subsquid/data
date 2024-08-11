use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray};
use arrow::datatypes::DataType;

use sqd_array::{AnySlice, make_data_builder, Slice};


pub fn serialize_array(array: &dyn Array, out: &mut Vec<u8>) {
    AnySlice::from(array).write_page(out)
}


pub fn deserialize_array(bytes: &[u8], data_type: DataType) -> anyhow::Result<ArrayRef> {
    let mut builder = make_data_builder(&data_type);
    builder.push_page(bytes)?;
    Ok(builder.into_arrow_array(Some(data_type)))
}


pub fn deserialize_primitive_array<T: ArrowPrimitiveType>(bytes: &[u8]) -> anyhow::Result<PrimitiveArray<T>> {
    let array = deserialize_array(bytes, T::DATA_TYPE)?;
    Ok(array.as_primitive::<T>().clone())
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, BooleanArray, PrimitiveArray, StringArray, StructArray};
    use arrow::buffer::{BooleanBuffer, Buffer};
    use arrow::datatypes::{DataType, Field, Fields, UInt32Type};
    use sqd_array::make_data_builder;
    use crate::array::serde::{deserialize_array, serialize_array};


    fn test_ser_de(array: &dyn Array) {
        let mut bytes = Vec::new();
        serialize_array(array, &mut bytes);
        let de = deserialize_array(&bytes, array.data_type().clone()).unwrap();
        assert_eq!(de.as_ref(), array)
    }

    #[test]
    fn primitive() {
        test_ser_de(&PrimitiveArray::from(vec![
            Some(1u32),
            Some(2u32),
            None,
            Some(4u32)
        ]))
    }

    #[test]
    fn string_array_slicing() {
        let array = StringArray::from(vec![
            Some("foo"),
            Some("bar"),
            None,
            Some("asdfgh"),
            None,
            None,
            Some("hello world")
        ]);
        
        let mut page = Vec::new();
        serialize_array(&array, &mut page);
        
        let mut builder = make_data_builder(&DataType::Utf8);
        builder.push_page_ranges(&page, &[1..4]).unwrap();
        
        assert_eq!(
            builder.into_arrow_array(Some(DataType::Utf8)).as_ref(),
            &array.slice(1, 3)
        );
    }

    #[test]
    fn boolean_slicing() {
        let bitmask = "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP".as_bytes();

        let array = BooleanArray::from(
            BooleanBuffer::new(
                Buffer::from_slice_ref(bitmask),
                0, 
                bitmask.len() * 8
            )
        );

        let mut page = Vec::new();
        serialize_array(&array, &mut page);
        
        let range = 10..200;

        let mut builder = make_data_builder(&DataType::Boolean);
        builder.push_page_ranges(&page, &[range.clone()]).unwrap();

        assert_eq!(
            builder.into_arrow_array(None).as_ref(),
            &array.slice(range.start as usize, range.len())
        );
    }

    #[test]
    fn struct_array_with_offset() {
        let array = StructArray::new(
            Fields::from(vec![
                Field::new("a", DataType::UInt32, true),
                Field::new("b", DataType::Utf8, true),
            ]),
            vec![
                Arc::new(PrimitiveArray::<UInt32Type>::from(vec![
                    Some(0),
                    Some(1),
                    None,
                    Some(3)
                ])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("b"),
                    Some("c")
                ]))
            ],
            None
        ).slice(1, 2);

        test_ser_de(&array)
    }
}