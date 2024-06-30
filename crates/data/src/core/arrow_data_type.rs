use std::sync::Arc;

use arrow::array::{ArrayBuilder, ArrowPrimitiveType, BinaryBuilder, BooleanBuilder, ListBuilder, PrimitiveBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field};


pub trait ArrowDataType {
    fn data_type() -> DataType;
}


impl <T: ArrowPrimitiveType> ArrowDataType for PrimitiveBuilder<T> {
    fn data_type() -> DataType {
        T::DATA_TYPE
    }
}


impl ArrowDataType for StringBuilder {
    fn data_type() -> DataType {
        DataType::Utf8
    }
}


impl ArrowDataType for BinaryBuilder {
    fn data_type() -> DataType {
        DataType::Binary
    }
}


impl ArrowDataType for BooleanBuilder {
    fn data_type() -> DataType {
        DataType::Boolean
    }
}


impl <T: ArrowDataType + ArrayBuilder> ArrowDataType for ListBuilder<T> {
    fn data_type() -> DataType {
        let field = Field::new_list_field(T::data_type(), true);
        DataType::List(Arc::new(field))
    }
}