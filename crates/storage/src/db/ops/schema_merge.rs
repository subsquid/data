use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use sqd_array::item_index_cast::common_item_index_type;
use sqd_array::util::SchemaPatch;
use std::sync::Arc;


pub fn can_merge_schema(base: &[FieldRef], schema: &Schema) -> bool {
    if base.len() != schema.fields().len() {
        return false
    }
    if base == schema.fields().as_ref() {
        return true
    }
    schema.fields().iter().all(|new_field| {
        base.iter()
            .find(|f| f.name() == new_field.name())
            .map(|f| {
                if f == new_field || f.data_type() == new_field.data_type() {
                    true
                } else {
                    common_item_index_type(f.data_type(), new_field.data_type()).is_some()
                }
            })
            .unwrap_or(false)
    })
}


pub fn merge_schema(base: &mut SchemaPatch, schema: &Schema) {
    if base.fields() == schema.fields().as_ref() {
        return
    }
    for new_field in schema.fields().iter() {
        let (bi, bf) = base.find_by_name(new_field.name())
            .expect("schemas to merge are not compatible");

        if &bf == new_field {
            continue
        }
        let new_field = merge_fields(&bf, new_field).expect("schemas to merge are not compatible");
        base.set_field(bi, new_field)
    }
}


fn merge_fields(a: &Field, b: &Field) -> Option<FieldRef> {
    assert_eq!(a.name(), b.name());
    let data_type = if a.data_type() == b.data_type() {
        Some(a.data_type().clone())
    } else {
        common_item_index_type(a.data_type(), b.data_type())
    };
    data_type.map(|data_type| {
        let field = Field::new(
            a.name(),
            data_type,
            a.is_nullable() || b.is_nullable()
        );
        Arc::new(field)
    })
}


/// DataType equality, that ignores list item names and nullability
pub fn data_types_equal(a: &DataType, b: &DataType) -> bool {
    a == b || match (a, b) { 
        (DataType::List(a), DataType::List(b)) => data_types_equal(a.data_type(), b.data_type()),
        _ => false
    }
}