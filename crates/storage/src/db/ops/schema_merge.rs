use anyhow::anyhow;
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use sqd_array::item_index_cast::common_item_index_type;
use sqd_array::schema_metadata::get_sort_key;
use sqd_array::schema_patch::SchemaPatch;
use std::sync::Arc;


pub fn can_merge_schemas(a: &Schema, b: &Schema) -> bool {
    if a == b {
        return true
    }

    if a.fields().len() != b.fields().len() {
        return false
    }

    let (a_sort_key, b_sort_key) = match (get_sort_key(a), get_sort_key(b)) {
        (Ok(a), Ok(b)) => (a, b),
        _ => return false
    };

    if a_sort_key.len() != b_sort_key.len() {
        return false
    }

    for (ai, bi) in a_sort_key.iter().zip(b_sort_key.iter()) {
        if a.field(*ai).name() != b.field(*bi).name() {
            return false
        }    
    }
    
    b.fields().iter().all(|bf| {
        a.fields().iter()
            .find(|f| f.name() == bf.name())
            .map(|af| if af.data_type() == bf.data_type() {
                true
            } else {
                common_item_index_type(af.data_type(), bf.data_type()).is_some()
            })
            .unwrap_or(false)
    })
}


pub fn merge_schema(base: &mut SchemaPatch, schema: &Schema) -> anyhow::Result<()> {
    if base.fields() == schema.fields().as_ref() {
        return Ok(())
    }
    for new_field in schema.fields().iter() {
        let (bi, bf) = base
            .find_by_name(new_field.name())
            .ok_or_else(|| {
                anyhow!("field `{}` does not exist in the base schema", new_field.name())
            })?;
        
        if &bf == new_field {
            continue
        }
        
        let new_field = merge_fields(&bf, new_field)
            .ok_or_else(|| {
                anyhow!(
                    "failed to merge field `{}`: data types {} and {} are not compatible", 
                    new_field.name(),
                    bf.data_type(),
                    new_field.data_type()
                )
            })?;
        
        base.set_field(bi, new_field)
    }
    Ok(())
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