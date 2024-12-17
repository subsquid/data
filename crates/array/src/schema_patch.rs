use crate::schema_metadata::{print_sort_key, SQD_SORT_KEY};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use std::collections::HashMap;
use std::sync::Arc;


struct NewSchema {
    fields: Vec<FieldRef>,
    metadata: HashMap<String, String>
}


pub struct SchemaPatch {
    original: SchemaRef,
    new_schema: Option<NewSchema>
}


impl SchemaPatch {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            original: schema,
            new_schema: None
        }
    }

    pub fn fields(&self) -> &[FieldRef] {
        match self.new_schema.as_ref() {
            Some(new_schema) => &new_schema.fields,
            None => self.original.fields()
        }
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        match self.new_schema.as_ref() {
            Some(new_schema) => &new_schema.metadata,
            None => self.original.metadata()
        }
    }

    pub fn find_by_name(&self, name: &str) -> Option<(usize, FieldRef)> {
        self.fields().iter()
            .enumerate()
            .find_map(|(i, f)| {
                (f.name() == name).then_some((i, f.clone()))
            })
    }
    
    pub fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.new_schema_mut().metadata
    }

    pub fn set_field_type(&mut self, index: usize, ty: DataType) {
        let field = self.original.field(index);
        if field.data_type() == &ty {
            return;
        }
        let new_field = Field::new(
            field.name(),
            ty,
            field.is_nullable()
        );
        self.set_field(index, Arc::new(new_field))
    }
    
    pub fn insert_metadata(&mut self, key: impl ToString, val: impl ToString) {
        let key = key.to_string();
        let val = val.to_string();
        if self.metadata().get(&key) != Some(&val) {
            self.metadata_mut().insert(key, val);
        }
    }

    pub fn set_sort_key(&mut self, key: &[usize]) {
        let num_columns = self.fields().len();
        assert!(key.iter().all(|i| *i < num_columns));
        let key = print_sort_key(key);
        if self.metadata().get(SQD_SORT_KEY) != Some(&key) {
            self.metadata_mut().insert(SQD_SORT_KEY.to_string(), key);
        }
    }

    pub fn set_field(&mut self, index: usize, field: FieldRef) {
        let new_schema = self.new_schema_mut();
        new_schema.fields[index] = field
    }

    fn new_schema_mut(&mut self) -> &mut NewSchema {
        if self.new_schema.is_none() {
            self.new_schema.replace(NewSchema {
                fields: self.original.fields().to_vec(),
                metadata: self.original.metadata().clone()
            });
        }
        self.new_schema.as_mut().unwrap()
    }

    pub fn finish(self) -> SchemaRef {
        self.new_schema.map(|new_schema| {
            let schema = Schema::new_with_metadata(
                new_schema.fields,
                new_schema.metadata
            );
            Arc::new(schema)
        }).unwrap_or(self.original)
    }
}