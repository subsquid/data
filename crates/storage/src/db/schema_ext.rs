use anyhow::{ensure, Context};
use arrow::datatypes::{Schema, SchemaRef};
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;


pub const SQD_SORT_KEY: &'static str = "sqd_sort_key";


pub fn get_sort_key(schema: &Schema) -> anyhow::Result<Vec<usize>> {
    if let Some(key) = schema.metadata().get(SQD_SORT_KEY) {
        parse_sort_key(key, schema.fields().len()).with_context(|| {
            format!("invalid sqd_sort_key - `{}`", key)
        })
    } else {
        Ok(Vec::new())
    }
}


fn parse_sort_key(key: &str, num_columns: usize) -> anyhow::Result<Vec<usize>> {
    let indexes = key.split(',').map(|s| {
        s.parse()
    }).collect::<Result<Vec<usize>, _>>()?;
    
    ensure!(
        indexes.iter().all(|i| *i < num_columns),
        "sort key refers to non-existent column"
    );
    
    Ok(indexes)
}


pub fn print_sort_key(key: &[usize]) -> String {
    let mut out = String::new();
    if key.len() > 0 {
        write!(&mut out, "{}", key[0]).unwrap();
        for i in key[1..].iter() {
            write!(&mut out, ",{}", i).unwrap();
        }
    }
    out
}


pub fn strip_unknown_metadata(a: SchemaRef) -> SchemaRef {
    if a.metadata().is_empty() || a.metadata().len() == 1 && a.metadata().contains_key(SQD_SORT_KEY) {
        a
    } else {
        let fields = a.fields().clone();
        let mut new_metadata = HashMap::new();
        if let Some(key) = a.metadata().get(SQD_SORT_KEY) {
            new_metadata.insert(SQD_SORT_KEY.to_string(), key.clone());
        }
        let new_schema = Schema::new_with_metadata(fields, new_metadata);
        Arc::new(new_schema)
    }
}