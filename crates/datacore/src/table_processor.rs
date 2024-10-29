use crate::downcast::Downcast;
use arrow::datatypes::SchemaRef;
use sqd_dataset::TableDescription;


pub struct TableProcessor {
    schema: SchemaRef,
    downcast: Option<Downcast>
}


impl TableProcessor {
    pub fn new(schema: SchemaRef, desc: &TableDescription) -> anyhow::Result<Self> {
        todo!()
    }
}
