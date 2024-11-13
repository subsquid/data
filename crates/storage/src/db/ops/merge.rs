use crate::db::SnapshotTableReader;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use sqd_array::util::build_field_offsets;
use sqd_array::writer::ArrayWriter;
use sqd_dataset::TableDescription;


fn merge_tables(
    dst: &mut impl ArrayWriter,
    dst_schema: SchemaRef,
    dst_desc: &TableDescription,
    chunks: &[SnapshotTableReader<'_>]
) -> anyhow::Result<()> 
{
    let column_offsets = build_field_offsets(dst_schema.fields(), 0);
    
    if dst_desc.sort_key.is_empty() {
        
    }
    
    Ok(())
}


fn merge_column(
    dst: &mut impl ArrayWriter,
    dst_data_type: &DataType,
    name: &str,
    chunks: &[SnapshotTableReader<'_>]
) -> anyhow::Result<()>
{
    todo!()
}


fn merge_schemas(a: SchemaRef, b: SchemaRef) -> Option<SchemaRef> {
    if a.fields() == b.fields() {
        Some(strip_schema_metadata(a))
    } else {
        None
    }
}


fn strip_schema_metadata(a: SchemaRef) -> SchemaRef {
    if a.metadata().is_empty() {
        a
    } else {
        SchemaRef::new(Schema::new(a.fields().clone()))
    }
}