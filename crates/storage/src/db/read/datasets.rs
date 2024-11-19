use crate::db::data::Dataset;
use crate::db::DatasetLabel;
use crate::kv::KvReadCursor;
use anyhow::Context;


pub fn list_all_datasets<C: KvReadCursor>(
    mut cursor: C
) -> impl Iterator<Item=anyhow::Result<Dataset>>
{
    let mut first_seek = true;
    
    let mut next = move || -> anyhow::Result<Option<Dataset>> {
        if first_seek {
            cursor.seek_first()?;
            first_seek = false;
        } else {
            cursor.next()?;
        }
     
        if !cursor.is_valid() {
            return Ok(None)
        }
        
        let id = borsh::from_slice(cursor.key())
            .context("invalid key in datasets CF")?;
        
        let label: DatasetLabel = borsh::from_slice(cursor.value())
            .with_context(|| {
                format!("invalid dataset label under id {}", id)
            })?;
        
        Ok(Some(Dataset {
            id,
            kind: label.kind,
            version: label.version
        }))
    };
    
    std::iter::from_fn(move || next().transpose())
}