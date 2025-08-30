use crate::{PreparedTable, TableProcessor};
use anyhow::anyhow;
use sqd_array::slice::AnyTableSlice;
use std::collections::BTreeMap;


type Name = &'static str;


pub struct ChunkProcessor {
    tables: BTreeMap<Name, TableProcessor>
}


impl ChunkProcessor {
    pub fn new(tables: BTreeMap<Name, TableProcessor>) -> ChunkProcessor {
        Self { tables }
    }

    pub fn push_table(&mut self, name: &str, records: &AnyTableSlice<'_>) -> anyhow::Result<()> {
        let processor = self.tables.get_mut(name).ok_or_else(|| {
            anyhow!("table '{}' is not present in the chunk", name)
        })?;
        processor.push_batch(records)
    }

    pub fn byte_size(&self) -> usize {
        self.tables.values().map(|t| t.byte_size()).sum()
    }

    pub fn max_num_rows(&self) -> usize {
        self.tables.values().map(|t| t.num_rows()).max().unwrap_or(0)
    }

    pub fn finish(self) -> anyhow::Result<PreparedChunk> {
        self.tables
            .into_iter()
            .map(|(name, table)| {
                table.finish().map(|table| (name, table))
            })
            .collect::<anyhow::Result<_>>()
            .map(|tables| PreparedChunk {
                tables
            })
    }
}


#[derive(Default)]
pub struct PreparedChunk {
    pub tables: BTreeMap<Name, PreparedTable>
}