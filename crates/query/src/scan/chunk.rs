use crate::{primitives::Name, scan::scan::Scan};

pub trait Chunk: Send + Sync {
    fn scan_table(&self, name: Name) -> anyhow::Result<Scan<'_>>;
}
