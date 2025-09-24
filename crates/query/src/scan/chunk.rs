use crate::primitives::Name;
use crate::scan::scan::Scan;


pub trait Chunk: Send + Sync {
    fn scan_table(&self, name: Name) -> anyhow::Result<Scan<'_>>;
}


#[derive(Debug)]
pub struct TableDoesNotExist {
    pub table_name: Name
}


impl TableDoesNotExist {
    pub fn new(table_name: Name) -> Self {
        Self {
            table_name
        }
    }
}


impl std::fmt::Display for TableDoesNotExist {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "table '{}' does not exist", self.table_name)
    }
}


impl std::error::Error for TableDoesNotExist {}