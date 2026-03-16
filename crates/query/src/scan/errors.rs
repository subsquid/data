use crate::primitives::Name;

#[derive(Debug)]
pub struct TableDoesNotExist {
    pub table_name: Name,
}

impl TableDoesNotExist {
    pub fn new(table_name: Name) -> Self {
        Self { table_name }
    }
}

impl std::fmt::Display for TableDoesNotExist {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "table '{}' does not exist", self.table_name)
    }
}

impl std::error::Error for TableDoesNotExist {}

#[derive(Debug)]
pub struct ColumnDoesNotExist {
    pub column_name: Name,
    pub table_name: String,
}

impl ColumnDoesNotExist {
    pub fn new(table_name: String, column_name: Name) -> Self {
        Self {
            table_name,
            column_name,
        }
    }
}

impl std::fmt::Display for ColumnDoesNotExist {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "column '{}' is not found in '{}'",
            self.column_name, self.table_name
        )
    }
}

impl std::error::Error for ColumnDoesNotExist {}
