use std::collections::HashMap;


type Name = &'static str;


pub type DatasetDescriptionRef = std::sync::Arc<DatasetDescription>;


#[derive(Clone, Debug, Default)]
pub struct DatasetDescription {
    pub tables: HashMap<Name, TableDescription>
}


#[derive(Clone, Debug, Default)]
pub struct TableDescription {
    pub downcast: DowncastColumns,
    pub sort_key: Vec<Name>,
    pub options: TableOptions
}


#[derive(Clone, Debug, Default)]
pub struct DowncastColumns {
    pub block_number: Vec<Name>,
    pub item_index: Vec<Name>
}


#[derive(Clone, Debug)]
pub struct TableOptions {
    pub column_options: HashMap<Name, ColumnOptions>,
    pub default_page_size: usize
}


impl Default for TableOptions {
    fn default() -> Self {
        Self {
            column_options: HashMap::new(),
            default_page_size: 32 * 1024
        }
    }
}


#[derive(Clone, Debug)]
pub struct ColumnOptions {
    pub with_stats: bool
}
