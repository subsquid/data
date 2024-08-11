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
    pub default_page_size: usize,
    pub row_group_size: usize
}


impl TableOptions {
    pub fn has_stats(&self, name: &str) -> bool {
        self.column_options.get(name).map_or(false, |c| c.with_stats)
    }
    
    pub fn add_stats(&mut self, name: Name) {
        let options = self.column_options.entry(name).or_insert(ColumnOptions::default());
        options.with_stats = true
    }
}


impl Default for TableOptions {
    fn default() -> Self {
        Self {
            column_options: HashMap::new(),
            default_page_size: 256 * 1024,
            row_group_size: 20000
        }
    }
}


#[derive(Clone, Debug, Default)]
pub struct ColumnOptions {
    pub with_stats: bool
}
