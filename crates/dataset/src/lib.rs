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
        self.column_options.get(name).map_or(false, |c| c.stats_enable)
    }
    
    pub fn get_stats_partition(&self, name: &str) -> Option<usize> {
        self.column_options.get(name).map(|c| c.stats_partition)
    }
    
    pub fn add_stats(&mut self, name: Name) {
        let options = self.column_options.entry(name).or_default();
        options.stats_enable = true
    }
}


impl Default for TableOptions {
    fn default() -> Self {
        Self {
            column_options: HashMap::new(),
            default_page_size: 128 * 1024,
            row_group_size: 20000
        }
    }
}


#[derive(Clone, Debug)]
pub struct ColumnOptions {
    pub stats_enable: bool,
    pub stats_partition: usize
}


impl Default for ColumnOptions {
    fn default() -> Self {
        Self {
            stats_enable: false,
            stats_partition: 4000
        }
    }
}