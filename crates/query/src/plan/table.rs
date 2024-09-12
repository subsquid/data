use std::collections::HashMap;
use crate::primitives::{Name, RowWeight};


pub enum ColumnWeight {
    Fixed(RowWeight),
    Stored(Name)
}


pub struct Table {
    pub name: Name,
    pub primary_key: Vec<Name>,
    pub column_weights: HashMap<Name, ColumnWeight>,
    pub result_item_name: Name,
    pub children: HashMap<Name, Vec<Name>>
}


impl Table {
    pub fn set_weight(&mut self, column: Name, weight: RowWeight) -> &mut Self {
        self.column_weights.insert(column, ColumnWeight::Fixed(weight));
        self
    }

    pub fn set_weight_column(&mut self, column: Name, weight_column: Name) -> &mut Self {
        self.column_weights.insert(column, ColumnWeight::Stored(weight_column));
        self
    }

    pub fn set_result_item_name(&mut self, name: Name) -> &mut Self {
        self.result_item_name = name;
        self
    }

    pub fn add_child(&mut self, name: Name, key: Vec<Name>) -> &mut Self {
        assert_eq!(key.len(), self.primary_key.len());
        self.children.insert(name, key);
        self
    }
}


pub struct TableSet {
    tables: Vec<Table>
}


impl TableSet {
    pub fn new() -> TableSet {
        TableSet {
            tables: Vec::new()
        }
    }

    pub fn get(&self, name: Name) -> &Table {
        &self.tables[self.get_index(name)]
    }

    pub fn get_index(&self, name: Name) -> usize {
        for (idx, table) in self.tables.iter().enumerate() {
            if table.name == name {
                return idx
            }
        }
        panic!("table {} is not defined", name)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Table> {
        self.tables.iter()
    }

    pub fn add_table(&mut self, name: Name, pk: Vec<Name>) -> &mut Table {
        assert!(
            self.tables.iter().position(|t| t.name == name).is_none(),
            "table '{}' was already defined", name
        );
        assert!(
            pk.len() > 0,
            "primary key of all tables must start with a block number"
        );
        assert!(
            self.tables.len() > 0 || pk.len() == 1,
            "block header table must be defined first"
        );

        self.tables.push(Table {
            name,
            primary_key: pk,
            column_weights: HashMap::new(),
            result_item_name: name,
            children: HashMap::new()
        });

        self.tables.last_mut().unwrap()
    }
}