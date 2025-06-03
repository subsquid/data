use crate::primitives::Name;
use crate::scan::{and, bloom_filter, col_eq, col_gt_eq, col_in_list, col_lt_eq, IntoArrowArray, IntoArrowScalar, RowPredicateRef};
use arrow::array::StringArray;
use std::hash::Hash;


macro_rules! item_field_selection {
    (
        $type_name:ident { $(  $field:ident, )* }
        project($fields:ident) $projection:expr
    ) => {
        #[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", default, deny_unknown_fields)]
        pub struct $type_name {
            $(
                #[serde(skip_serializing_if = "std::ops::Not::not")]
                pub $field: bool
            ),*
        }

        impl $type_name {
            pub fn project(&self) -> crate::json::exp::Exp {
                let $fields = self;
                $projection.into()
            }
        }
    };
}
pub(crate) use item_field_selection;


macro_rules! field_selection {
    (
        $($item_name:ident: $field_selection:ty ,)*
    ) => {
        #[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", default, deny_unknown_fields)]
        pub struct FieldSelection {
            $(
                #[serde(skip_serializing_if = "crate::query::util::is_default")]
                pub $item_name: $field_selection,
            )*
        }
    };
}
pub(crate) use field_selection;


pub fn is_default<T: Default + Eq>(value: &T) -> bool {
    value.eq(&T::default())
}


macro_rules! request {
    ($(
        pub struct $name:ident {
            $(
                $(#[serde($($serde_attr:tt)*)])?
                pub $field:ident: $field_type:ty,
            )*
        }
    )*) => {
        $(
            #[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
            #[serde(rename_all = "camelCase", default, deny_unknown_fields)]
            pub struct $name {
                $(
                    #[serde(skip_serializing_if = "crate::query::util::is_default" $(, $($serde_attr)*)*)]
                    pub $field: $field_type,
                )*
            }
        )*
    };
}
pub(crate) use request;


macro_rules! ensure_block_range {
    ($query:ident) => {
        if let Some(to_block) = $query.to_block {
            use anyhow::ensure;
            ensure!($query.from_block <= to_block, "got \"toBlock\" < \"fromBlock\"")
        }
    };
}
pub(crate) use ensure_block_range;


macro_rules! ensure_item_count {
    ($query:ident, $i:ident $(, $is:ident)*) => {{
        let num_items = $query.$i.len() $(+ $query.$is.len())*;
        anyhow::ensure!(
            num_items <= 100,
            "query contains {} item requests, but only 100 is allowed",
            num_items
        )
    }};
}
pub(crate) use ensure_item_count;


pub struct PredicateBuilder {
    conditions: Vec<RowPredicateRef>,
    is_never: bool
}


impl PredicateBuilder {
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
            is_never: false
        }
    }

    pub fn col_eq<T: IntoArrowScalar>(&mut self, name: Name, maybe_value: Option<T>) -> &mut Self {
        if let Some(value) = maybe_value {
            let predicate = col_eq(name, value);
            self.conditions.push(predicate)
        }
        self
    }

    pub fn col_in_list<L>(&mut self, name: Name, maybe_list: Option<L>) -> &mut Self
        where L: IntoArrowArray
    {
        if let Some(list) = maybe_list {
            let values = list.into_array();
            if values.len() == 0 {
                self.is_never = true
            }
            let predicate = col_in_list(name, values);
            self.conditions.push(predicate)
        }
        self
    }

    pub fn col_gt_eq<T: IntoArrowScalar>(&mut self, name: Name, maybe_value: Option<T>) -> &mut Self {
        if let Some(value) = maybe_value {
            let predicate = col_gt_eq(name, value);
            self.conditions.push(predicate)
        }
        self
    }

    pub fn col_lt_eq<T: IntoArrowScalar>(&mut self, name: Name, maybe_value: Option<T>) -> &mut Self {
        if let Some(value) = maybe_value {
            let predicate = col_lt_eq(name, value);
            self.conditions.push(predicate)
        }
        self
    }

    pub fn bloom_filter<T: Hash>(
        &mut self, 
        name: Name,
        byte_size: usize,
        num_hashes: usize,
        maybe_list: Option<&[T]>
    ) -> &mut Self
    {
        if let Some(list) = maybe_list {
            if list.len() == 0 {
                self.is_never = true
            }
            let predicate = bloom_filter(name, byte_size, num_hashes, list);
            self.conditions.push(predicate)
        }
        self
    }
    
    pub fn add(&mut self, condition: RowPredicateRef) -> &mut Self {
        self.conditions.push(condition);
        self
    }

    pub fn is_never(&self) -> bool {
        self.is_never
    }
    
    pub fn mark_as_never(&mut self) {
        self.is_never = true
    }
    
    pub fn build(self) -> Option<RowPredicateRef> {
        if self.conditions.len() > 0 {
            Some(and(self.conditions))
        } else {
            None
        }
    }
}


macro_rules! compile_plan {
    (
        $this:ident,
        $table_ref:expr,
        $([$out:ident : $fields:expr],)*
        $($item:ident,)*
        $(<$table_item:ident : $table:ident>,)*
    ) => {{
        use crate::plan::*;
        let mut plan = PlanBuilder::new($table_ref);
        plan.set_include_all_blocks($this.include_all_blocks);
        plan.set_parent_block_hash($this.parent_block_hash.clone());
        plan.set_first_block($this.from_block);
        plan.set_last_block($this.to_block);
        $(
            plan.set_projection(stringify!($out), $fields);
        )*

        macro_rules! _compile_item {
            ($an_item:ident, $a_table:ident) => {
                for item in $this.$an_item.iter() {
                    let mut predicate = PredicateBuilder::new();
                    item.predicate(&mut predicate);
                    if predicate.is_never() {
                        continue;
                    }
                    let mut scan = plan.add_scan(stringify!($a_table));
                    if let Some(predicate) = predicate.build() {
                        scan.with_predicate(predicate);
                    }
                    item.relations(&mut scan);
                }
            };
        }
        $(
            _compile_item!($item, $item);
        )*
        $(
            _compile_item!($table_item, $table);
        )*

        plan.build()
    }};
}
pub(crate) use compile_plan;


pub fn check_hex(s: &str) -> Result<(), &'static str> {
    if !s.starts_with("0x") {
        return Err("binary hex string should start with '0x'")
    }
    if s.len() % 2 != 0 {
        return Err("binary hex string should have an even length")
    }
    if !faster_hex::hex_check(s[2..].as_bytes()) {
        return Err("contains non-hex character")
    }
    Ok(())
}


pub fn parse_hex(s: &str) -> Option<Vec<u8>> {
    if !s.starts_with("0x") {
        return None
    }
    if s.len() % 2 != 0 {
        return None
    }
    let mut bytes = vec![0; s.len() / 2 - 1];
    faster_hex::hex_decode(s[2..].as_bytes(), &mut bytes)
        .ok()
        .map(|_| bytes)
}


pub fn parse_static_hex<const N: usize>(s: &str) -> Option<[u8; N]> {
    if !s.starts_with("0x") {
        return None
    }
    if s.len() != 2 + N * 2 {
        return None
    }
    let mut bytes: [u8; N] = [0; N];
    faster_hex::hex_decode(s[2..].as_bytes(), &mut bytes)
        .ok()
        .map(|_| bytes)
}


pub fn to_lowercase_list(list: &Option<Vec<String>>) -> Option<StringArray> {
    list.as_ref().map(|v| {
        StringArray::from_iter_values(
            v.iter().map(|s| s.to_ascii_lowercase())
        )
    })
}
