use crate::primitives::Name;
use crate::scan::{and, col_eq, col_gt_eq, col_in_list, col_lt_eq, IntoArrow, RowPredicateRef};

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
        if let Some(from_block) = $query.from_block {
            if let Some(to_block) = $query.to_block {
                use anyhow::ensure;
                ensure!(from_block <= to_block, "got \"toBlock\" < \"fromBlock\"")
            }
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

    pub fn col_eq<T: IntoArrow>(&mut self, name: Name, maybe_value: Option<T>) -> &mut Self {
        if let Some(value) = maybe_value {
            let predicate = col_eq(name, value);
            self.conditions.push(predicate)
        }
        self
    }

    pub fn col_in_list<L>(&mut self, name: Name, maybe_list: Option<L>) -> &mut Self
        where L: IntoIterator,
              L::Item: IntoArrow
    {
        if let Some(list) = maybe_list {
            let list: Vec<_> = list.into_iter().collect();
            if list.len() == 0 {
                self.is_never = true
            }
            let predicate = col_in_list(name, list);
            self.conditions.push(predicate)
        }
        self
    }

    pub fn col_gt_eq<T: IntoArrow>(&mut self, name: Name, maybe_value: Option<T>) -> &mut Self {
        if let Some(value) = maybe_value {
            let predicate = col_gt_eq(name, value);
            self.conditions.push(predicate)
        }
        self
    }

    pub fn col_lt_eq<T: IntoArrow>(&mut self, name: Name, maybe_value: Option<T>) -> &mut Self {
        if let Some(value) = maybe_value {
            let predicate = col_lt_eq(name, value);
            self.conditions.push(predicate)
        }
        self
    }

    pub fn is_never(&self) -> bool {
        self.is_never
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