use std::collections::{BTreeSet, HashSet};

use anyhow::bail;
use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray, OffsetSizeTrait};
use arrow::datatypes::{DataType, Int32Type, UInt32Type, UInt16Type};

use sqd_polars::arrow::{polars_series_to_arrow_array, polars_series_to_row_index_iter};
use sqd_primitives::RowRangeList;

use crate::plan::key::{GenericListKey, Key, PrimitiveGenericListKey};
use crate::plan::row_list::RowList;
use crate::primitives::{Name, RowIndex, RowIndexArrowType, schema_error, SchemaError};
use crate::scan::Chunk;


#[derive(PartialEq, Eq, Hash, Clone)]
pub enum Rel {
    Join {
        input_table: Name,
        input_key: Vec<Name>,
        output_table: Name,
        output_key: Vec<Name>
    },
    Children {
        table: Name,
        key: Vec<Name>
    },
    Sub {
        input_table: Name,
        input_key: Vec<Name>,
        output_table: Name,
        output_key: Vec<Name>
    },
    Stack {
        table: Name,
        key: Vec<Name>
    }
}


impl Rel {
    pub fn output_table(&self) -> Name {
        match self {
            Rel::Join { output_table, .. } => *output_table,
            Rel::Children { table, .. } => *table,
            Rel::Sub { output_table, .. } => *output_table,
            Rel::Stack { table, .. } => *table
        }
    }

    pub fn eval(
        &self,
        chunk: &dyn Chunk,
        input: &BTreeSet<RowIndex>,
        output: &RowList
    ) -> anyhow::Result<()>
    {
        match self {
            Rel::Join {
                input_table,
                input_key,
                output_table,
                output_key
            } => {
                eval_join(chunk, input, input_table, input_key, output_table, output_key, output)
            },
            Rel::Children {
                table,
                key
            } => {
                eval_children(chunk, input, table, key, output)
            },
            Rel::Sub {
                input_table,
                input_key,
                output_table,
                output_key
            } => {
                eval_sub(chunk, input, input_table, input_key, output_table, output_key, output)
            },
            Rel::Stack {
                table,
                key
            } => {
                eval_stack(chunk, input, table, key, output)
            }
        }
    }
}


fn eval_join(
    chunk: &dyn Chunk,
    input: &BTreeSet<RowIndex>,
    input_table: Name,
    input_key: &Vec<Name>,
    output_table: Name,
    output_key: &Vec<Name>,
    output: &RowList
) -> anyhow::Result<()>
{
    use sqd_polars::prelude::*;
    
    let input_rows = chunk.scan_table(input_table)?
        .with_row_selection(RowRangeList::from_sorted_indexes(input.iter().copied()))
        .with_columns(input_key.iter().copied())
        .to_lazy_df()?;

    let output_rows = chunk.scan_table(output_table)?
        .with_row_index(true)
        .with_columns(output_key.iter().copied())
        .to_lazy_df()?;

    let result = output_rows.join(
        input_rows,
        output_key.iter().copied().map(col).collect::<Vec<_>>(),
        input_key.iter().copied().map(col).collect::<Vec<_>>(),
        JoinArgs::new(JoinType::Semi)
    ).select([
        col("row_index")
    ]).collect()?;

    output.extend_from_polars_df(&result);

    Ok(())
}


fn eval_children(
    chunk: &dyn Chunk,
    input: &BTreeSet<RowIndex>,
    table: Name,
    key: &Vec<Name>,
    output: &RowList
) -> anyhow::Result<()>
{
    let stack = select_stack(chunk, table, key, input)?;

    let children = find_children(
        &stack,
        input,
        false
    )?;

    output.extend(children);

    Ok(())
}


fn select_stack(
    chunk: &dyn Chunk,
    table: Name,
    key: &Vec<Name>,
    input: &BTreeSet<RowIndex>
) -> anyhow::Result<Stack>
{
    use sqd_polars::prelude::*;

    let items = chunk.scan_table(table)?
        .with_row_index(true)
        .with_columns(key.iter().copied())
        .to_lazy_df()?
        .collect()?;

    let row_index = DataFrame::new(vec![{
        let mut series = Series::from_iter(input.iter().copied());
        series.rename("row_index");
        series
    }])?;

    let group_key_columns: Vec<_> = key.iter().copied()
        .map(col)
        .take(key.len() - 1)
        .collect();

    let address_column = key.last().unwrap();

    let groups = row_index.lazy().join(
        items.clone().lazy(),
        [col("row_index")],
        [col("row_index")],
        JoinArgs::new(JoinType::Inner)
    ).select(
        &group_key_columns
    ).unique(
        None,
        UniqueKeepStrategy::Any
    ).with_row_index(
        "_group",
        None
    );

    let items = groups.join(
        items.lazy(),
        &group_key_columns,
        &group_key_columns,
        JoinArgs::new(JoinType::Inner)
    ).group_by(
        ["_group"]
    ).agg([
        col(address_column),
        col("row_index")
    ]).select([
        col(address_column).alias("address"),
        col("row_index")
    ]).collect()?;

    Ok(Stack::from_df(&items))
}


fn eval_sub(
    chunk: &dyn Chunk,
    input: &BTreeSet<RowIndex>,
    input_table: Name,
    input_key: &Vec<Name>,
    output_table: Name,
    output_key: &Vec<Name>,
    output: &RowList
) -> anyhow::Result<()>
{
    use sqd_polars::prelude::*;

    let group_key_exp: Vec<_> = output_key.iter().copied()
        .map(col)
        .take(output_key.len() - 1)
        .collect();

    let address_column = output_key.last().unwrap();

    let input = chunk.scan_table(input_table)?
        .with_row_selection(RowRangeList::from_sorted_indexes(input.iter().copied()))
        .with_columns(input_key.iter().copied())
        .to_lazy_df()?
        .select(
            input_key.iter().zip(output_key.iter())
                .map(|(i, o)| col(*i).alias(*o))
                .collect::<Vec<_>>()
        )
        .collect()?;

    let items = chunk.scan_table(output_table)?
        .with_columns(output_key.iter().copied())
        .with_row_index(true)
        .to_lazy_df()?
        .join(
            input.clone().lazy(),
            &group_key_exp,
            &group_key_exp,
            JoinArgs::new(JoinType::Semi)
        )
        .collect()?;

    let output_key_exp = output_key.iter().copied().map(col).collect::<Vec<_>>();

    let parents = input.lazy()
        .join(
            items.clone().lazy(),
            &output_key_exp,
            &output_key_exp,
            JoinArgs::new(JoinType::Inner)
        )
        .select([col("row_index")])
        .collect()
        .map(|df| {
            BTreeSet::from_iter(
                polars_series_to_row_index_iter(df.column("row_index").unwrap())
            )
        })?;

    let groups = items.lazy()
        .group_by(group_key_exp)
        .agg([
            col(address_column),
            col("row_index")
        ])
        .select([
            col(address_column).alias("address"),
            col("row_index")
        ])
        .collect()?;

    let stack = Stack::from_df(&groups);

    let children = find_children(
        &stack,
        &parents,
        true
    )?;

    output.extend(children);

    Ok(())
}


struct Stack {
    address: ArrayRef,
    row_index: ArrayRef
}


impl Stack {
    fn from_df(df: &sqd_polars::prelude::DataFrame) -> Self {
        let address = polars_series_to_arrow_array(
            df.column("address").unwrap()
        );
        let row_index = polars_series_to_arrow_array(
            df.column("row_index").unwrap()
        );
        Self {
            address,
            row_index
        }
    }

    fn get_address_type(&self) -> &DataType {
        if let DataType::LargeList(f) = self.address.data_type() {
            f.data_type()
        } else {
            panic!("got unexpected address group type - {}", self.address.data_type())
        }
    }
}


macro_rules! downcast_address_type {
    ($address_type:expr, $co:path) => {
        match $address_type {
            DataType::LargeList(it) if it.data_type() == &DataType::Int32 => {
                $co!(Int32Type)
            },
            DataType::LargeList(it) if it.data_type() == &DataType::UInt32 => {
                $co!(UInt32Type)
            },
            DataType::LargeList(it) if it.data_type() == &DataType::UInt16 => {
                $co!(UInt16Type)
            },
            it => bail!(
                schema_error!("invalid address type - {}", it)
            ),
        }
    };
}


fn find_children(
    stack: &Stack,
    parents: &BTreeSet<RowIndex>,
    include_parent: bool
) -> anyhow::Result<Vec<RowIndex>>
{
    macro_rules! find {
        ($address_type:ty) => {
            find_children_impl::<$address_type, i64>(
                &stack,
                parents,
                include_parent
            )
        };
    }

    Ok(downcast_address_type!(stack.get_address_type(), find))
}


fn find_children_impl<A, O>(
    stack:   &Stack,
    parents: &BTreeSet<RowIndex>,
    include_parent: bool
) -> Vec<RowIndex>
    where A: ArrowPrimitiveType,
          A::Native: Eq + Ord,
          O: OffsetSizeTrait
{
    let n_groups = stack.address.len();
    let mut children = Vec::with_capacity(stack.row_index.as_list::<O>().values().len());

    let address = GenericListKey::<PrimitiveGenericListKey<A, O>, O>::from(stack.address.as_ref());
    let row_index = PrimitiveGenericListKey::<RowIndexArrowType, O>::from(stack.row_index.as_ref());

    let mut order = Vec::with_capacity(
        (0..n_groups).map(|g| row_index.get(g).len()).max().unwrap_or(0)
    );

    for g in 0..n_groups {
        let rows = row_index.get(g);
        let addrs = address.get(g);
        assert_eq!(rows.len(), addrs.len());

        // polars can't sort addresses for us,
        // so we have to do it here
        order.clear();
        order.extend(0..rows.len());
        order.sort_unstable_by_key(|i| addrs.get(*i));

        let mut i = 0;
        while i < rows.len() {
            let parent = rows[order[i]];
            if parents.contains(&parent) {
                if include_parent {
                    children.push(parent);
                }
                let parent_addr = addrs.get(order[i]);
                i += 1;
                while i < rows.len() && is_parent_address(parent_addr, addrs.get(order[i])) {
                    children.push(rows[order[i]]);
                    i += 1;
                }
            } else {
                i += 1;
            }
        }
    }

    children
}


fn is_parent_address<I: Eq>(parent: &[I], child: &[I]) -> bool {
    parent.len() <= child.len() && parent.eq(&child[0..parent.len()])
}


fn eval_stack(
    chunk: &dyn Chunk,
    input: &BTreeSet<RowIndex>,
    table: Name,
    key: &Vec<Name>,
    output: &RowList
) -> anyhow::Result<()>
{
    let stack = select_stack(chunk, table, key, input)?;
    let parents = find_parents(&stack, input)?;
    output.extend(parents);
    Ok(())
}


fn find_parents(stack: &Stack, children: &BTreeSet<RowIndex>) -> anyhow::Result<HashSet<RowIndex>> {
    macro_rules! find {
        ($address_type:ty) => {
            find_parents_impl::<$address_type, i64>(stack, children)
        };
    }

    Ok(downcast_address_type!(stack.get_address_type(), find))
}


fn find_parents_impl<A, O>(
    stack: &Stack,
    children: &BTreeSet<RowIndex>
) -> HashSet<RowIndex>
    where A: ArrowPrimitiveType,
          A::Native: Eq + Ord,
          O: OffsetSizeTrait
{
    let n_groups = stack.address.len();
    let mut parents = HashSet::with_capacity(stack.row_index.as_list::<O>().values().len());

    let address = GenericListKey::<PrimitiveGenericListKey<A, O>, O>::from(stack.address.as_ref());
    let row_index = PrimitiveGenericListKey::<RowIndexArrowType, O>::from(stack.row_index.as_ref());

    let mut order = Vec::with_capacity(
        (0..n_groups).map(|g| row_index.get(g).len()).max().unwrap_or(0)
    );

    let mut s = Vec::with_capacity(50);

    for g in 0..n_groups {
        let rows = row_index.get(g);
        let addrs = address.get(g);
        assert_eq!(rows.len(), addrs.len());

        // polars can't sort addresses for us,
        // so we have to do it here
        order.clear();
        order.extend(0..rows.len());
        order.sort_unstable_by_key(|i| addrs.get(*i));

        s.clear();

        for i in 0..rows.len() {
            let addr = addrs.get(order[i]);
            while let Some(top) = s.last().copied() {
                if is_parent_address(addrs.get(top), addr) {
                    break
                } else {
                    s.pop();
                }
            }
            s.push(order[i]);
            if children.contains(&rows[order[i]]) {
                parents.extend(
                    s.iter().map(|i| rows[*i])
                );
            }
        }
    }

    parents
}