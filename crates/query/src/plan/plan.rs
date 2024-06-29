use std::collections::HashSet;

use anyhow::ensure;
use rayon::prelude::*;

use crate::json::exp::Exp;
use crate::plan::chunk_ctx::ChunkCtx;
use crate::plan::rel::Rel;
use crate::plan::result::{BlockWriter, DataItem};
use crate::plan::row_list::RowList;
use crate::plan::table::{ColumnWeight, TableSet};
use crate::primitives::{BlockNumber, Name, RowWeight, RowWeightPolarsType};
use crate::scan::{col_between, col_gt_eq, col_lt_eq, row_selection, RowPredicateRef};
use crate::util::record_batch_vec_to_lazy_polars_df;


type Idx = usize;


struct Scan {
    table: Name,
    predicate: Option<RowPredicateRef>,
    relations: Vec<Idx>,
    output: Option<Idx>
}


struct Output {
    table: Name,
    key: &'static [Name],
    projection: HashSet<Name>,
    weight_per_row: RowWeight,
    weight_columns: Vec<Name>,
    exp: Exp,
    item_name: Name
}


pub struct Plan {
    scans: Vec<Scan>,
    relations: Vec<Rel>,
    outputs: Vec<Output>,
    include_all_blocks: bool,
    first_block: Option<BlockNumber>,
    last_block: Option<BlockNumber>
}


impl Plan {
    pub fn execute(&self, data_chunk_path: &str) -> anyhow::Result<BlockWriter> {
        PlanExecution {
            chunk_ctx: ChunkCtx::new(data_chunk_path.to_string()),
            plan: self,
        }.execute()
    }
}


struct PlanExecution<'a> {
    chunk_ctx: ChunkCtx,
    plan: &'a Plan,
}


impl <'a> PlanExecution<'a> {
    fn execute(&self) -> anyhow::Result<BlockWriter> {
        let relation_inputs = self.plan.relations.iter()
            .map(|_| RowList::new())
            .collect();

        let output_inputs = self.plan.outputs.iter()
            .map(|_| RowList::new())
            .collect();

        self.execute_scans(&relation_inputs, &output_inputs)?;
        self.execute_relations(relation_inputs, &output_inputs)?;
        self.execute_output(output_inputs)
    }

    fn execute_scans(
        &self,
        relation_inputs: &Vec<RowList>,
        output_inputs: &Vec<RowList>
    ) -> anyhow::Result<()>
    {
        self.plan.scans.par_iter().try_for_each(|scan| -> anyhow::Result<()> {
            let parquet_scan = self.chunk_ctx.get_parquet(scan.table)?;

            let rows = parquet_scan
                .with_row_index(true)
                .with_columns([])
                .with_predicate(scan.predicate.clone())
                .execute()?;

            for rel_idx in scan.relations.iter() {
                relation_inputs[*rel_idx].extend_from_record_batch_vec(&rows);
            }

            if let Some(idx) = &scan.output {
                output_inputs[*idx].extend_from_record_batch_vec(&rows)
            }

            Ok(())
        })
    }

    fn execute_relations(
        &self,
        relation_inputs: Vec<RowList>,
        output_inputs: &Vec<RowList>
    ) -> anyhow::Result<()>
    {
        relation_inputs.into_par_iter().enumerate().try_for_each(|(idx, row_list)| -> anyhow::Result<()> {
            let input = row_list.into_inner();
            if input.is_empty() {
                return Ok(())
            }
            let rel = &self.plan.relations[idx];
            let output = &output_inputs[self.get_output_index(rel.output_table())];
            rel.eval(&self.chunk_ctx, &input, output)
        })
    }

    fn execute_output(&self, output_inputs: Vec<RowList>) -> anyhow::Result<BlockWriter> {
        use polars::prelude::*;

        let rows = output_inputs.into_par_iter()
            .enumerate()
            .map(|(idx, row_list)| -> anyhow::Result<DataFrame> {
                let output = &self.plan.outputs[idx];

                let maybe_row_selection = if idx == 0 { // block header
                    None
                } else {
                    let row_indexes = row_list.into_inner();
                    if row_indexes.is_empty() {
                        return Ok(DataFrame::empty())
                    }
                    Some(row_selection::from_row_indexes(row_indexes))
                };

                let record_batches = self.chunk_ctx.get_parquet(output.table)?
                    .with_row_selection(maybe_row_selection)
                    .with_predicate(self.get_block_number_predicate(output.key[0]))
                    .with_row_index(true)
                    .with_column(output.key[0])
                    .with_columns(output.weight_columns.iter().copied())
                    .execute()?;

                let df = if record_batches.len() == 0 {
                    return Ok(DataFrame::empty())
                } else {
                    record_batch_vec_to_lazy_polars_df(&record_batches)?
                };

                let mut weight_exp = lit(output.weight_per_row);
                for name in output.weight_columns.iter() {
                    weight_exp = weight_exp + col(*name).strict_cast(RowWeightPolarsType::get_dtype());
                }
                weight_exp = weight_exp.alias("weight");

                let rows = df.select([
                    col("row_index"),
                    col(output.key[0]).alias("block_number"),
                    weight_exp
                ]).collect()?;

                Ok(rows)
            }).collect::<anyhow::Result<Vec<_>>>()?;

        let header_rows = &rows[0];

        ensure!(header_rows.shape().0 > 0, "no desired blocks in the data chunk");

        let mut item_union = Vec::with_capacity(self.plan.outputs.len() + 1);

        for df in rows.iter().skip(1).filter(|df| !df.is_empty()) {
            item_union.push(
                df.clone().lazy().select([
                    col("block_number"),
                    col("weight")
                ])
            )
        }

        let block_weights = if self.plan.include_all_blocks {
            item_union.push(
                header_rows.clone().lazy().select([
                    col("block_number"),
                    col("weight")
                ])
            );
            concat(item_union, UnionArgs::default())?
                .group_by([col("block_number")])
                .agg([sum("weight").alias("weight")])
        } else {
            let agg = header_rows.clone()
                .lazy()
                .select([
                    min("block_number").alias("first_block"),
                    max("block_number").alias("last_block")
                ])
                .collect()?;

            let mut block_numbers = agg.column("first_block").unwrap().clone();
            block_numbers.append(agg.column("last_block").unwrap())?;
            block_numbers.rename("block_number");

            item_union.push(
                DataFrame::new(vec![
                    block_numbers,
                    Series::new("weight", &[0 as RowWeight, 0 as RowWeight])
                ])?.lazy()
            );

            let item_stats = concat(item_union, UnionArgs::default())?
                .group_by([col("block_number")])
                .agg([sum("weight").alias("weight")]);

            header_rows.clone().lazy().left_join(
                item_stats,
                col("block_number"),
                col("block_number")
            ).select([
                col("block_number"),
                (col("weight") + col("weight_right")).alias("weight")
            ])
        };

        let package_weight = block_weights.sort(
            ["block_number"],
            SortMultipleOptions::default()
        ).select([
            col("block_number"),
            col("weight").cum_sum(false)
        ]).collect()?;

        let mut selected_blocks = package_weight.clone()
            .lazy()
            .filter(col("weight").lt_eq(lit(20 * 1024 * 1024)))
            .select([col("block_number")])
            .collect()?;

        if selected_blocks.shape().0 == 0 {
            selected_blocks = package_weight.head(Some(1))
        }

        let last_block: BlockNumber = selected_blocks.column("block_number")
            .unwrap()
            .tail(Some(1))
            .cast(&DataType::UInt64)?
            .u64()?
            .get(0)
            .unwrap();

        let data_items_mutex = parking_lot::Mutex::new(
            std::iter::repeat_with(|| None)
                .take(self.plan.outputs.len())
                .collect::<Vec<_>>()
        );

        rows.into_par_iter().enumerate().try_for_each(|(idx, rows)| -> anyhow::Result<()> {
            if rows.is_empty() {
                return Ok(());
            }

            let output = &self.plan.outputs[idx];

            let row_index = if idx == 0 && !self.plan.include_all_blocks {
                rows.lazy()
                    .semi_join(
                        selected_blocks.clone().lazy(),
                        col("block_number"),
                        col("block_number")
                    )
            } else {
                rows.lazy().filter(
                    col("block_number").lt_eq(
                        lit(last_block)
                    )
                )
            }.select([
                col("row_index")
            ]).collect()?;

            let row_selection = row_selection::from_row_indexes(
                row_index.column("row_index").unwrap().u32()?.into_no_null_iter()
            );

            let records = self.chunk_ctx.get_parquet(output.table)?
                .with_row_selection(row_selection)
                .with_projection(output.projection.clone())
                .execute()?;

            let data_item = DataItem::new(
                output.item_name,
                output.key,
                records,
                &output.exp
            )?;

            data_items_mutex.lock()[idx] = Some(data_item);

            Ok(())
        })?;

        Ok(BlockWriter::new(data_items_mutex
            .into_inner()
            .into_iter()
            .filter_map(|i| i)
            .collect()
        ))
    }

    fn get_output_index(&self, table: Name) -> usize {
        self.plan.outputs.iter()
            .position(|o| o.table == table)
            .unwrap()
    }

    fn get_block_number_predicate(&self, block_number_column: Name) -> Option<RowPredicateRef> {
        match (self.plan.first_block, self.plan.last_block) {
            (Some(fst), Some(lst)) => Some(col_between(block_number_column, fst, lst)),
            (Some(fst), None) => Some(col_gt_eq(block_number_column, fst)),
            (None, Some(lst)) => Some(col_lt_eq(block_number_column, lst)),
            (None, None) => None
        }
    }
}


pub struct PlanBuilder {
    tables: &'static TableSet,
    scans: Vec<Scan>,
    relations: Vec<Rel>,
    outputs: Vec<Output>,
    include_all_blocks: bool,
    first_block: Option<BlockNumber>,
    last_block: Option<BlockNumber>
}


impl PlanBuilder{
    pub fn new(tables: &'static TableSet) -> Self {
        PlanBuilder {
            tables,
            scans: Vec::new(),
            relations: Vec::new(),
            outputs: tables.iter().map(|table| {
                Output {
                    table: table.name,
                    key: &table.primary_key,
                    projection: HashSet::new(),
                    weight_per_row: 0,
                    weight_columns: Vec::new(),
                    exp: Exp::Object(vec![]),
                    item_name: table.result_item_name
                }
            }).collect(),
            include_all_blocks: false,
            first_block: None,
            last_block: None
        }
    }

    pub fn add_scan(&mut self, table: Name) -> ScanBuilder<'_> {
        let scan = Scan {
            table,
            predicate: None,
            output: Some(self.tables.get_index(table)),
            relations: Vec::new(),
        };
        let scan_idx = self.scans.len();
        self.scans.push(scan);
        ScanBuilder {
            plan: self,
            scan_idx
        }
    }

    pub fn set_projection<E: Into<Exp>>(&mut self, table: Name, exp: E) -> &mut Self {
        let idx = self.tables.get_index(table);
        self.outputs[idx].exp = exp.into();
        self
    }

    pub fn set_include_all_blocks(&mut self, yes: bool) -> &mut Self {
        self.include_all_blocks = yes;
        self
    }

    pub fn set_first_block<B: Into<Option<BlockNumber>>>(&mut self, block_number: B) -> &mut Self {
        self.first_block = block_number.into();
        self
    }

    pub fn set_last_block<B: Into<Option<BlockNumber>>>(&mut self, block_number: B) -> &mut Self {
        self.last_block = block_number.into();
        self
    }

    pub fn build(mut self) -> Plan {
        self.set_output_weights();
        Plan {
            scans: self.scans,
            relations: self.relations,
            outputs: self.outputs,
            include_all_blocks: self.include_all_blocks,
            first_block: self.first_block,
            last_block: self.last_block
        }
    }

    fn set_output_weights(&mut self) {
        for output in self.outputs.iter_mut() {
            let table = self.tables.get(output.table);

            output.projection.extend(table.primary_key.iter());

            output.exp.for_each_column(&mut |name| {
                output.projection.insert(name);
            });

            let mut per_row = 0;
            let mut weight_columns = Vec::new();

            for col in output.projection.iter() {
                match table.column_weights.get(col).unwrap_or(&ColumnWeight::Fixed(32)) {
                    ColumnWeight::Fixed(weight) => {
                        per_row += weight;
                    }
                    ColumnWeight::Stored(column) => {
                        weight_columns.push(*column);
                    }
                }
            }
            output.weight_per_row = per_row;
            output.weight_columns = weight_columns;
        }
    }

    fn add_rel(&mut self, rel: Rel) -> usize {
        self.relations.iter().position(|r| r == &rel).unwrap_or_else(|| {
            self.relations.push(rel);
            self.relations.len() - 1
        })
    }
}


pub struct ScanBuilder<'a> {
    plan: &'a mut PlanBuilder,
    scan_idx: usize
}


impl <'a> ScanBuilder<'a> {
    pub fn with_predicate(&mut self, predicate: RowPredicateRef) -> &mut Self {
        self.scan_mut().predicate = Some(predicate);
        self
    }

    pub fn join(
        &mut self,
        output_table: Name,
        output_key: Vec<Name>,
        scan_key: Vec<Name>
    ) -> &mut Self {
        let rel_idx = self.plan.add_rel(Rel::Join {
            input_table: self.plan.scans[self.scan_idx].table,
            input_key: scan_key,
            output_table,
            output_key
        });
        self.scan_mut().relations.push(rel_idx);
        self
    }

    pub fn include_children(&mut self) -> &mut Self {
        let table = &self.plan.scans[self.scan_idx].table;
        let key = self.plan.tables.get(table).primary_key.clone();
        let rel_idx = self.plan.add_rel(Rel::Children {
            table,
            key
        });
        self.scan_mut().relations.push(rel_idx);
        self
    }

    pub fn include_sub_items(
        &mut self,
        output_table: Name,
        output_key: Vec<Name>,
        scan_key: Vec<Name>
    ) -> &mut Self {
        let rel_idx = self.plan.add_rel(Rel::Sub {
            input_table: self.plan.scans[self.scan_idx].table,
            input_key: scan_key,
            output_table,
            output_key
        });
        self.scan_mut().relations.push(rel_idx);
        self
    }

    pub fn include_stack(&mut self) -> &mut Self {
        let table = &self.plan.scans[self.scan_idx].table;
        let key = self.plan.tables.get(table).primary_key.clone();
        let rel_idx = self.plan.add_rel(Rel::Stack {
            table,
            key
        });
        self.scan_mut().relations.push(rel_idx);
        self
    }

    pub fn with_no_output(&mut self) -> &mut Self {
        self.scan_mut().output = None;
        self
    }

    fn scan_mut(&mut self) -> &mut Scan {
        &mut self.plan.scans[self.scan_idx]
    }
}