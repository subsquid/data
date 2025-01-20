use crate::json::exp::Exp;
use crate::plan::rel::Rel;
use crate::plan::result::{BlockWriter, DataItem};
use crate::plan::row_list::RowList;
use crate::plan::table::{ColumnWeight, TableSet};
use crate::primitives::{BlockNumber, Name, RowRangeList, RowWeight, RowWeightPolarsType};
use crate::scan::{col_between, col_gt_eq, col_lt_eq, Chunk, RowPredicateRef};
use crate::UnexpectedBaseBlock;
use anyhow::{anyhow, bail, ensure};
use rayon::prelude::*;
use sqd_polars::arrow::record_batch_vec_to_lazy_polars_df;
use sqd_primitives::BlockRef;
use std::collections::{HashMap, HashSet};


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
    parent_block_hash: Option<String>,
    first_block: Option<BlockNumber>,
    last_block: Option<BlockNumber>
}


impl Plan {
    pub fn execute(&self, data_chunk: &dyn Chunk) -> anyhow::Result<BlockWriter> {
        PlanExecution {
            data_chunk,
            plan: self,
        }.execute()
    }

    pub fn set_parent_block_hash(&mut self, hash: impl Into<Option<String>>) {
        self.parent_block_hash = hash.into();
    }

    pub fn set_first_block(&mut self, block_number: impl Into<Option<BlockNumber>>) {
        self.first_block = block_number.into()
    }

    pub fn set_last_block(&mut self, block_number: impl Into<Option<BlockNumber>>) {
        self.last_block = block_number.into()
    }
}


struct PlanExecution<'a> {
    data_chunk: &'a dyn Chunk,
    plan: &'a Plan,
}


impl <'a> PlanExecution<'a> {
    fn execute(&self) -> anyhow::Result<BlockWriter> {
        self.check_parent_block()?;

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

    fn check_parent_block(&self) -> anyhow::Result<()> {
        let parent_hash = match self.plan.parent_block_hash.as_ref() {
            Some(s) => s.as_str(),
            None => return Ok(())
        };

        let block_number = match self.plan.first_block {
            Some(bn) => bn,
            None => bail!("invalid plan: parent block hash is specified, but block number is not available")
        };

        let block_scan = self.data_chunk.scan_table("blocks")?;

        let mut refs: Vec<_> = if block_scan.schema().column_with_name("parent_number").is_some() {
            // this is Solana with possible gaps in block numbers
            let df = block_scan
                .with_column("parent_number")
                .with_column("parent_hash")
                .with_predicate(
                    col_between("parent_number", block_number.saturating_sub(10), block_number.saturating_sub(1))
                )
                .to_lazy_df()?
                .collect()?;

            let numbers = df.column("parent_number")?.cast(&sqd_polars::prelude::DataType::UInt64)?;
            let numbers = numbers.u64()?;

            let hashes = df.column("parent_hash")?;
            let hashes = hashes.str()?;

            (0..df.shape().0).map(|i| {
                let number = numbers.get(i).expect("block number can't be null according to the predicate applied");
                let hash = hashes.get(i).unwrap_or("");
                BlockRef {
                    number,
                    hash: hash.to_string()
                }
            }).collect()
        } else {
            let df = block_scan
                .with_column("number")
                .with_column("parent_hash")
                .with_predicate(
                    col_between("number", block_number.saturating_sub(9), block_number)
                )
                .to_lazy_df()?
                .collect()?;
            
            let numbers = df.column("number")?.cast(&sqd_polars::prelude::DataType::UInt64)?;
            let numbers = numbers.u64()?;

            let hashes = df.column("parent_hash")?;
            let hashes = hashes.str()?;

            (0..df.shape().0).map(|i| {
                let number = numbers.get(i).expect("block number can't be null according to the predicate applied");
                let hash = hashes.get(i).unwrap_or("");
                BlockRef {
                    number: number.saturating_sub(1),
                    hash: hash.to_string()
                }
            }).collect()
        };

        refs.sort_by(|a, b| b.number.cmp(&a.number));

        let parent_block = refs.first().ok_or_else(|| {
            anyhow!("block {} is not present in the chunk", block_number)
        })?;
        
        if parent_block.hash == parent_hash {
            Ok(())
        } else {
            Err(anyhow!(UnexpectedBaseBlock {
                prev_blocks: refs,
                expected_hash: parent_hash.to_string()
            }))   
        }
    }

    fn execute_scans(
        &self,
        relation_inputs: &Vec<RowList>,
        output_inputs: &Vec<RowList>
    ) -> anyhow::Result<()>
    {
        self.plan.scans.par_iter().try_for_each(|scan| -> anyhow::Result<()> {
            let rows = self.data_chunk
                .scan_table(scan.table)?
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
            rel.eval(self.data_chunk, &input, output)
        })
    }

    fn execute_output(&self, output_inputs: Vec<RowList>) -> anyhow::Result<BlockWriter> {
        use sqd_polars::prelude::*;

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
                    Some(RowRangeList::from_sorted_indexes(row_indexes))
                };

                let record_batches = self.data_chunk
                    .scan_table(output.table)?
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
                    weight_exp = weight_exp + col(*name);
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
                    col("weight").strict_cast(RowWeightPolarsType::get_dtype())
                ])
            )
        }

        let block_weights = if self.plan.include_all_blocks {
            item_union.push(
                header_rows.clone().lazy().select([
                    col("block_number"),
                    col("weight").strict_cast(RowWeightPolarsType::get_dtype())
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

            let mut block_numbers = agg.column("first_block")?.clone();
            block_numbers.append(agg.column("last_block").unwrap())?;
            block_numbers.rename("block_number".into());

            item_union.push(
                DataFrame::new(vec![
                    block_numbers,
                    Series::new("weight".into(), &[0 as RowWeight, 0 as RowWeight])
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

            let row_selection = RowRangeList::from_sorted_indexes(
                row_index.column("row_index").unwrap().u32()?.into_no_null_iter()
            );

            let records = self.data_chunk
                .scan_table(output.table)?
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
            .flatten()
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
    parent_block_hash: Option<String>,
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
            parent_block_hash: None,
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

    pub fn set_parent_block_hash(&mut self, hash: impl Into<Option<String>>) -> &mut Self {
        self.parent_block_hash = hash.into();
        self
    }

    pub fn set_first_block(&mut self, block_number: impl Into<Option<BlockNumber>>) -> &mut Self {
        self.first_block = block_number.into();
        self
    }

    pub fn set_last_block(&mut self, block_number: impl Into<Option<BlockNumber>>) -> &mut Self {
        self.last_block = block_number.into();
        self
    }

    pub fn build(mut self) -> Plan {
        self.simplify();
        self.set_output_weights();
        Plan {
            scans: self.scans,
            relations: self.relations,
            outputs: self.outputs,
            include_all_blocks: self.include_all_blocks,
            parent_block_hash: self.parent_block_hash,
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

    /// Lame plan optimization procedure
    ///
    /// The problem:
    ///
    /// "Give me all the data" guys often send requests like this:
    /// ```json
    /// {
    ///   "transactions": [
    ///     {
    ///       "logs": true,
    ///       "traces": true
    ///     }
    ///   ],
    ///   "logs": [
    ///     {
    ///       "transaction": true,
    ///       "transactionTraces": true
    ///     }
    ///   ],
    ///   "traces": [
    ///     {
    ///       "transaction": true,
    ///       "subtraces": true,
    ///       "parents": true
    ///     }
    ///   ]
    /// }
    /// ```
    ///
    /// Data leaching is expensive for us in general,
    /// but especially when they come with (needless) relations.
    ///
    /// This procedure tries to rewrite the above query to its sane variant
    /// ```json
    /// {
    ///   "transactions": [{}],
    ///   "logs": [{}],
    ///   "traces": [{}]
    /// }
    /// ```
    fn simplify(&mut self) {
        if self.scans.iter().all(|s| s.predicate.is_some()) {
            return;
        }

        let mut is_full = vec![false; self.outputs.len()];
        let mut is_full_rel = vec![false; self.relations.len()];

        for scan in self.scans.iter() {
            if scan.predicate.is_none() {
                // no-predicate scans select the entire table
                if let Some(out_idx) = scan.output {
                    // mark output as fully fetched
                    is_full[out_idx] = true;
                }
                for rel_idx in scan.relations.iter().copied() {
                    // mark relation input as fully populated
                    is_full_rel[rel_idx] = true;

                    let rel = &self.relations[rel_idx];
                    if self.is_full_rel(rel) {
                        // for this relation fully populated input implies fully populated output
                        // hence, mark output as fully fetched
                        is_full[self.tables.get_index(rel.output_table())] = true;
                    }
                }
            }
        }

        // Remove relations, that point to fully populated outputs.
        // We'll make sure, that those outputs remain populated later.
        let relations_remove_mask = self.relations.iter().map(|rel| {
            is_full[self.tables.get_index(rel.output_table())]
        }).collect::<Vec<_>>();

        remove_elements(&mut is_full_rel, &relations_remove_mask);
        self.remove_relations(&relations_remove_mask);

        // Now, remove all scans, that either feed a fully populated relations/outputs
        // or produce fully populated relations/outputs themselves.
        self.scans.retain_mut(|scan| {
            if let Some(out_idx) = scan.output {
                if is_full[out_idx] {
                    scan.output = None;
                }
            }
            scan.relations.retain(|&i| !is_full_rel[i]);
            scan.predicate.is_some() && (scan.relations.len() > 0 || scan.output.is_some())
        });

        // At this stage everything, that should be fully populated is not populated at all.
        // Introduce new scans to populate that.
        let mut new_scans: HashMap<Name, Scan> = HashMap::new();

        for out_idx in is_full.iter().enumerate().filter_map(|(idx, full)| full.then_some(idx)) {
            let table = self.outputs[out_idx].table;
            new_scans.insert(table, Scan {
                table,
                predicate: None,
                relations: vec![],
                output: Some(out_idx)
            });
        }

        for (idx, rel) in self.relations.iter().enumerate().filter(|(idx, _)| is_full_rel[*idx]) {
            let table = rel.output_table();
            let scan = new_scans.entry(table).or_insert_with(|| {
                Scan {
                    table,
                    predicate: None,
                    relations: vec![],
                    output: None
                }
            });
            scan.relations.push(idx);
        }

        self.scans.extend(new_scans.into_values())
    }

    fn remove_relations(&mut self, remove_mask: &[bool]) {
        remove_elements(&mut self.relations, remove_mask);

        let mut idx = 0;
        let index_map = remove_mask.iter().map(|&remove| {
            if remove {
                None
            } else {
                let i = idx;
                idx += 1;
                Some(i)
            }
        }).collect::<Vec<_>>();

        for scan in self.scans.iter_mut() {
            scan.relations.retain_mut(|rel_idx| {
                if let Some(i) = index_map[*rel_idx] {
                    *rel_idx = i;
                    true
                } else {
                    false
                }
            })
        }
    }

    /// checks whether full input implies full output
    fn is_full_rel(&self, rel: &Rel) -> bool {
        match rel {
            Rel::Join { input_table, input_key, output_table, output_key } => {
                let input_desc = self.tables.get(input_table);
                input_key == &input_desc.primary_key
                    && input_desc.children.get(output_table) == Some(output_key)
            },
            Rel::Children { .. } => true,
            Rel::Parents { .. } => true,
            _ => false
        }
    }
}


fn remove_elements<T>(vec: &mut Vec<T>, remove_mask: &[bool]) {
    let mut idx = 0;
    vec.retain(|_| {
        let remove = remove_mask[idx];
        idx += 1;
        !remove
    });
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

    pub fn include_parents(&mut self) -> &mut Self {
        let table = &self.plan.scans[self.scan_idx].table;
        let key = self.plan.tables.get(table).primary_key.clone();
        let rel_idx = self.plan.add_rel(Rel::Parents {
            table,
            key
        });
        self.scan_mut().relations.push(rel_idx);
        self
    }

    pub fn include_foreign_children(
        &mut self,
        output_table: Name,
        output_key: Vec<Name>,
        scan_key: Vec<Name>
    ) -> &mut Self {
        let rel_idx = self.plan.add_rel(Rel::ForeignChildren {
            input_table: self.plan.scans[self.scan_idx].table,
            input_key: scan_key,
            output_table,
            output_key
        });
        self.scan_mut().relations.push(rel_idx);
        self
    }

    pub fn include_foreign_parents(
        &mut self,
        output_table: Name,
        output_key: Vec<Name>,
        scan_key: Vec<Name>
    ) -> &mut Self {
        let rel_idx = self.plan.add_rel(Rel::ForeignParents {
            input_table: self.plan.scans[self.scan_idx].table,
            input_key: scan_key,
            output_table,
            output_key
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