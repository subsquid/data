use anyhow::ensure;
use arrow::array::Array;
use arrow::datatypes::{Field, SchemaRef};
use arrow::ipc::convert::schema_to_fb;

use sqd_array::{AnySlice, Slice};
use sqd_dataset::TableOptions;

use crate::kv::KvWrite;
use crate::table::key::{Statistic, TableKeyFactory};
use crate::table::write::stats::{Summary, SummaryBuilder};


fn make_summary_builder(f: &Field, options: &TableOptions) -> SummaryBuilder {
    let with_stats = options.has_stats(f.name());
    if with_stats && SummaryBuilder::can_have_stats(f.data_type()) {
        SummaryBuilder::new(Some(f.data_type().clone()))
    } else {
        SummaryBuilder::new(None)
    }
}


pub struct TableWriter<S> {
    storage: S,
    schema: SchemaRef,
    summary0: Vec<SummaryBuilder>,
    summary1: Vec<SummaryBuilder>,
    key: TableKeyFactory,
    buf: Vec<u8>
}


macro_rules! write_array {
    ($this:ident, $key:expr, $array:expr) => {{
        $this.buf.clear();
        AnySlice::from($array).write_page(&mut $this.buf);
        $this.storage.put($key, &$this.buf)
    }};
}


impl <S: KvWrite> TableWriter<S> {
    pub fn new(
        storage: S,
        schema: SchemaRef,
        table_name: &[u8],
        table_options: &TableOptions
    ) -> Self {
        let summary0 = schema.fields().iter()
            .map(|f| make_summary_builder(f, table_options))
            .collect();

        let summary1 = schema.fields().iter()
            .map(|f| make_summary_builder(f, table_options))
            .collect();

        Self {
            storage,
            schema,
            summary0,
            summary1,
            key: TableKeyFactory::new(table_name),
            buf: Vec::with_capacity(3 * table_options.default_page_size / 2)
        }
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
    }

    pub fn push_page(&mut self, column_index: usize, array: &dyn Array) -> anyhow::Result<()> {
        if array.is_empty() {
            return Ok(());
        }
        assert_eq!(self.schema.field(column_index).data_type(), array.data_type());
        let row_group = self.current_row_group_index();
        let page_index = self.summary1[column_index].len();
        let key = self.key.page(row_group, column_index, page_index);
        write_array!(self, key, array)?;
        self.summary1[column_index].push(array);
        Ok(())
    }

    fn current_row_group_index(&self) -> usize {
        self.summary0[0].len()
    }

    fn current_row_group_len(&self) -> anyhow::Result<u32> {
        let len = self.summary1[0].item_count();
        for (idx, col) in self.summary1.iter().enumerate().skip(1) {
            let col_len = col.item_count();
            ensure!(
                len == col_len,
                "columns `{}` and `{}` have different lengths",
                self.schema.field(0).name(),
                self.schema.field(idx).name()
            )
        }
        Ok(len)
    }

    pub fn new_row_group(&mut self) -> anyhow::Result<()> {
        if self.current_row_group_len()? == 0 {
            return Ok(())
        }
        let row_group = self.current_row_group_index();
        let columns: Vec<Summary> = self.summary1.iter_mut().map(|s| s.finish()).collect();

        // write page level stats
        for (col_idx, summary) in columns.iter().enumerate() {
            write_array!(
                self,
                self.key.statistic1(Statistic::Offsets, row_group, col_idx),
                &summary.offsets
            )?;
            write_array!(
                self,
                self.key.statistic1(Statistic::NullCount, row_group, col_idx),
                &summary.null_count
            )?;
            if let Some(stats) = summary.stats.as_ref() {
                write_array!(
                    self,
                    self.key.statistic1(Statistic::Min, row_group, col_idx),
                    stats.min.as_ref()
                )?;
                write_array!(
                    self,
                    self.key.statistic1(Statistic::Max, row_group, col_idx),
                    stats.max.as_ref()
                )?;
            }
        }

        for (rgs, pages) in self.summary0.iter_mut().zip(columns.iter()) {
            rgs.acc(pages)
        }

        Ok(())
    }

    pub fn finish(mut self) -> anyhow::Result<S> {
        self.new_row_group()?;

        for (col_idx, mut summary_builder) in self.summary0.into_iter().enumerate() {
            let summary = summary_builder.finish();
            if col_idx == 0 {
                write_array!(
                    self,
                    self.key.row_group_offsets(),
                    &summary.offsets
                )?;
            }
            write_array!(
                self,
                self.key.statistic0(Statistic::NullCount, col_idx),
                &summary.null_count
            )?;
            if let Some(stats) = summary.stats.as_ref() {
                write_array!(
                    self,
                    self.key.statistic0(Statistic::Min, col_idx),
                    stats.min.as_ref()
                )?;
                write_array!(
                    self,
                    self.key.statistic0(Statistic::Max, col_idx),
                    stats.max.as_ref()
                )?;
            }
        }

        self.storage.put(
            self.key.schema(),
            schema_to_fb(&self.schema).finished_data()
        )?;

        Ok(self.storage)
    }
}