use crate::kv::KvWrite;
use crate::table::key::TableKeyFactory;
use crate::table::util::{get_num_buffers, BufferBag};
use crate::table::write::array::{make_any_builder, AnyBuilder, FlushCallback};
use crate::table::write::stats::StatsBuilder;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::convert::schema_to_fb;
use arrow_buffer::ToByteSlice;
use sqd_dataset::TableOptions;


pub struct TableWriter<S> {
    schema: SchemaRef,
    columns: Vec<AnyBuilder>,
    pages: Vec<Vec<Vec<u32>>>,
    stats: Vec<Option<StatsBuilder>>,
    key: TableKeyFactory,
    storage: S
}


impl <S: KvWrite> TableWriter<S> {
    pub fn new(
        storage: S,
        schema: SchemaRef,
        table_name: &[u8],
        table_options: &TableOptions
    ) -> Self
    {
        let columns: Vec<_> = schema.fields().iter().map(|f| {
            make_any_builder(f.data_type(), table_options.default_page_size)
        }).collect();

        let pages: Vec<_> = schema.fields().iter().map(|f| {
            let num_buffers = get_num_buffers(f.data_type());
            std::iter::repeat_with(|| vec![0])
                .take(num_buffers)
                .collect::<Vec<_>>()
        }).collect();
        
        let stats: Vec<_> = schema.fields().iter().map(|f| {
            StatsBuilder::can_have_stats(f.data_type()).then_some(())?;
            let partition = table_options.get_stats_partition(f.name())?;
            let builder = StatsBuilder::new(f.data_type(), partition);
            Some(builder)
        }).collect();

        Self {
            schema,
            columns,
            pages,
            stats,
            key: TableKeyFactory::new(table_name),
            storage
        }
    }

    pub fn push_record_batch(&mut self, record_batch: &RecordBatch) {
        assert_eq!(self.columns.len(), record_batch.num_columns());
        for ((idx, col), arr) in self.columns.iter_mut().enumerate().zip(record_batch.columns().iter()) {
            col.push_array(&arr);
            if let Some(stats) = self.stats[idx].as_mut() {
                stats.push_array(&arr)
            }
        }
    }

    pub fn flush(&mut self) -> anyhow::Result<()> {
        self.flush_impl(|col, cb| col.flush(cb))
    }

    fn flush_impl<F>(&mut self, mut flush: F) -> anyhow::Result<()>
    where
        F: for<'a> FnMut(&'a mut AnyBuilder, FlushCallback<'a>) -> anyhow::Result<()>
    {
        for (col_idx, col) in self.columns.iter_mut().enumerate() {
            flush(col, &mut |buf_idx, len, data| {
                let offsets = &mut self.pages[col_idx][buf_idx];
                let key = self.key.page(col_idx, buf_idx, offsets.len() - 1);
                self.storage.put(key, data)?;
                offsets.push(offsets.last().cloned().unwrap() + len as u32);
                Ok(())
            })?
        }
        Ok(())
    }

    pub fn finish(mut self) -> anyhow::Result<S> {
        self.flush_impl(|col, cb| col.flush_all(cb))?;

        for (col_idx, stats) in self.stats.into_iter().enumerate().filter_map(|(i, stats)| {
            let s = stats?.finish()?;
            Some((i, s))
        }) {
            let mut data = Vec::new();
            stats.serialize(&mut data);
            let key = self.key.statistic(col_idx);
            self.storage.put(key, &data)?;
            data.clear()
        }

        for (col_idx, buffers) in self.pages.iter().enumerate() {
            for (buf_idx, offsets) in buffers.iter().enumerate() {
                let key = self.key.offsets(col_idx, buf_idx);
                self.storage.put(key, offsets.to_byte_slice())?
            }
        }

        self.storage.put(
            self.key.schema(),
            schema_to_fb(&self.schema).finished_data()
        )?;

        Ok(self.storage)
    }
}