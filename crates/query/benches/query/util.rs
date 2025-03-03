use sqd_query::{Chunk, JsonLinesWriter, Plan, Query};


macro_rules! query {
    ($name:ident, $($json:tt)+) => {
        static $name: std::sync::LazyLock<sqd_query::Query> = std::sync::LazyLock::new(|| {
            let json = serde_json::json!($($json)*);
            sqd_query::Query::from_json_value(json).unwrap()
        });
    };
}
pub(crate) use query;


pub fn bench_solana_parquet_query(bench: divan::Bencher, query: &Query) {
    bench_parquet_query(bench, "fixtures/solana/chunk", query)
}


pub fn bench_solana_storage_query(bench: divan::Bencher, query: &Query) {
    storage::bench_solana_query(bench, query)
}


fn bench_parquet_query(bench: divan::Bencher, chunk_path: &str, query: &Query) {
    let chunk = sqd_query::ParquetChunk::new(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(chunk_path)
            .to_str()
            .unwrap()
    );
    bench_query(bench, &chunk, query)
}


fn bench_query(bench: divan::Bencher, chunk: &dyn Chunk, query: &Query) {
    let plan = query.compile();
    bench.bench_local(|| {
        perform_query(&plan, chunk).unwrap()
    })
}


fn perform_query(plan: &Plan, chunk: &dyn Chunk) -> anyhow::Result<Vec<u8>> {
    sqd_polars::POOL.install(|| {
        let mut json_writer = JsonLinesWriter::new(Vec::new());
        if let Some(mut blocks) = plan.execute(chunk)? {
            json_writer.write_blocks(&mut blocks)?;
        }
        Ok(json_writer.finish()?)
    })
}


mod storage {
    use arrow::array::RecordBatchReader;
    use arrow::datatypes::Schema;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use sqd_data::solana::tables::SolanaChunkBuilder;
    use sqd_dataset::DatasetDescription;
    use sqd_storage::db::{Chunk, Database, DatabaseSettings, DatasetId, DatasetKind};
    use std::collections::BTreeMap;
    use std::fs::File;
    use std::path::Path;
    use std::sync::LazyLock;
    use tempfile::TempDir;
    use sqd_query::Query;
    use crate::query::util::bench_query;


    pub fn bench_solana_query(bench: divan::Bencher, query: &Query) {
        let db = DatabaseSettings::default()
            .set_data_cache_size(2048)
            .open(DB_DIR.path())
            .unwrap();

        let dataset_id = "solana".parse().unwrap();
        let snapshot = db.snapshot();
        let chunk = snapshot.get_first_chunk(dataset_id).unwrap().unwrap();
        let chunk_reader = snapshot.create_chunk_reader(chunk);

        bench_query(bench, &chunk_reader, query)
    }


    static DB_DIR: LazyLock<TempDir> = LazyLock::new(|| {
        let dir = tempfile::tempdir().unwrap();
        prepare_database(&dir).unwrap();
        dir
    });


    fn prepare_database(dir: &TempDir) -> anyhow::Result<()> {
        let db = DatabaseSettings::default().open(dir.path())?;
        prepare_solana_chunk(&db)?;
        Ok(())
    }


    fn prepare_solana_chunk(db: &Database) -> anyhow::Result<()> {
        let dataset_id = DatasetId::try_from("solana").unwrap();
        let dataset_kind = DatasetKind::try_from("solana").unwrap();

        db.create_dataset(dataset_id, dataset_kind)?;

        let mut tables = BTreeMap::new();

        let parquet_chunk_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/solana/chunk");

        for item_result in std::fs::read_dir(&parquet_chunk_path)? {
            let item = item_result?.file_name();
            let item_name = item.to_str().unwrap();

            if let Some(table) = item_name.strip_suffix(".parquet") {
                let file = File::open(parquet_chunk_path.join(&item))?;

                let mut parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
                    .with_batch_size(500)
                    .build()?;

                let mut builder = db.new_table_builder(parquet_reader.schema());

                builder.set_stats(
                    get_columns_with_stats(
                        &SolanaChunkBuilder::dataset_description(),
                        table,
                        &parquet_reader.schema()
                    )
                )?;

                while let Some(record_batch) = parquet_reader.next().transpose()? {
                    builder.write_record_batch(&record_batch)?;
                }

                tables.insert(table.to_string(), builder.finish()?);
            }
        }

        db.insert_chunk(dataset_id, &Chunk::V0 {
            first_block: 200000000,
            last_block: 200000899,
            last_block_hash: "hello".to_string(),
            parent_block_hash: "".to_string(),
            tables
        })?;

        Ok(())
    }


    fn get_columns_with_stats(d: &DatasetDescription, name: &str, schema: &Schema) -> Vec<usize> {
        if let Some(table_desc) = d.tables.get(name) {
            table_desc.options.column_options.iter()
                .filter_map(|(&name, opts)| {
                    opts.stats_enable.then(|| {
                        schema.index_of(name).unwrap()
                    })
                })
                .collect()
        } else {
            Vec::new()
        }
    }
}