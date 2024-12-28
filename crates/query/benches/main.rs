use criterion::{criterion_group, criterion_main, Criterion};
use serde_json::json;
use sqd_query::{Chunk, JsonLinesWriter, Plan, Query};
use std::sync::LazyLock;


#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;


// #[global_allocator]
// static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;


static WHIRLPOOL_SWAP: LazyLock<Query> = LazyLock::new(|| {
    let query = json!({
        "type": "solana",
        "fromBlock": 200_000_000,
        "fields": {
            "block": {
                "slot": true,
                "parentHash": true,
                "parentSlot": true
            },
            "transaction": {
                "signatures": true,
                "err": true
            },
            "instruction": {
                "programId": true,
                "accounts": true,
                "data": true
            }
        },
        "instructions": [
            {
                "programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"],
                "d8": ["0xf8c69e91e17587c8"],
                // "innerInstructions": true,
                "transaction": true,
                "isCommitted": true
            }
        ]
    });

    Query::from_json_value(query).unwrap()
});


fn perform_query(plan: &Plan, chunk: &dyn Chunk) -> anyhow::Result<Vec<u8>> {
    sqd_polars::POOL.install(|| {
        let mut json_writer = JsonLinesWriter::new(Vec::new());
        let mut blocks = plan.execute(chunk)?;
        json_writer.write_blocks(&mut blocks)?;
        Ok(json_writer.finish()?)
    })
}


mod parquet {
    use crate::{perform_query, WHIRLPOOL_SWAP};
    use criterion::Criterion;
    use sqd_query::ParquetChunk;
    use std::path::Path;


    pub fn setup(c: &mut Criterion) {
        c.bench_function("parquet: whirlpool swap", |bench| {
            let chunk = ParquetChunk::new(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("fixtures/solana/chunk")
                    .to_str()
                    .unwrap()
            );

            let plan = WHIRLPOOL_SWAP.compile();

            bench.iter(|| {
                perform_query(&plan, &chunk).unwrap()
            })
        });
    }
}


mod storage {
    use crate::{perform_query, WHIRLPOOL_SWAP};
    use arrow::array::RecordBatchReader;
    use arrow::datatypes::Schema;
    use criterion::Criterion;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use sqd_data::solana::tables::SolanaChunkBuilder;
    use sqd_dataset::DatasetDescription;
    use sqd_storage::db::{Chunk, Database, DatabaseSettings, DatasetId, DatasetKind};
    use std::fs::File;
    use std::path::Path;


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

    pub fn setup(c: &mut Criterion) {
        let db_dir = tempfile::tempdir().unwrap();
        let db = DatabaseSettings::default().open(db_dir.path()).unwrap();
        let dataset_id = prepare_solana_chunk(&db).unwrap();

        drop(db);

        let db = DatabaseSettings::default()
            .set_data_cache_size(1024)
            .open(db_dir.path())
            .unwrap();
        
        let snapshot = db.snapshot();
        let chunk = snapshot.get_first_chunk(dataset_id).unwrap().unwrap();
        let chunk_reader = snapshot.create_chunk_reader(chunk);
        
        c.bench_function("storage: whirlpool swap", |bench| {
            let plan = WHIRLPOOL_SWAP.compile();

            bench.iter(|| {
                perform_query(&plan, &chunk_reader).unwrap()
            })
        });

        if let Some(stats) = db.get_statistics() {
            println!("{}", stats);
        }
    }

    fn prepare_solana_chunk(db: &Database) -> anyhow::Result<DatasetId> {
        let dataset_id = DatasetId::try_from("solana").unwrap();
        let dataset_kind = DatasetKind::try_from("solana").unwrap();

        db.create_dataset(dataset_id, dataset_kind)?;

        let chunk_builder = db.new_chunk_builder();

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

                let mut writer = chunk_builder.add_table(
                    table,
                    parquet_reader.schema()
                );
                
                writer.set_stats(
                    get_columns_with_stats(
                        &SolanaChunkBuilder::dataset_description(),
                        table,
                        &parquet_reader.schema()
                    ) 
                )?;

                while let Some(record_batch) = parquet_reader.next().transpose()? {
                    writer.write_record_batch(&record_batch)?;
                }

                writer.finish()?
            }
        }

        db.insert_chunk(dataset_id, &Chunk::V0 {
            first_block: 200000000,
            last_block: 200000899,
            last_block_hash: "hello".to_string(),
            parent_block_hash: "".to_string(),
            tables: chunk_builder.finish()
        })?;

        Ok(dataset_id)
    }
}


criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = parquet::setup, storage::setup
);


criterion_main!(benches);