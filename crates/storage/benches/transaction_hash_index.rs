use std::{collections::BTreeMap, sync::Arc, time::Duration};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, UInt32Array, UInt64Array},
    datatypes::{DataType, Field, Schema}
};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use sqd_storage::db::{Chunk, Database, DatabaseSettings, DatasetId, DatasetKind};
use tempfile::TempDir;

struct Fixture {
    // Field order is intentional: close RocksDB before TempDir removes its files.
    db: Database,
    chunk: Chunk,
    dataset_id: DatasetId,
    _dir: TempDir
}

fn transaction_hash(index: usize) -> String {
    format!("0x{index:064x}")
}

fn fixture(transaction_count: usize, index_enabled: bool) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseSettings::default()
        .with_transaction_hash_index(index_enabled)
        .open(dir.path())
        .unwrap();
    let dataset_id = DatasetId::from_str("benchmark");
    db.create_dataset(dataset_id, DatasetKind::from_str("evm")).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("hash", DataType::Utf8, false),
    ]));
    let block_numbers = Arc::new(UInt64Array::from(
        (0..transaction_count)
            .map(|index| (index / 100) as u64)
            .collect::<Vec<_>>()
    )) as ArrayRef;
    let transaction_indexes = Arc::new(UInt32Array::from(
        (0..transaction_count)
            .map(|index| (index % 100) as u32)
            .collect::<Vec<_>>()
    )) as ArrayRef;
    let hashes = (0..transaction_count).map(transaction_hash).collect::<Vec<_>>();
    let hashes = Arc::new(StringArray::from(hashes.iter().map(String::as_str).collect::<Vec<_>>())) as ArrayRef;

    let mut builder = db.new_table_builder(schema.clone());
    builder
        .write_record_batch(&RecordBatch::try_new(schema, vec![block_numbers, transaction_indexes, hashes]).unwrap())
        .unwrap();
    let mut tables = BTreeMap::new();
    tables.insert("transactions".to_owned(), builder.finish().unwrap());
    let last_block = transaction_count.saturating_sub(1) as u64 / 100;
    let chunk = Chunk::V1 {
        first_block: 0,
        last_block,
        last_block_hash: format!("block-{last_block}"),
        parent_block_hash: "base".to_owned(),
        first_block_time: None,
        last_block_time: None,
        tables
    };

    Fixture {
        db,
        chunk,
        dataset_id,
        _dir: dir
    }
}

fn ingest_benchmark(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("transaction_hash_index/ingest");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    for transaction_count in [1_000, 10_000] {
        group.throughput(Throughput::Elements(transaction_count as u64));
        for enabled in [false, true] {
            let variant = if enabled { "enabled" } else { "disabled" };
            group.bench_with_input(
                BenchmarkId::new(variant, transaction_count),
                &transaction_count,
                |bencher, &transaction_count| {
                    bencher.iter_batched(
                        || fixture(transaction_count, enabled),
                        |fixture| {
                            fixture.db.insert_chunk(fixture.dataset_id, &fixture.chunk).unwrap();
                            fixture
                        },
                        BatchSize::LargeInput
                    );
                }
            );
        }
    }
    group.finish();
}

fn lookup_benchmark(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("transaction_hash_index/lookup");

    for transaction_count in [1_000, 100_000] {
        let fixture = fixture(transaction_count, true);
        fixture.db.insert_chunk(fixture.dataset_id, &fixture.chunk).unwrap();
        let hit = transaction_hash(transaction_count - 1);
        let miss = transaction_hash(transaction_count + 1);

        group.bench_function(BenchmarkId::new("hit", transaction_count), |bencher| {
            bencher.iter(|| {
                criterion::black_box(
                    fixture
                        .db
                        .snapshot()
                        .find_transaction_by_hash(fixture.dataset_id, criterion::black_box(&hit))
                        .unwrap()
                )
            });
        });
        group.bench_function(BenchmarkId::new("miss", transaction_count), |bencher| {
            bencher.iter(|| {
                criterion::black_box(
                    fixture
                        .db
                        .snapshot()
                        .find_transaction_by_hash(fixture.dataset_id, criterion::black_box(&miss))
                        .unwrap()
                )
            });
        });
    }
    group.finish();
}

criterion_group!(benches, ingest_benchmark, lookup_benchmark);
criterion_main!(benches);
