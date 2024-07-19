use std::time::Duration;
use criterion::{Criterion, criterion_group, criterion_main};
use serde_json::json;
use sqd_query::{JsonLinesWriter, Query, StorageChunk};
use sqd_storage::db::{Database, DatasetId};


fn setup(c: &mut Criterion) {
    let db = Database::open("../../node.temp.db").unwrap();
    let solana = DatasetId::try_from("solana").unwrap();

    let whirlpool_swap = {
        let query = json!({
            "type": "solana",
            "fromBlock": 256001100,
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
                    "innerInstructions": true,
                    "transaction": true,
                    "isCommitted": true
                }
            ]
        });

        Query::from_json_value(query).unwrap()
    };

    c.bench_function("storage: whirlpool swap", |b| {
        b.iter(|| {
            perform_query(solana, &whirlpool_swap, &db).unwrap()
        })
    });
}


fn perform_query(ds: DatasetId, query: &Query, db: &Database) -> anyhow::Result<Vec<u8>> {
    let plan = query.compile();
    let snapshot = db.get_snapshot();
    sqd_polars::POOL.install(|| {
        let mut json_writer = JsonLinesWriter::new(Vec::new());
        for chunk_result in snapshot.list_chunks(
            ds,
            query.first_block().unwrap_or(0),
            query.last_block()
        ) {
            let chunk = chunk_result?;
            let mut blocks = plan.execute(&StorageChunk::new(&chunk))?;
            json_writer.write_blocks(&mut blocks)?;
        }
        Ok(json_writer.finish()?)
    })
}


criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(30));
    targets = setup
);
criterion_main!(benches);