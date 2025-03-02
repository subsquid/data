mod util;


#[divan::bench_group(sample_size = 20)]
mod whirlpool_swap {
    use super::util::{bench_solana_parquet_query, bench_solana_storage_query, query};

    query!(QUERY, {
        "type": "solana",
        "fromBlock": 0,
        "fields": {
            "block": {
                "number": true,
                "hash": true,
                "parentNumber": true
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

    #[divan::bench]
    fn parquet(bench: divan::Bencher) {
        bench_solana_parquet_query(bench, &QUERY)
    }

    #[divan::bench]
    fn storage(bench: divan::Bencher) {
        bench_solana_storage_query(bench, &QUERY)
    }
}