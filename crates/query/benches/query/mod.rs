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


#[divan::bench_group(sample_size = 5, sample_count = 20)]
mod solana_hard {
    use crate::query::util::{bench_parquet_query, query};

    query!(QUERY, {
        "type": "solana",
        "fields": {
        "block": {
          "number": true,
          "parentNumber": true,
          "parentHash": true,
          "height": true,
          "timestamp": true
        },
        "transaction": {
          "signatures": true,
          "err": true,
          "feePayer": true
        },
        "instruction": {
          "programId": true,
          "accounts": true,
          "data": true,
          "isCommitted": true
        },
        "log": {
          "instructionAddress": true,
          "programId": true,
          "kind": true,
          "message": true
        },
        "balance": {
          "pre": true,
          "post": true
        },
        "tokenBalance": {
          "preMint": true,
          "postMint": true,
          "preDecimals": true,
          "postDecimals": true,
          "preOwner": true,
          "postOwner": true,
          "preAmount": true,
          "postAmount": true
        },
        "reward": {
          "lamports": true,
          "postBalance": true,
          "rewardType": true,
          "commission": true
        }
        },
        "instructions": [
        {
          "programId": [
            "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"
          ],
          "d8": [
            "0xf8c69e91e17587c8",
            "0xb59d59438fb63448",
            "0x1c8cee63e7a21595",
            "0x0703967f94283dc8",
            "0x2905eeaf64e106cd",
            "0x5e9b6797465fdca5",
            "0xa1c26754ab47fa9a",
            "0x5055d14818ceb16c",
            "0x0a333d2370691855",
            "0x1a526698f04a691a",
            "0x2d9aedd2dd0fa65c"
          ],
          "isCommitted": true,
          "transaction": true,
          "transactionTokenBalances": true,
          "innerInstructions": true
        }
        ]
    });

    #[divan::bench]
    fn chunk_200(bench: divan::Bencher) {
        bench_parquet_query(bench, "benches/data/solana/200", &QUERY)
    }

    #[divan::bench]
    fn chunk_400(bench: divan::Bencher) {
        bench_parquet_query(bench, "benches/data/solana/400", &QUERY)
    }

    #[divan::bench]
    fn large(bench: divan::Bencher) {
        bench_parquet_query(bench, "benches/data/solana/large", &QUERY)
    }
}