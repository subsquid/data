use std::sync::Arc;

use anyhow::Result;
use sqd_hotblocks_harness::{
    chain::Evm,
    harness::{Harness, HarnessConfig},
    types::block_hash
};

const START: u64 = 1_000;

#[tokio::test(flavor = "multi_thread")]
async fn hash_lookup_resolves_ingested_blocks_and_returns_none_for_unknown_hash() -> Result<()> {
    let mut h = start_harness().await?;

    if let Err(err) = assert_ingest_lookup(&mut h).await {
        panic!("block-hash ingest lookup failed: {err:?}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn hash_lookup_removes_the_replaced_branch_after_a_reorg() -> Result<()> {
    let mut h = start_harness().await?;

    if let Err(err) = assert_reorg_lookup(&mut h).await {
        panic!("block-hash reorg lookup failed: {err:?}");
    }
    Ok(())
}

async fn start_harness() -> Result<Harness> {
    let mut cfg = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(Evm), START);
    cfg.sut_args.push("--block-hash-index".into());
    Harness::start(cfg).await
}

async fn assert_ingest_lookup(h: &mut Harness) -> Result<()> {
    h.produce(20)?;
    h.settle().await?;
    h.assert_hash_index_conforms().await?;

    let unknown_hash = block_hash(START + 1_000, 99);
    assert_eq!(
        h.client.block_by_hash(&unknown_hash).await?,
        None,
        "a hash outside the ingested chain must return 404"
    );
    assert_eq!(
        h.client.block_by_hash_status(&"é".repeat(256)).await?,
        400,
        "the existing block endpoint must enforce the 256-byte limit"
    );

    Ok(())
}

async fn assert_reorg_lookup(h: &mut Harness) -> Result<()> {
    h.produce(20)?;
    h.settle().await?;
    h.assert_hash_index_conforms().await?;

    let fork_from = START + 10;
    let stale_hashes: Vec<String> = h
        .model
        .blocks_in(fork_from, START + 19)
        .iter()
        .map(|block| block.hash.clone())
        .collect();

    h.fork(fork_from, 10)?;
    h.settle().await?;
    h.assert_conforms().await?;
    h.assert_hash_index_conforms().await?;

    for hash in stale_hashes {
        assert_eq!(
            h.client.block_by_hash(&hash).await?,
            None,
            "a hash from the replaced branch must return 404"
        );
    }

    Ok(())
}
