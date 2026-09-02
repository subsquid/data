use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;
use sqd_hotblocks_harness::{
    chain::{Chain, Evm},
    compare::expected_transactions,
    driver::Client,
    harness::{Harness, HarnessConfig},
    types::{Block, BlockNumber, TransactionRef, block_hash}
};

const START: u64 = 1_000;
const REINCLUDED_HASH: &str = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

#[tokio::test(flavor = "multi_thread")]
async fn transaction_lookup_resolves_ingested_transactions_and_misses_unknown_hashes() -> Result<()> {
    let mut harness = start_harness(Arc::new(Evm)).await?;

    harness.produce(20)?;
    settle_or_panic(&harness).await;
    harness.assert_transaction_hash_index_conforms().await?;

    assert_eq!(
        harness
            .client
            .transaction_by_hash(&block_hash(START + 10_000, 99))
            .await?,
        None
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn transaction_lookup_removes_the_replaced_branch_after_a_reorg() -> Result<()> {
    let mut harness = start_harness(Arc::new(Evm)).await?;
    harness.produce(20)?;
    settle_or_panic(&harness).await;

    let fork_from = START + 10;
    let stale_hashes: Vec<String> = harness
        .model
        .seg
        .iter()
        .filter(|block| block.number >= fork_from)
        .flat_map(|block| expected_transactions(&*harness.chain, block).unwrap())
        .map(|transaction| transaction.hash)
        .collect();

    harness.fork(fork_from, 10)?;
    settle_or_panic(&harness).await;
    harness.assert_transaction_hash_index_conforms().await?;

    for hash in stale_hashes {
        assert_eq!(
            harness.client.transaction_by_hash(&hash).await?,
            None,
            "a transaction from the replaced branch must stop resolving"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn reorg_reinclusion_resolves_to_the_new_block_and_index() -> Result<()> {
    let mut harness = start_harness(Arc::new(ReinclusionEvm)).await?;
    harness.produce(20)?;
    settle_or_panic(&harness).await;

    assert_eq!(
        harness.client.transaction_by_hash(REINCLUDED_HASH).await?,
        Some(TransactionRef::new(START + 11, 0, REINCLUDED_HASH))
    );

    harness.fork(START + 10, 10)?;
    harness.settle().await?;
    harness.assert_transaction_hash_index_conforms().await?;

    assert_eq!(
        harness.client.transaction_by_hash(REINCLUDED_HASH).await?,
        Some(TransactionRef::new(START + 13, 0, REINCLUDED_HASH)),
        "INV-47 requires old-branch removals to happen before new-branch insertion"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn disabled_index_misses_and_malformed_hashes_are_rejected_before_storage() -> Result<()> {
    let config = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(Evm), START);
    let mut harness = Harness::start(config).await?;
    harness.produce(20)?;
    settle_or_panic(&harness).await;

    let indexed_while_disabled = harness
        .model
        .seg
        .iter()
        .flat_map(|block| expected_transactions(&*harness.chain, block).unwrap())
        .next()
        .expect("the EVM fixture should emit at least one transaction");
    assert_eq!(
        harness.client.transaction_by_hash(&indexed_while_disabled.hash).await?,
        None,
        "a disabled index must behave like an empty index"
    );

    assert_eq!(
        harness.client.transaction_by_hash_status(&"é".repeat(256)).await?,
        400,
        "P-HASH-MAXLEN counts UTF-8 bytes"
    );

    let unknown_dataset = Client::new(harness.sut.base_url(), "unknown-dataset")?;
    assert_eq!(
        unknown_dataset.transaction_by_hash_status(&"x".repeat(257)).await?,
        400,
        "an oversized hash must be rejected before dataset/storage lookup"
    );
    Ok(())
}

async fn start_harness(chain: Arc<dyn Chain>) -> Result<Harness> {
    let mut config = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), chain, START);
    config.sut_args.push("--transaction-hash-index".into());
    Harness::start(config).await
}

async fn settle_or_panic(harness: &Harness) {
    if let Err(err) = harness.settle().await {
        panic!("transaction-index harness failed to settle: {err:?}");
    }
}

/// Re-includes one transaction from block 1011 at block 1013 on the first
/// replacement branch. The normal EVM generator makes hashes branch-specific,
/// so this focused chain variant exercises the ordering hazard in INV-47.
struct ReinclusionEvm;

impl Chain for ReinclusionEvm {
    fn config_kind(&self) -> &'static str {
        Evm.config_kind()
    }

    fn storage_kind(&self) -> &'static str {
        Evm.storage_kind()
    }

    fn dialect(&self) -> &'static str {
        Evm.dialect()
    }

    fn source_block(&self, block: &Block) -> Value {
        rewrite_reincluded_transaction(block, Evm.source_block(block))
    }

    fn scan_query(&self, from: BlockNumber, to: Option<BlockNumber>, expected_parent: Option<&str>) -> Value {
        Evm.scan_query(from, to, expected_parent)
    }

    fn expected_emission(&self, block: &Block) -> Value {
        rewrite_reincluded_transaction(block, Evm.expected_emission(block))
    }
}

fn rewrite_reincluded_transaction(block: &Block, mut value: Value) -> Value {
    let is_old_position = block.fork_id == 0 && block.number == START + 11;
    let is_new_position = block.fork_id == 1 && block.number == START + 13;
    if is_old_position || is_new_position {
        let transaction = value
            .get_mut("transactions")
            .and_then(Value::as_array_mut)
            .and_then(|transactions| transactions.first_mut())
            .expect("both selected odd-numbered blocks carry one transaction");
        transaction["hash"] = Value::String(REINCLUDED_HASH.to_owned());
    }
    value
}
