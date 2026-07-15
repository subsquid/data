//! CT-5 — error-soundness request matrix (INV-26).

use std::sync::Arc;

use anyhow::Result;
use serde_json::{Value, json};
use sqd_hotblocks_harness::{
    Harness, HarnessConfig, Numbering, Outcome,
    chain::{Chain, Evm, Solana},
    types::{Block, BlockNumber}
};

const START: BlockNumber = 1_000;

/// EVM data with one deliberately unavailable optional collection (RP-8).
struct EvmWithoutTraces;

impl Chain for EvmWithoutTraces {
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
        let mut value = Evm.source_block(block);
        value
            .as_object_mut()
            .expect("EVM source blocks are objects")
            .remove("traces");
        value
    }

    fn scan_query(&self, from: BlockNumber, to: Option<BlockNumber>, expected_parent: Option<&str>) -> Value {
        Evm.scan_query(from, to, expected_parent)
    }

    fn expected_emission(&self, block: &Block) -> Value {
        Evm.expected_emission(block)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ct5_query_errors_are_internally_classified_and_remain_text() -> Result<()> {
    let mut h = Harness::start(HarnessConfig::from_block(
        env!("CARGO_BIN_EXE_sqd-hotblocks"),
        Arc::new(EvmWithoutTraces),
        START
    ))
    .await?;
    h.produce(4)?;
    h.settle().await?;

    let mut malformed = Evm.scan_query(START, None, None);
    malformed["toBlock"] = json!(START - 1);
    assert_text_error(h.client.query(&malformed).await?, 400);

    assert_text_error(h.client.query(&Solana.scan_query(START, None, None)).await?, 400);
    assert_text_error(h.client.query(&Evm.scan_query(START - 1, None, None)).await?, 400);

    let mut unavailable_item = Evm.scan_query(START, None, None);
    unavailable_item["traces"] = json!([{}]);
    assert_text_error(h.client.query(&unavailable_item).await?, 400);

    for dialect in ["substrate", "fuel"] {
        let outcome = h
            .client
            .query(&json!({
                "type": dialect,
                "fromBlock": START
            }))
            .await?;
        let Outcome::Error { status, body } = outcome else {
            panic!("{dialect} query did not return an error: {outcome:?}")
        };
        assert_eq!(status, 400, "{body}");
        assert!(body.contains(dialect), "unexpected text error body: {body}");
    }

    assert!(
        h.client.head().await?.is_some(),
        "unsupported queries must not kill the service"
    );

    let metrics = h.client.metrics().await?;
    for (code, minimum) in [
        ("MALFORMED_REQUEST", 1.0),
        ("KIND_MISMATCH", 1.0),
        ("RANGE_UNAVAILABLE", 1.0),
        ("ITEM_UNAVAILABLE", 1.0),
        ("UNSUPPORTED_QUERY", 2.0)
    ] {
        let count = metrics
            .get("hotblocks_http_status_total", Some(("error_class", code)))
            .unwrap_or_default();
        assert!(
            count >= minimum,
            "{code} was not counted: got {count}, want at least {minimum}"
        );
    }

    Ok(())
}

fn assert_text_error(outcome: Outcome, expected_status: u16) {
    let Outcome::Error { status, body } = outcome else {
        panic!("expected a text error, got {outcome:?}")
    };
    assert_eq!(status, expected_status, "{body}");
    assert!(!body.is_empty());
}

#[ignore = "GAP-21 deferred; see crates/query/src/plan/plan.rs FIXME(GAP-21)"]
#[tokio::test(flavor = "multi_thread")]
async fn ct5_anchor_is_evaluated_across_a_large_number_hole() -> Result<()> {
    let mut cfg = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(Solana), START);
    cfg.numbering = Numbering::FixedGap(1_000);
    let mut h = Harness::start(cfg).await?;
    h.produce(3)?;
    h.settle().await?;

    let preceding = h.model.seg[0].clone();
    let from = preceding.number + 500;

    let outcome = h
        .client
        .query(&Solana.scan_query(from, None, Some(&preceding.hash)))
        .await?;
    assert!(
        matches!(outcome, Outcome::Ok { .. }),
        "matching ancestry returned {outcome:?}"
    );

    let outcome = h.client.query(&Solana.scan_query(from, None, Some("0xwrong"))).await?;
    let Outcome::Conflict { hints } = outcome else {
        panic!("mismatching ancestry did not return CONFLICT: {outcome:?}")
    };
    assert_eq!(hints.last(), Some(&preceding.as_ref()));

    Ok(())
}
