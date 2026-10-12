//! The new query engine, shadowing every query, answers what the legacy engine
//! does on the storage this service writes, and the shadow reports when it
//! does not.

use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqd_hotblocks_harness::{
    Harness, HarnessConfig, Numbering, Outcome,
    chain::{Chain, Evm, Solana},
    driver::Metrics,
    types::{BlockNumber, block_hash}
};

const START: BlockNumber = 1_000;
/// Copies of the pinned engine revision's catalogs; refresh them when the pin
/// moves.
const METADATA_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/metadata");
const CONTRACT: &str = "0x3333333333333333333333333333333333333333";
const SENDER: &str = "0x2222222222222222222222222222222222222222";
const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
/// Rolled in another order, every log's topics differ and no column changes type.
const TOPICS: &str = "columns: [ topic0, topic1, topic2, topic3 ]";
const SWAPPED_TOPICS: &str = "columns: [ topic1, topic0, topic2, topic3 ]";

#[tokio::test(flavor = "multi_thread")]
async fn the_new_engine_answers_evm_queries_like_the_legacy_one() -> Result<()> {
    let mut h = start(Arc::new(Evm), Numbering::Dense).await?;
    h.produce(60)?;
    h.settle().await?;
    h.finalize(START + 40)?;
    h.settle().await?;

    let fields = json!({
        "block": {"number": true, "hash": true, "parentHash": true, "timestamp": true},
        "transaction": {"transactionIndex": true, "hash": true, "from": true, "nonce": true},
        "log": {"logIndex": true, "transactionIndex": true, "address": true, "topics": true, "data": true}
    });
    let queries = [
        Evm.scan_query(START, None, None),
        Evm.scan_query(START + 7, Some(START + 23), None),
        Evm.scan_query(START + 5, None, Some(&block_hash(START + 4, 0))),
        json!({
            "type": "evm", "fromBlock": START, "fields": fields,
            "logs": [{"address": [CONTRACT], "topic0": [TRANSFER_TOPIC], "transaction": true}]
        }),
        json!({
            "type": "evm", "fromBlock": START + 3, "fields": fields,
            "transactions": [{"from": [SENDER], "logs": true}]
        }),
        json!({
            "type": "evm", "fromBlock": START, "toBlock": START + 30, "includeAllBlocks": true,
            "fields": fields, "logs": [{"address": [CONTRACT]}]
        }),
        json!({
            "type": "evm", "fromBlock": START, "fields": fields,
            "logs": [{"address": ["0x4444444444444444444444444444444444444444"]}]
        })
    ];
    for query in &queries {
        expect_data(h.client.query(query).await?, query)?;
    }
    expect_data(h.client.query_finalized(&queries[0]).await?, &queries[0])?;

    assert_engines_agree(&h).await
}

#[tokio::test(flavor = "multi_thread")]
async fn the_new_engine_answers_solana_queries_like_the_legacy_one() -> Result<()> {
    let mut h = start(Arc::new(Solana), Numbering::Sparse).await?;
    h.produce(50)?;
    h.settle().await?;

    for query in [
        Solana.scan_query(START, None, None),
        Solana.scan_query(START + 10, Some(START + 60), None)
    ] {
        expect_data(h.client.query(&query).await?, &query)?;
    }

    assert_engines_agree(&h).await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_different_answer_is_counted_and_logged() -> Result<()> {
    let catalogs = edited_catalogs(TOPICS, SWAPPED_TOPICS)?;
    let h = start_evm(catalogs.path(), &[]).await?;

    let query = Evm.scan_query(START, None, None);
    expect_data(h.client.query(&query).await?, &query)?;

    let runs = Runs::collect(&h).await?;
    ensure!(runs.count("mismatch") > 0.0, "no mismatch was counted");
    ensure!(
        runs.logged("shadow engine answered differently", "logs[0].topics[0]"),
        "no mismatch was logged:\n{}",
        runs.findings.join("\n")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_log_limit_of_zero_silences_the_logs_but_not_the_counts() -> Result<()> {
    let catalogs = edited_catalogs(TOPICS, SWAPPED_TOPICS)?;
    let h = start_evm(catalogs.path(), &["--shadow-log-limit", "0"]).await?;

    let query = Evm.scan_query(START, None, None);
    expect_data(h.client.query(&query).await?, &query)?;

    let runs = Runs::collect(&h).await?;
    ensure!(runs.count("mismatch") > 0.0, "no mismatch was counted");
    ensure!(
        !runs.logged("shadow engine answered differently", ""),
        "a mismatch was logged:\n{}",
        runs.findings.join("\n")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_error_of_one_engine_is_counted_and_logged() -> Result<()> {
    // The new engine no longer knows the logs request, which the legacy one answers.
    let catalogs = edited_catalogs(
        "    request:\n      name: logs\n",
        "    request:\n      name: no_logs\n"
    )?;
    let h = start_evm(catalogs.path(), &[]).await?;

    let query = Evm.scan_query(START, None, None);
    expect_data(h.client.query(&query).await?, &query)?;

    let runs = Runs::collect(&h).await?;
    ensure!(runs.count("new_error") > 0.0, "no new engine error was counted");
    ensure!(
        runs.logged("shadow engine failed where the other answered", "new_error"),
        "no new engine error was logged:\n{}",
        runs.findings.join("\n")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn answers_cut_at_different_blocks_are_compared_on_the_same_blocks() -> Result<()> {
    // Every block outweighs the new engine's budget, so it cuts each answer
    // after one block; the legacy engine does not.
    let catalogs = edited_catalogs(
        "      hash:\n        type: string\n        encoding: hex_bytes\n      parent_hash:\n",
        "      hash:\n        type: string\n        encoding: hex_bytes\n        weight: 100000000\n      parent_hash:\n"
    )?;
    let h = start_evm(catalogs.path(), &[]).await?;

    for query in [
        Evm.scan_query(START, None, None),
        Evm.scan_query(START + 3, Some(START + 15), None)
    ] {
        expect_data(h.client.query(&query).await?, &query)?;
    }

    // The cut is found without the block numbers in the answers. The harness
    // client needs them, so this query is sent as is.
    let mut without_numbers = Evm.scan_query(START, None, None);
    without_numbers["fields"]["block"]
        .as_object_mut()
        .expect("the scan query selects block fields")
        .remove("number");
    let response = reqwest::Client::new()
        .post(format!("{}/datasets/{}/stream", h.sut.base_url(), h.dataset))
        .json(&without_numbers)
        .send()
        .await?;
    let status = response.status();
    ensure!(status == 200, "{without_numbers} answered {status}");
    let body = response.text().await?;
    ensure!(
        !body.contains("\"number\""),
        "the answer holds block numbers: {body:.300}"
    );

    assert_engines_agree(&h).await
}

#[tokio::test(flavor = "multi_thread")]
async fn no_query_is_shadowed_without_metadata_catalogs() -> Result<()> {
    let mut cfg = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(Evm), START);
    cfg.sut_args
        .extend(["--shadow-percent", "100"].into_iter().map(String::from));
    let mut h = Harness::start(cfg).await?;
    h.produce(10)?;
    h.settle().await?;

    let query = Evm.scan_query(START, None, None);
    expect_data(h.client.query(&query).await?, &query)?;

    let metrics = h.client.metrics().await?;
    let shadowed = metrics.get("hotblocks_shadow_queries_total", Some(("dataset", &h.dataset)));
    assert_eq!(shadowed, None);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_shadow_share_changes_without_a_restart() -> Result<()> {
    let mut cfg = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(Evm), START);
    cfg.sut_args
        .extend(["--metadata-dir", METADATA_DIR].into_iter().map(String::from));
    let mut h = Harness::start(cfg).await?;
    h.produce(10)?;
    h.settle().await?;
    let query = Evm.scan_query(START, None, None);
    let shadowed = |metrics: &Metrics| metrics.get("hotblocks_shadow_queries_total", Some(("dataset", &h.dataset)));

    expect_data(h.client.query(&query).await?, &query)?;
    assert_eq!(shadowed(&h.client.metrics().await?), None);

    assert_eq!(
        h.client.update_settings(&json!({"shadow": {"percent": 150}})).await?,
        400
    );
    assert_eq!(
        h.client.update_settings(&json!({"shadow": {"percent": 100}})).await?,
        200
    );
    assert_eq!(h.client.settings().await?["shadow"]["percent"], json!(100.0));

    expect_data(h.client.query(&query).await?, &query)?;
    let metrics = h.client.metrics().await?;
    assert_eq!(shadowed(&metrics), Some(1.0));
    assert_eq!(metrics.get("hotblocks_shadow_percent", None), Some(100.0));
    Ok(())
}

async fn start(chain: Arc<dyn Chain>, numbering: Numbering) -> Result<Harness> {
    start_with(chain, numbering, Path::new(METADATA_DIR), &[]).await
}

/// EVM blocks written and settled, with the new engine reading `metadata_dir`.
async fn start_evm(metadata_dir: &Path, args: &[&str]) -> Result<Harness> {
    let mut h = start_with(Arc::new(Evm), Numbering::Dense, metadata_dir, args).await?;
    h.produce(20)?;
    h.settle().await?;
    Ok(h)
}

async fn start_with(
    chain: Arc<dyn Chain>,
    numbering: Numbering,
    metadata_dir: &Path,
    args: &[&str]
) -> Result<Harness> {
    let mut cfg = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), chain, START);
    cfg.numbering = numbering;
    cfg.sut_args.extend(
        [
            "--shadow-percent",
            "100",
            "--shadow-max-in-flight",
            "64",
            "--metadata-dir"
        ]
        .into_iter()
        .map(String::from)
    );
    cfg.sut_args.push(metadata_dir.display().to_string());
    cfg.sut_args.extend(args.iter().map(|arg| arg.to_string()));
    Harness::start(cfg).await
}

/// The test catalogs with one edit to `evm.yaml`, which makes the new engine
/// disagree with the legacy one.
fn edited_catalogs(from: &str, to: &str) -> Result<tempfile::TempDir> {
    let evm = std::fs::read_to_string(Path::new(METADATA_DIR).join("evm.yaml"))?;
    ensure!(
        evm.matches(from).count() == 1,
        "evm.yaml holds `{from}` other than once"
    );

    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("evm.yaml"), evm.replace(from, to))?;
    Ok(dir)
}

fn expect_data(outcome: Outcome, query: &Value) -> Result<()> {
    ensure!(
        matches!(outcome, Outcome::Ok { .. }),
        "{query} did not return data: {outcome:?}"
    );
    Ok(())
}

/// Require that every shadow run matched.
async fn assert_engines_agree(h: &Harness) -> Result<()> {
    let runs = Runs::collect(h).await?;
    for name in ["mismatch", "legacy_error", "new_error", "both_error", "skipped"] {
        ensure!(
            runs.count(name) == 0.0,
            "{} shadow runs ended in {name}:\n{}",
            runs.count(name),
            runs.findings.join("\n")
        );
    }
    ensure!(runs.count("match") > 0.0, "no shadow run was compared");
    ensure!(
        runs.metrics
            .get("hotblocks_shadow_queries_total", Some(("dataset", &h.dataset)))
            > Some(0.0),
        "no query was shadowed"
    );
    Ok(())
}

/// The outcome of every shadow run and the lines the shadow logged.
struct Runs {
    metrics: Metrics,
    findings: Vec<String>
}

impl Runs {
    /// Wait until no run is in flight. Every run is submitted before its
    /// response ends, and a run leaves the in-flight gauge only after it is
    /// counted.
    async fn collect(h: &Harness) -> Result<Self> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let metrics = loop {
            let metrics = h.client.metrics().await?;
            let in_flight = metrics.get("hotblocks_shadow_in_flight", None);
            if in_flight == Some(0.0) {
                break metrics;
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "shadow runs did not finish: {in_flight:?} in flight"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        let log = std::fs::read_to_string(h.sut.log_path()).unwrap_or_default();
        Ok(Self {
            metrics,
            findings: log
                .lines()
                .filter(|line| line.contains("shadow engine"))
                .map(String::from)
                .collect()
        })
    }

    fn count(&self, outcome: &str) -> f64 {
        self.metrics
            .get("hotblocks_shadow_runs_total", Some(("outcome", outcome)))
            .unwrap_or(0.0)
    }

    fn logged(&self, message: &str, detail: &str) -> bool {
        self.findings
            .iter()
            .any(|line| line.contains(message) && line.contains(detail))
    }
}
