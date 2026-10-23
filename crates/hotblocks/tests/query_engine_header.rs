//! A stream request with `x-sqd-query-engine: dynamic` is answered by the new query
//! engine alone: with the bytes the legacy engine sends, and with no shadow.

use std::{path::Path, sync::Arc};

use anyhow::{Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use serde_json::{Value, json};
use sqd_hotblocks_harness::{
    Harness, HarnessConfig,
    chain::{Chain, Evm},
    types::{BlockNumber, block_hash}
};
use xorf::BinaryFuse8;
use xxhash_rust::xxh3::xxh3_64;

const START: BlockNumber = 1_000;
/// Copies of the pinned engine revision's catalogs; refresh them when the pin
/// moves.
const METADATA_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/metadata");
const CONTRACT: &str = "0x3333333333333333333333333333333333333333";
const SENDER: &str = "0x2222222222222222222222222222222222222222";
const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
/// The catalog edit after which the new engine no longer knows the logs request,
/// which the legacy one answers.
const NO_LOGS: (&str, &str) = (
    "    request:\n      name: logs\n",
    "    request:\n      name: no_logs\n"
);

#[tokio::test(flavor = "multi_thread")]
async fn the_new_engine_answers_with_the_legacy_bytes() -> Result<()> {
    let mut h = start(Some(Path::new(METADATA_DIR)), &[]).await?;
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
        expect_same_answer(&h, "stream", query, 200).await?;
    }
    expect_same_answer(&h, "finalized-stream", &queries[0], 200).await?;

    // Only the requests without the header were shadowed.
    let metrics = h.client.metrics().await?;
    let shadowed = metrics.get("hotblocks_shadow_queries_total", Some(("dataset", &h.dataset)));
    assert_eq!(shadowed, Some(queries.len() as f64 + 1.0));
    ensure!(
        metrics.get("hotblocks_http_status_total", Some(("engine", "dynamic"))) > Some(0.0),
        "no response was counted for the new engine"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_new_engine_is_the_one_that_answers() -> Result<()> {
    // Rolled in another order, every log's topics differ in the new engine's answer.
    let catalogs = edited_catalogs(
        "columns: [ topic0, topic1, topic2, topic3 ]",
        "columns: [ topic1, topic0, topic2, topic3 ]"
    )?;
    let h = start(Some(catalogs.path()), &[]).await?;

    let query = Evm.scan_query(START, None, None);
    let legacy = ask(&h, "stream", &query, None).await?;
    let new = ask(&h, "stream", &query, Some("dynamic")).await?;
    ensure!(legacy.status == 200 && new.status == 200, "{legacy:?}\n{new:?}");
    ensure!(
        legacy.body != new.body,
        "the legacy engine answered the request for the new one"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_answer_the_new_engine_cuts_goes_on_from_the_cut() -> Result<()> {
    // Every block outweighs the new engine's budget, so it cuts each chunk's
    // answer after one block.
    let catalogs = edited_catalogs(
        "      hash:\n        type: string\n        encoding: hex_bytes\n      parent_hash:\n",
        "      hash:\n        type: string\n        encoding: hex_bytes\n        weight: 100000000\n      parent_hash:\n"
    )?;
    let h = start(Some(catalogs.path()), &[]).await?;

    for query in [
        Evm.scan_query(START, None, None),
        Evm.scan_query(START + 3, Some(START + 15), None)
    ] {
        expect_same_answer(&h, "stream", &query, 200).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_new_engine_reports_a_fork_as_the_legacy_one_does() -> Result<()> {
    let h = start(Some(Path::new(METADATA_DIR)), &[]).await?;

    // Some of these blocks start a chunk and some are inside one, where the
    // engine checks the parent hash itself.
    for first in [START + 5, START + 17, START + 33] {
        let query = Evm.scan_query(first, None, Some(&block_hash(first - 1, 1)));
        expect_same_answer(&h, "stream", &query, 409).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_engine_is_refused() -> Result<()> {
    let h = start(Some(Path::new(METADATA_DIR)), &[]).await?;

    let answer = ask(&h, "stream", &Evm.scan_query(START, None, None), Some("newest")).await?;
    ensure!(answer.status == 400, "{answer:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_new_engine_is_refused_without_a_catalog() -> Result<()> {
    let h = start(None, &[]).await?;

    let answer = ask(&h, "stream", &Evm.scan_query(START, None, None), Some("dynamic")).await?;
    ensure!(answer.status == 400, "{answer:?}");
    ensure!(answer.body.contains("new query engine"), "{answer:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_set_query_is_answered_only_with_the_header() -> Result<()> {
    let h = start(Some(Path::new(METADATA_DIR)), &[]).await?;

    let fields = json!({"log": {"logIndex": true, "transactionIndex": true, "address": true}});
    let with_list = json!({
        "type": "evm", "fromBlock": START, "fields": fields,
        "logs": [{"address": [CONTRACT]}]
    });
    let with_set = json!({
        "type": "evm", "fromBlock": START, "fields": fields,
        "sets": {"contracts": value_set(&[CONTRACT])},
        "logs": [{"address": [{"set": "contracts"}]}]
    });

    let list = ask(&h, "stream", &with_list, None).await?;
    ensure!(list.status == 200 && list.body.contains(CONTRACT), "{list:?}");

    let legacy = ask(&h, "stream", &with_set, None).await?;
    ensure!(legacy.status == 422, "the legacy engine took a set: {legacy:?}");

    let dynamic = ask(&h, "stream", &with_set, Some("dynamic")).await?;
    ensure!(
        dynamic == list,
        "the set answered unlike the list:\nset: {dynamic:?}\nlist: {list:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_query_the_new_engine_refuses_is_a_client_error() -> Result<()> {
    let catalogs = edited_catalogs(NO_LOGS.0, NO_LOGS.1)?;
    let h = start(Some(catalogs.path()), &[]).await?;

    let query = Evm.scan_query(START, None, None);
    let legacy = ask(&h, "stream", &query, None).await?;
    ensure!(legacy.status == 200, "{legacy:?}");
    let refused = ask(&h, "stream", &query, Some("dynamic")).await?;
    ensure!(refused.status == 400, "{refused:?}");

    let other_kind = json!({"type": "solana", "fromBlock": START});
    let refused = ask(&h, "stream", &other_kind, Some("dynamic")).await?;
    ensure!(refused.status == 400, "{refused:?}");

    // Above the head both are refused before the wait for it, not answered as
    // no data when the wait runs out.
    let above_head = START + 1_000;
    for query in [
        Evm.scan_query(above_head, None, None),
        json!({"type": "solana", "fromBlock": above_head})
    ] {
        let refused = ask(&h, "stream", &query, Some("dynamic")).await?;
        ensure!(refused.status == 400, "{query} answered {refused:?}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dynamic_query_is_compiled_only_once_admitted() -> Result<()> {
    // With no query capacity, a query the new engine cannot compile is turned
    // away as busy: compiling it first would answer 400.
    let catalogs = edited_catalogs(NO_LOGS.0, NO_LOGS.1)?;
    let h = start(Some(catalogs.path()), &["--query-task-queue", "0"]).await?;

    let answer = ask(&h, "stream", &Evm.scan_query(START, None, None), Some("dynamic")).await?;
    ensure!(answer.status == 503, "{answer:?}");
    Ok(())
}

/// EVM blocks written and settled, with every request without the header
/// shadowed.
async fn start(metadata_dir: Option<&Path>, args: &[&str]) -> Result<Harness> {
    let mut cfg = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(Evm), START);
    cfg.sut_args.extend(
        ["--shadow-percent", "100", "--shadow-max-in-flight", "64"]
            .into_iter()
            .map(String::from)
    );
    if let Some(dir) = metadata_dir {
        cfg.sut_args.push("--metadata-dir".to_string());
        cfg.sut_args.push(dir.display().to_string());
    }
    cfg.sut_args.extend(args.iter().map(|arg| arg.to_string()));

    let mut h = Harness::start(cfg).await?;
    h.produce(60)?;
    h.settle().await?;
    Ok(h)
}

/// The test catalogs with one edit to `evm.yaml`, which makes the new engine
/// answer differently from the legacy one.
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

/// A set as a client declares it: a binary fuse filter over the XXH3 of each
/// value's text, with the values as its canaries.
fn value_set(values: &[&str]) -> Value {
    let mut keys: Vec<u64> = values.iter().map(|value| xxh3_64(value.as_bytes())).collect();
    keys.sort_unstable();
    keys.dedup();
    let filter = BinaryFuse8::try_from(&keys).expect("the keys are distinct");

    let descriptor = &filter.descriptor;
    let mut bytes = descriptor.seed.to_le_bytes().to_vec();
    bytes.extend(descriptor.segment_length.to_le_bytes());
    bytes.extend(descriptor.segment_length_mask.to_le_bytes());
    bytes.extend(descriptor.segment_count_length.to_le_bytes());
    bytes.extend(filter.fingerprints.iter());

    json!({"fuse8": BASE64.encode(bytes), "canaries": values})
}

#[derive(Debug, PartialEq)]
struct Answer {
    status: u16,
    body: String
}

async fn ask(h: &Harness, endpoint: &str, query: &Value, engine: Option<&str>) -> Result<Answer> {
    let client = reqwest::Client::builder().gzip(true).build()?;
    let mut request = client
        .post(format!("{}/datasets/{}/{endpoint}", h.sut.base_url(), h.dataset))
        .json(query);
    if let Some(engine) = engine {
        request = request.header("x-sqd-query-engine", engine);
    }

    let response = request.send().await?;
    Ok(Answer {
        status: response.status().as_u16(),
        body: response.text().await?
    })
}

async fn expect_same_answer(h: &Harness, endpoint: &str, query: &Value, status: u16) -> Result<()> {
    let legacy = ask(h, endpoint, query, None).await?;
    ensure!(legacy.status == status, "{query} answered {legacy:?}");
    ensure!(!legacy.body.is_empty(), "{query} answered with no body");

    let new = ask(h, endpoint, query, Some("dynamic")).await?;
    ensure!(
        new == legacy,
        "{query} answered differently\nlegacy: {legacy:?}\nnew: {new:?}"
    );
    Ok(())
}
