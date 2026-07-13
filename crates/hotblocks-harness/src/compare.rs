//! The quiescence comparator (spec 12 §1) — diffs every observable against the reference model,
//! at quiescence: no pending input, watermarks stable. That is what absorbs the two legitimate
//! sources of nondeterminism, batch boundaries and coverage cuts (INV-22).
//!
//! `P-QUIESCENCE` (14) proposes `2 × P-CLEANUP-PERIOD` (20s), but that bound is about *space*
//! observables converging after a deferred sweep. Chain state settles as soon as the write path
//! commits, so the default here is far shorter. CT-7 must raise it back.

use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use serde_json::Value;

use crate::{
    chain::Chain,
    driver::{Client, Status},
    model::Model,
    types::{BlockNumber, BlockRef}
};

#[derive(Clone, Debug)]
pub struct Quiescence {
    /// How long the observables must hold still before they count as settled.
    pub stable_for: Duration,
    pub timeout: Duration,
    pub poll: Duration
}

impl Default for Quiescence {
    fn default() -> Self {
        Self {
            stable_for: Duration::from_millis(300),
            timeout: Duration::from_secs(30),
            poll: Duration::from_millis(50)
        }
    }
}

/// The triple that says "the write path has caught up with the script".
#[derive(Clone, Debug, PartialEq, Eq)]
struct Watermarks {
    first: Option<BlockNumber>,
    head: Option<BlockRef>,
    fin: Option<BlockRef>
}

impl Watermarks {
    fn of_model(m: &Model) -> Self {
        Self {
            first: m.first(),
            head: m.head(),
            fin: m.fin.clone()
        }
    }

    fn of_status(s: &Status) -> Self {
        match &s.data {
            None => Self {
                first: None,
                head: None,
                fin: None
            },
            Some(d) => Self {
                first: Some(d.first_block),
                head: Some(BlockRef::new(d.last_block, d.last_block_hash.clone())),
                fin: d.finalized_head.clone()
            }
        }
    }
}

/// Wait until the SUT's watermarks equal the model's and stay there.
pub async fn await_quiescence(client: &Client, model: &Model, q: &Quiescence) -> Result<()> {
    let expected = Watermarks::of_model(model);
    let deadline = Instant::now() + q.timeout;
    let mut stable_since: Option<Instant> = None;

    loop {
        let observed = Watermarks::of_status(&client.status().await?);
        if observed == expected {
            let since = *stable_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= q.stable_for {
                return Ok(());
            }
        } else {
            stable_since = None;
        }

        if Instant::now() > deadline {
            bail!(
                "the service did not converge on the script within {:?}\n  expected: {expected:?}\n  observed: {observed:?}",
                q.timeout
            );
        }
        tokio::time::sleep(q.poll).await;
    }
}

/// Diff every observable against the model, reporting *all* violations, not the first.
pub async fn assert_conforms(client: &Client, model: &Model, chain: &dyn Chain, dataset: &str) -> Result<()> {
    let mut errs: Vec<String> = Vec::new();

    // --- watermark reads (RP-12/RP-14, INV-30) --------------------------------------------
    let head = client.head().await?;
    if head != model.head() {
        errs.push(format!("HEAD: expected {:?}, got {head:?}", model.head()));
    }
    let fin = client.finalized_head().await?;
    if fin != model.fin {
        errs.push(format!("FINALIZED-HEAD: expected {:?}, got {fin:?}", model.fin));
    }

    let status = client.status().await?;
    if status.kind != chain.storage_kind() {
        errs.push(format!(
            "STATUS.kind: expected {:?}, got {:?}",
            chain.storage_kind(),
            status.kind
        ));
    }
    let observed = Watermarks::of_status(&status);
    let expected = Watermarks::of_model(model);
    if observed != expected {
        errs.push(format!("STATUS: expected {expected:?}, got {observed:?}"));
    }
    // INV-5/30: first <= fin <= head, in one consistent view.
    if let Some(data) = &status.data {
        if let Some(fin) = &data.finalized_head
            && !(data.first_block <= fin.number && fin.number <= data.last_block)
        {
            errs.push(format!(
                "INV-5: finalized head {} outside [{}, {}]",
                fin.number, data.first_block, data.last_block
            ));
        }
        let head_ts = model
            .head()
            .and_then(|h| model.blocks_in(h.number, h.number).first().map(|b| b.timestamp_ms));
        if data.last_block_timestamp != head_ts {
            errs.push(format!(
                "STATUS.lastBlockTimestamp: expected {head_ts:?}, got {:?}",
                data.last_block_timestamp
            ));
        }
    }

    let metadata = client.metadata().await?;
    let start_block = metadata["start_block"].as_u64();
    if start_block != model.first() {
        errs.push(format!(
            "METADATA.start_block: expected {:?}, got {start_block:?}",
            model.first()
        ));
    }

    // --- the OB-1 gauges ------------------------------------------------------------------
    let metrics = client.metrics().await?;
    let gauge = |name: &str| metrics.get(name, Some(("dataset", dataset)));
    if let (Some(first), Some(head)) = (model.first(), model.head()) {
        for (name, expected) in [
            ("hotblocks_first_block", first as f64),
            ("hotblocks_last_block", head.number as f64),
            // Reported as 0, not absent, when nothing is finalized yet.
            (
                "hotblocks_last_finalized_block",
                model.fin.as_ref().map_or(0, |f| f.number) as f64
            )
        ] {
            match gauge(name) {
                Some(got) if got == expected => {}
                got => errs.push(format!("OB-1 {name}: expected {expected}, got {got:?}"))
            }
        }
    }

    // --- the window itself (INV-7/21/22/23/27) --------------------------------------------
    if let (Some(first), Some(head)) = (model.first(), model.head()) {
        let emitted = client
            .scan(chain, first, head.number, model.anchor.hash.as_deref())
            .await?;
        let expected = model.emission(first, head.number, chain);

        if emitted.len() != expected.len() {
            errs.push(format!(
                "the scan of [{first}, {}] emitted {} blocks, the model predicts {}",
                head.number,
                emitted.len(),
                expected.len()
            ));
        }
        let mut shown = 0;
        for (got, want) in emitted.iter().zip(&expected) {
            if normalize(&got.raw) != normalize(want) {
                shown += 1;
                if shown <= 3 {
                    errs.push(format!(
                        "INV-22: block {} was emitted as {} but the model predicts {}",
                        got.number, got.raw, want
                    ));
                }
            }
        }
        if shown > 3 {
            errs.push(format!("... and {} more block mismatches", shown - 3));
        }
    }

    if errs.is_empty() {
        Ok(())
    } else {
        bail!("the service diverged from the model:\n  - {}", errs.join("\n  - "))
    }
}

/// An emitted block may carry an empty item collection or omit it; both mean "no items".
fn normalize(v: &Value) -> Value {
    let mut v = v.clone();
    if let Some(o) = v.as_object_mut() {
        o.retain(|_, value| !matches!(value, Value::Array(a) if a.is_empty()));
    }
    v
}
