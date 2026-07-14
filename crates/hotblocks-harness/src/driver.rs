//! The client driver: the read binding (spec 13 §2-§5), the kind-agnostic structural validators
//! (12 §4), and the two normative client algorithms of 04 §7 — the backfill scanner and the
//! anchored follower.
//!
//! Every response goes through [`validate`] before anything else looks at it: a violation there
//! is a bug in the service, whatever the test was asserting.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant}
};

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    chain::Chain,
    types::{BlockNumber, BlockRef}
};

/// One block as the service emitted it (IB-4: one JSON object per record).
#[derive(Clone, Debug)]
pub struct Emitted {
    pub number: BlockNumber,
    pub hash: String,
    pub parent_hash: String,
    pub raw: Value
}

impl Emitted {
    pub fn as_ref(&self) -> BlockRef {
        BlockRef::new(self.number, self.hash.clone())
    }
}

/// IB-6 — the watermarks a query response carries.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Watermarks {
    pub head: Option<BlockNumber>,
    pub finalized: Option<BlockRef>
}

/// The error taxonomy of 04 §8, as it arrives over the binding.
#[derive(Debug)]
pub enum Outcome {
    Ok {
        blocks: Vec<Emitted>,
        watermarks: Watermarks
    },
    NoData {
        watermarks: Watermarks
    },
    Conflict {
        hints: Vec<BlockRef>
    },
    Error {
        status: u16,
        body: String
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    /// The *storage* kind, not always the config one (`hl-fills` vs `hyperliquid-fills`).
    pub kind: String,
    pub retention_strategy: Value,
    pub data: Option<StatusData>
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusData {
    pub first_block: BlockNumber,
    pub last_block: BlockNumber,
    pub last_block_hash: String,
    pub last_block_timestamp: Option<i64>,
    pub finalized_head: Option<BlockRef>
}

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    base: String,
    dataset: String
}

impl Client {
    pub fn new(base_url: impl Into<String>, dataset: impl Into<String>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .gzip(true)
            // A query may long-poll for `P-HEAD-WAIT` (5s) and then run for `P-QUERY-TIME` (10s).
            .timeout(Duration::from_secs(60))
            .no_proxy()
            .build()?;
        Ok(Self {
            http,
            base: base_url.into(),
            dataset: dataset.into()
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}/datasets/{}/{}", self.base, self.dataset, path)
    }

    pub async fn head(&self) -> Result<Option<BlockRef>> {
        Ok(self
            .http
            .get(self.url("head"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    pub async fn finalized_head(&self) -> Result<Option<BlockRef>> {
        Ok(self
            .http
            .get(self.url("finalized-head"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    pub async fn status(&self) -> Result<Status> {
        Ok(self
            .http
            .get(self.url("status"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    pub async fn metadata(&self) -> Result<Value> {
        Ok(self
            .http
            .get(self.url("metadata"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    /// SET-RETENTION (DEF-9). Returns the status code: `403` unless the dataset is API-controlled.
    pub async fn set_retention(&self, policy: &Value) -> Result<u16> {
        let res = self.http.post(self.url("retention")).json(policy).send().await?;
        Ok(res.status().as_u16())
    }

    /// The OB-* surface, parsed into `name{labels} -> value`.
    pub async fn metrics(&self) -> Result<Metrics> {
        let text = self
            .http
            .get(format!("{}/metrics", self.base))
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        let mut out = BTreeMap::new();
        for line in text.lines() {
            if line.starts_with('#') {
                continue;
            }
            if let Some((key, value)) = line.rsplit_once(' ')
                && let Ok(value) = value.parse::<f64>()
            {
                out.insert(key.to_string(), value);
            }
        }
        Ok(Metrics(out))
    }

    /// Reads the service's existing intrinsic RocksDB diagnostic property surface.
    pub async fn rocksdb_property(&self, column_family: &str, name: &str) -> Result<Option<u64>> {
        let res = self
            .http
            .get(format!("{}/rocksdb/prop/{column_family}/{name}", self.base))
            .send()
            .await?;
        if res.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(res.error_for_status()?.text().await?.trim().parse()?))
    }

    pub async fn query(&self, body: &Value) -> Result<Outcome> {
        self.query_at("stream", body).await
    }

    pub async fn query_finalized(&self, body: &Value) -> Result<Outcome> {
        self.query_at("finalized-stream", body).await
    }

    async fn query_at(&self, path: &str, body: &Value) -> Result<Outcome> {
        let res = self.http.post(self.url(path)).json(body).send().await?;
        let status = res.status().as_u16();

        // Headers must be read before the body consumes the response.
        let watermarks = Watermarks {
            head: header(&res, "x-sqd-head-number").and_then(|v| v.parse().ok()),
            finalized: match (
                header(&res, "x-sqd-finalized-head-number").and_then(|v| v.parse().ok()),
                header(&res, "x-sqd-finalized-head-hash")
            ) {
                (Some(number), Some(hash)) => Some(BlockRef::new(number, hash)),
                (None, None) => None,
                _ => bail!("IB-6: the finalized-head headers must be present as a pair")
            }
        };

        Ok(match status {
            200 => Outcome::Ok {
                blocks: parse_jsonl(&res.text().await?)?,
                watermarks
            },
            204 => Outcome::NoData { watermarks },
            409 => {
                let body: ConflictBody = res.json().await.context("malformed CONFLICT payload")?;
                ensure!(
                    !body.previous_blocks.is_empty(),
                    "RP-11: CONFLICT hints must be non-empty"
                );
                Outcome::Conflict {
                    hints: body.previous_blocks
                }
            }
            _ => Outcome::Error {
                status,
                body: res.text().await.unwrap_or_default()
            }
        })
    }

    /// The backfill scanner of 04 §7: walk `[from, to]` with anchored, budget-truncated responses
    /// until the window is covered.
    pub async fn scan(
        &self,
        chain: &dyn Chain,
        from: BlockNumber,
        to: BlockNumber,
        expected_parent: Option<&str>
    ) -> Result<Vec<Emitted>> {
        let mut out: Vec<Emitted> = Vec::new();
        let mut cursor = from;
        let mut parent = expected_parent.map(str::to_string);
        let deadline = Instant::now() + Duration::from_secs(120);

        while cursor <= to {
            ensure!(
                Instant::now() < deadline,
                "the scan of [{from}, {to}] did not converge: stuck at {cursor} after {} blocks",
                out.len()
            );
            let query = chain.scan_query(cursor, Some(to), parent.as_deref());
            match self.query(&query).await? {
                Outcome::Ok { blocks, watermarks } => {
                    let bounds = Bounds {
                        from: cursor,
                        to: Some(to),
                        expected_parent: parent.clone(),
                        include_all: true
                    };
                    validate(&blocks, &watermarks, &bounds)?;
                    // Under `include_all`, an empty success means zero coverage (INV-25, RP-9).
                    let last = blocks.last().context("INV-25: successful response covered no block")?;
                    cursor = last.number + 1;
                    parent = Some(last.hash.clone());
                    out.extend(blocks);
                }
                Outcome::NoData { watermarks } => bail!(
                    "NO_DATA at {cursor} while scanning the committed window [{from}, {to}] \
                     (watermarks: {watermarks:?})"
                ),
                Outcome::Conflict { hints } => bail!(
                    "unexpected CONFLICT at {cursor}: the scan follows the chain it is reading; \
                     hints = {hints:?}"
                ),
                Outcome::Error { status, body } => bail!("query failed with {status}: {body}")
            }
        }
        Ok(out)
    }
}

/// Prometheus text output, keyed by `name{labels}` exactly as exposed.
#[derive(Clone, Debug)]
pub struct Metrics(BTreeMap<String, f64>);

impl Metrics {
    pub fn get(&self, name: &str, label: Option<(&str, &str)>) -> Option<f64> {
        self.0
            .iter()
            .find(|(key, _)| {
                let Some(rest) = key.strip_prefix(name) else {
                    return false;
                };
                match label {
                    None => rest.is_empty(),
                    Some((k, v)) => rest.starts_with('{') && rest.contains(&format!("{k}=\"{v}\""))
                }
            })
            .map(|(_, value)| *value)
    }
}

/// What a response is allowed to contain, per the request that produced it.
pub struct Bounds {
    pub from: BlockNumber,
    pub to: Option<BlockNumber>,
    pub expected_parent: Option<String>,
    /// The query emits every covered block, so consecutive records must be parent and child —
    /// whatever their numbers, which are sparse on a slot-numbered chain.
    pub include_all: bool
}

/// The structural validators of 12 §4 — the cheap 80% of INV-21/22/23, checkable without
/// understanding the kind.
pub fn validate(blocks: &[Emitted], watermarks: &Watermarks, bounds: &Bounds) -> Result<()> {
    let Some(first) = blocks.first() else { return Ok(()) };

    // Strictly ascending, unique, linked.
    for w in blocks.windows(2) {
        ensure!(
            w[1].number > w[0].number,
            "INV-21: emitted blocks are not strictly ascending ({} then {})",
            w[0].number,
            w[1].number
        );
        if bounds.include_all || w[1].number == w[0].number + 1 {
            ensure!(
                w[1].parent_hash == w[0].hash,
                "INV-2/23: block {} does not link to block {}",
                w[1].number,
                w[0].number
            );
        }
    }

    // Within the requested range and at or below the reported head.
    ensure!(
        first.number >= bounds.from,
        "INV-27: block {} is below the requested from {}",
        first.number,
        bounds.from
    );
    let last = blocks.last().expect("non-empty");
    if let Some(to) = bounds.to {
        ensure!(
            last.number <= to,
            "INV-27: block {} is above the requested to {to}",
            last.number
        );
    }
    if let Some(head) = watermarks.head {
        ensure!(
            last.number <= head,
            "INV-27: block {} is above the reported head {head}",
            last.number
        );
    }

    // Anchored continuation: the first block links to what the client asserted.
    if let Some(expected) = &bounds.expected_parent {
        ensure!(
            &first.parent_hash == expected,
            "INV-23: block {} claims parent {} but the client anchored on {expected}",
            first.number,
            first.parent_hash
        );
    }

    // Watermark coherence, whenever both are reported.
    if let (Some(head), Some(fin)) = (watermarks.head, watermarks.finalized.as_ref()) {
        ensure!(
            fin.number <= head,
            "INV-5/30: finalized head {} is above the head {head}",
            fin.number
        );
    }
    Ok(())
}

/// The anchored follower of 04 §7, CONFLICT recovery included. Its local chain is what a correct
/// client would hold.
pub struct Follower {
    pub local: Vec<Emitted>,
    pub from: BlockNumber,
    pub parent: Option<String>,
    pub rollbacks: u32
}

#[derive(Debug, PartialEq, Eq)]
pub enum FollowStep {
    Applied(usize),
    NoData,
    RolledBack { to: BlockNumber }
}

impl Follower {
    pub fn new(from: BlockNumber, parent: Option<String>) -> Self {
        Self {
            local: Vec::new(),
            from,
            parent,
            rollbacks: 0
        }
    }

    pub fn head(&self) -> Option<BlockRef> {
        self.local.last().map(Emitted::as_ref)
    }

    /// One round trip of the client loop.
    pub async fn step(&mut self, client: &Client, chain: &dyn Chain) -> Result<FollowStep> {
        let query = chain.scan_query(self.from, None, self.parent.as_deref());
        match client.query(&query).await? {
            Outcome::Ok { blocks, watermarks } => {
                let bounds = Bounds {
                    from: self.from,
                    to: None,
                    expected_parent: self.parent.clone(),
                    include_all: true
                };
                validate(&blocks, &watermarks, &bounds)?;
                let last = blocks.last().context("INV-25: successful response covered no block")?;
                if let Some(tail) = self.local.last() {
                    ensure!(
                        blocks[0].parent_hash == tail.hash,
                        "INV-23: the follower's chain broke between responses at block {}",
                        blocks[0].number
                    );
                }
                self.from = last.number + 1;
                self.parent = Some(last.hash.clone());
                let n = blocks.len();
                self.local.extend(blocks);
                Ok(FollowStep::Applied(n))
            }
            Outcome::NoData { .. } => Ok(FollowStep::NoData),
            Outcome::Conflict { hints } => {
                self.rollbacks += 1;
                // "a = highest hint the local chain has" (04 §7).
                let anchor = hints
                    .iter()
                    .rev()
                    .find(|h| self.local.iter().any(|b| b.number == h.number && b.hash == h.hash))
                    .cloned();
                match anchor {
                    Some(a) => {
                        self.local.retain(|b| b.number <= a.number);
                        self.from = a.number + 1;
                        self.parent = Some(a.hash);
                        Ok(FollowStep::RolledBack { to: self.from - 1 })
                    }
                    None => {
                        // Nothing in common: walk back to the lowest hint and re-anchor there.
                        let lowest = hints.first().expect("hints are non-empty").clone();
                        self.local.retain(|b| b.number < lowest.number);
                        self.from = lowest.number;
                        self.parent = None;
                        Ok(FollowStep::RolledBack {
                            to: lowest.number.saturating_sub(1)
                        })
                    }
                }
            }
            Outcome::Error { status, body } => bail!("the follower got {status}: {body}")
        }
    }

    /// Follow until the local chain reaches `target`, or give up.
    pub async fn follow_to(
        &mut self,
        client: &Client,
        chain: &dyn Chain,
        target: BlockNumber,
        timeout: Duration
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        while self.head().is_none_or(|h| h.number < target) {
            ensure!(
                Instant::now() < deadline,
                "the follower did not reach block {target} within {timeout:?} (at {:?})",
                self.head()
            );
            self.step(client, chain).await?;
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConflictBody {
    previous_blocks: Vec<BlockRef>
}

fn header(res: &reqwest::Response, name: &str) -> Option<String> {
    res.headers().get(name)?.to_str().ok().map(str::to_string)
}

/// IB-4 — a success body decodes as JSON Lines, one block object per record.
fn parse_jsonl(text: &str) -> Result<Vec<Emitted>> {
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        let raw: Value = serde_json::from_str(line)
            .with_context(|| format!("IB-4: record {i} is not a JSON object: {:.120}", line))?;
        let header = &raw["header"];
        let number = header["number"]
            .as_u64()
            .with_context(|| format!("record {i} has no header.number"))?;
        let hash = header["hash"]
            .as_str()
            .with_context(|| format!("record {i} has no header.hash"))?
            .to_string();
        let parent_hash = header["parentHash"]
            .as_str()
            .with_context(|| format!("record {i} has no header.parentHash"))?
            .to_string();
        out.push(Emitted {
            number,
            hash,
            parent_hash,
            raw
        });
    }
    Ok(out)
}
