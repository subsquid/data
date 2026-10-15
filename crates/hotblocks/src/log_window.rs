//! Rate limits for log lines that carry a client's query.

use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    time::{Duration, Instant}
};

use sqd_storage::db::DatasetId;

/// How much of a query a log line carries: a query can list tens of thousands
/// of addresses.
const MAX_LOGGED_QUERY_BYTES: usize = 16 * 1024;
/// The window a log limit counts lines in.
const LOG_WINDOW: Duration = Duration::from_secs(300);

/// The queries logged in detail in the current [`LOG_WINDOW`].
pub struct LogWindow {
    started: Instant,
    /// Query fingerprints and reasons.
    logged: HashSet<(u64, &'static str)>
}

impl LogWindow {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            logged: HashSet::new()
        }
    }

    /// Whether this query may be logged in detail for this reason now: at most
    /// `limit` lines per window, one per query shape and reason.
    pub fn claim(&mut self, fingerprint: u64, reason: &'static str, limit: usize) -> bool {
        if self.started.elapsed() >= LOG_WINDOW {
            self.started = Instant::now();
            self.logged.clear();
        }
        self.logged.len() < limit && self.logged.insert((fingerprint, reason))
    }
}

/// The query with its block range and parent hash removed: what stays the
/// same while a client follows the chain.
pub fn query_fingerprint(dataset_id: DatasetId, body: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    dataset_id.as_str().hash(&mut hasher);
    match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(serde_json::Value::Object(mut query)) => {
            for key in ["fromBlock", "toBlock", "parentBlockHash"] {
                query.remove(key);
            }
            serde_json::Value::Object(query).to_string().hash(&mut hasher);
        }
        _ => body.hash(&mut hasher)
    }
    hasher.finish()
}

pub fn truncate_query(mut query: String) -> String {
    if query.len() > MAX_LOGGED_QUERY_BYTES {
        let mut cut = MAX_LOGGED_QUERY_BYTES;
        while !query.is_char_boundary(cut) {
            cut -= 1;
        }
        query.truncate(cut);
        query.push_str("…<truncated>");
    }
    query
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_window_logs_each_query_shape_once_up_to_the_limit() {
        let mut window = LogWindow::new();

        assert!(window.claim(1, "slow", 2));
        assert!(!window.claim(1, "slow", 2), "the same shape was logged twice");
        assert!(window.claim(1, "mismatch", 2), "another reason is another line");
        assert!(!window.claim(2, "slow", 2), "the limit was exceeded");
    }

    #[test]
    fn the_block_range_does_not_change_the_fingerprint() {
        let dataset_id = DatasetId::from_str("fingerprint-test");
        let first = br#"{"type":"evm","fromBlock":1,"toBlock":9,"parentBlockHash":"0x01","logs":[{}]}"#;
        let next = br#"{"type":"evm","fromBlock":10,"logs":[{}]}"#;
        let other = br#"{"type":"evm","fromBlock":10,"transactions":[{}]}"#;

        assert_eq!(
            query_fingerprint(dataset_id, first),
            query_fingerprint(dataset_id, next)
        );
        assert_ne!(
            query_fingerprint(dataset_id, next),
            query_fingerprint(dataset_id, other)
        );
    }
}
