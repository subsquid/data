use serde::Deserialize;
use sqd_primitives::BlockNumber;
use sqd_query::Query;

use crate::{errors::UnsupportedQuery, types::DatasetKind};

/// What the service reads from a query before an engine runs it: the dataset
/// kind and the blocks the answer covers.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryRange {
    pub kind: DatasetKind,
    pub first_block: BlockNumber,
    pub last_block: Option<BlockNumber>,
    pub parent_block_hash: Option<String>
}

impl QueryRange {
    pub fn of(query: &Query) -> Result<Self, UnsupportedQuery> {
        Ok(Self {
            kind: DatasetKind::from_query(query)?,
            first_block: query.first_block(),
            last_block: query.last_block(),
            parent_block_hash: query.parent_block_hash().map(str::to_owned)
        })
    }

    /// Read only the block keys of a request body to a dataset of `kind` and
    /// leave the rest, `type` too, to the new engine, which accepts requests the
    /// legacy parser refuses. The error is the message for the client.
    pub fn from_body(body: &[u8], kind: DatasetKind) -> Result<Self, String> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Keys {
            #[serde(default)]
            from_block: BlockNumber,
            to_block: Option<BlockNumber>,
            parent_block_hash: Option<String>
        }

        let keys: Keys = serde_json::from_slice(body).map_err(|err| format!("invalid query: {err}"))?;

        let reversed = keys.to_block.is_some_and(|to_block| to_block < keys.from_block);
        if reversed {
            return Err("got \"toBlock\" < \"fromBlock\"".to_string());
        }

        Ok(Self {
            kind,
            first_block: keys.from_block,
            last_block: keys.to_block,
            parent_block_hash: keys.parent_block_hash
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn from_body(body: serde_json::Value) -> Result<QueryRange, String> {
        QueryRange::from_body(body.to_string().as_bytes(), DatasetKind::Evm)
    }

    #[test]
    fn the_range_ignores_every_other_key() {
        let range = from_body(json!({
            "type": "evm",
            "fromBlock": 10,
            "toBlock": 20,
            "parentBlockHash": "0xab",
            "sets": {"s": {"fuse8": "AAAA", "canaries": []}},
            "logs": [{"address": [{"set": "s"}]}]
        }))
        .unwrap();

        assert_eq!(
            range,
            QueryRange {
                kind: DatasetKind::Evm,
                first_block: 10,
                last_block: Some(20),
                parent_block_hash: Some("0xab".to_string())
            }
        );
    }

    #[test]
    fn the_range_reads_the_keys_as_the_legacy_parser_does() {
        let body = json!({"type": "evm", "toBlock": 5});
        let legacy = QueryRange::of(&Query::from_json_value(body.clone()).unwrap()).unwrap();

        assert_eq!(from_body(body), Ok(legacy));
        assert!(from_body(json!({"type": "evm", "fromBlock": 6, "toBlock": 5})).is_err());
        assert!(from_body(json!({"type": "evm", "fromBlock": -1})).is_err());
        assert!(from_body(json!({"type": "evm", "parentBlockHash": 1})).is_err());
    }
}
