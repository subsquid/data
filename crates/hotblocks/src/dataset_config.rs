use std::{collections::BTreeMap, fmt};

use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, IgnoredAny, MapAccess, Visitor}
};
use sqd_query::BlockNumber;
use sqd_storage::db::DatasetId;
use url::Url;

use crate::types::DatasetKind;

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub enum RetentionConfig {
    // Fixed, starting from the block number
    FromBlock {
        number: BlockNumber,
        parent_hash: Option<String>
    },
    // Moving window that keeps up to N blocks
    Head(u64),
    // Retention is set dynamically from the portal. `max_blocks` is a soft cap
    // applied when the portal stops advancing the floor, see `Ctl::max_blocks`.
    Api {
        max_blocks: Option<u64>
    },
    None
}

const RETENTION_VARIANTS: &[&str] = &["FromBlock", "Head", "Api", "None"];

// Hand-written to accept both the bare `Api` string and `Api: { max_blocks: N }`.
// The config is read through `singleton_map_recursive`, which encodes unit
// variants as strings and struct variants as maps, so a derived impl can accept
// only one of the two forms. Can be derived again once no config uses bare `Api`.
impl<'de> Deserialize<'de> for RetentionConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct FromBlockCfg {
            number: BlockNumber,
            #[serde(default)]
            parent_hash: Option<String>
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct ApiCfg {
            #[serde(default)]
            max_blocks: Option<u64>
        }

        struct RetentionVisitor;

        impl<'de> Visitor<'de> for RetentionVisitor {
            type Value = RetentionConfig;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a retention strategy")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                match value {
                    "Api" => Ok(RetentionConfig::Api { max_blocks: None }),
                    "None" => Ok(RetentionConfig::None),
                    other => Err(E::unknown_variant(other, RETENTION_VARIANTS))
                }
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let tag: String = map
                    .next_key()?
                    .ok_or_else(|| de::Error::custom("expected a retention strategy"))?;

                let strategy = match tag.as_str() {
                    "FromBlock" => {
                        let cfg: FromBlockCfg = map.next_value()?;
                        RetentionConfig::FromBlock {
                            number: cfg.number,
                            parent_hash: cfg.parent_hash
                        }
                    }
                    "Head" => RetentionConfig::Head(map.next_value()?),
                    "Api" => {
                        let cfg: ApiCfg = map.next_value()?;
                        RetentionConfig::Api {
                            max_blocks: cfg.max_blocks
                        }
                    }
                    "None" => {
                        map.next_value::<IgnoredAny>()?;
                        RetentionConfig::None
                    }
                    other => return Err(de::Error::unknown_variant(other, RETENTION_VARIANTS))
                };

                if map.next_key::<IgnoredAny>()?.is_some() {
                    return Err(de::Error::custom("retention strategy must have a single key"));
                }

                Ok(strategy)
            }
        }

        deserializer.deserialize_any(RetentionVisitor)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetConfig {
    pub kind: DatasetKind,
    pub retention_strategy: RetentionConfig,
    #[serde(default)]
    pub disable_compaction: bool,
    pub data_sources: Vec<Url>
}

impl DatasetConfig {
    pub fn read_config_file(file: &str) -> anyhow::Result<BTreeMap<DatasetId, DatasetConfig>> {
        let reader = std::io::BufReader::new(std::fs::File::open(file)?);
        let deser = serde_yaml::Deserializer::from_reader(reader);
        let config = serde_yaml::with::singleton_map_recursive::deserialize(deser)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mirror how the field is read inside `read_config_file`.
    fn parse(yaml: &str) -> Result<RetentionConfig, serde_yaml::Error> {
        let deser = serde_yaml::Deserializer::from_str(yaml);
        serde_yaml::with::singleton_map_recursive::deserialize(deser)
    }

    #[test]
    fn api_accepts_both_forms() {
        assert_eq!(parse("Api").unwrap(), RetentionConfig::Api { max_blocks: None });
        assert_eq!(parse("Api: {}").unwrap(), RetentionConfig::Api { max_blocks: None });
        assert_eq!(
            parse("Api:\n  max_blocks: 100000").unwrap(),
            RetentionConfig::Api {
                max_blocks: Some(100000)
            }
        );
        assert!(parse("Api:\n  max_blcks: 5").is_err());
    }

    #[test]
    fn other_strategies_still_parse() {
        assert_eq!(parse("None").unwrap(), RetentionConfig::None);
        assert_eq!(parse("Head: 2000").unwrap(), RetentionConfig::Head(2000));
        assert_eq!(
            parse("FromBlock:\n  number: 10\n  parent_hash: '0xabc'").unwrap(),
            RetentionConfig::FromBlock {
                number: 10,
                parent_hash: Some("0xabc".to_owned())
            }
        );
        assert!(parse("Bogus").is_err());
    }
}
