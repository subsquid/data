use std::{
    collections::HashSet,
    fmt,
    io::ErrorKind,
    path::{Path, PathBuf}
};

use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use sqd_query::{Chunk, JsonArrayWriter, Query};

/// Deserialization target that accepts any JSON document, but fails if some
/// object in it repeats a property name.
///
/// `serde_json::Value` cannot be used for this: its map keeps a single entry
/// per key, so a repeated property is silently collapsed and the defect never
/// reaches an assertion. Every fixture comparison in this file parses the
/// response, so a duplicate property is invisible to all of them.
struct UniqueKeys;

impl<'de> Deserialize<'de> for UniqueKeys {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(UniqueKeysVisitor)
    }
}

struct UniqueKeysVisitor;

impl<'de> Visitor<'de> for UniqueKeysVisitor {
    type Value = UniqueKeys;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut seen: HashSet<String> = HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
            map.next_value::<UniqueKeys>()?;
            if !seen.insert(key.clone()) {
                return Err(de::Error::custom(format!("duplicate property {:?}", key)));
            }
        }
        Ok(UniqueKeys)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        while seq.next_element::<UniqueKeys>()?.is_some() {}
        Ok(UniqueKeys)
    }

    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        Deserialize::deserialize(d)
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self::Value, E> {
        Ok(UniqueKeys)
    }
    fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self::Value, E> {
        Ok(UniqueKeys)
    }
    fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self::Value, E> {
        Ok(UniqueKeys)
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
        Ok(UniqueKeys)
    }
    fn visit_str<E: de::Error>(self, _: &str) -> Result<Self::Value, E> {
        Ok(UniqueKeys)
    }
    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(UniqueKeys)
    }
    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(UniqueKeys)
    }
}

/// A response must be a valid JSON document in which no object repeats a
/// property name (RFC 8259 s4). Checked on the raw bytes, before any parse.
fn assert_unique_keys(bytes: &[u8], query_file: &Path) {
    if let Err(err) = serde_json::from_slice::<UniqueKeys>(bytes) {
        panic!("{}: {}", query_file.display(), err)
    }
}

fn execute_query_bytes(chunk: &dyn Chunk, query_json: &[u8]) -> anyhow::Result<Vec<u8>> {
    let query = Query::from_json_bytes(query_json)?;
    let data = Vec::with_capacity(4 * 1024 * 1024);
    let mut writer = JsonArrayWriter::new(data);
    if let Some(mut blocks) = query.compile().execute(chunk)? {
        writer.write_blocks(&mut blocks)?;
    }
    Ok(writer.finish()?)
}

fn execute_query(chunk: &dyn Chunk, query_file: impl AsRef<Path>) -> anyhow::Result<Vec<u8>> {
    execute_query_bytes(chunk, &std::fs::read(query_file)?)
}

fn test_fixture(chunk: &dyn Chunk, query_file: PathBuf) {
    let case_dir = query_file.parent().unwrap();
    let result_file = case_dir.join("result.json");

    let actual: serde_json::Value = match execute_query(chunk, &query_file) {
        Ok(bytes) => {
            assert_unique_keys(&bytes, &query_file);
            serde_json::from_slice(&bytes).unwrap()
        }
        Err(err) => serde_json::Value::String(err.to_string())
    };

    let expected: serde_json::Value = match std::fs::read(&result_file) {
        Ok(expected_bytes) => serde_json::from_slice(&expected_bytes).unwrap(),
        Err(err) if err.kind() == ErrorKind::NotFound => {
            serde_json::to_writer_pretty(
                std::fs::File::create(case_dir.join("actual.temp.json")).unwrap(),
                &actual
            )
            .unwrap();
            return;
        }
        Err(err) => panic!("{:?}", err)
    };

    if expected != actual {
        serde_json::to_writer_pretty(
            std::fs::File::create(case_dir.join("actual.temp.json")).unwrap(),
            &actual
        )
        .unwrap();
        panic!("actual != expected")
    }
}

#[cfg(feature = "parquet")]
mod parquet {
    use std::path::{Path, PathBuf};

    use rstest::rstest;
    use sqd_query::ParquetChunk;

    use crate::{assert_unique_keys, execute_query_bytes, test_fixture};

    #[rstest]
    fn query(#[files("fixtures/*/queries/*/query.json")] query_file: PathBuf) {
        let case_dir = query_file.parent().unwrap();
        let chunk_dir = case_dir.parent().unwrap().parent().unwrap().join("chunk");
        let chunk = ParquetChunk::new(chunk_dir.to_str().unwrap());
        test_fixture(&chunk, query_file)
    }

    /// A substrate event must carry `callAddress` exactly once.
    ///
    /// The projection used to list the column twice, so the property was
    /// written twice into every event object. No fixture could catch it: the
    /// comparison parses both sides, and a JSON parser keeps one entry per
    /// key, collapsing the duplicate before any assertion runs.
    ///
    /// This test therefore asserts on the raw response bytes, and does not
    /// depend on any fixture's `query.json` continuing to select the field.
    #[test]
    fn substrate_event_emits_call_address_once() {
        let chunk = ParquetChunk::new("fixtures/moonbeam/chunk");
        let query = br#"{
            "type": "substrate",
            "fields": {"event": {"index": true, "callAddress": true, "name": true}},
            "events": [{}]
        }"#;

        let bytes = execute_query_bytes(&chunk, query).unwrap();

        // The duplicate lives in the bytes, not in the parsed document.
        assert_unique_keys(&bytes, Path::new("substrate_event_emits_call_address_once"));

        // Guard against a vacuous pass: the query must actually have returned
        // events, and each must actually carry the property under test.
        let blocks: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let events: Vec<&serde_json::Value> = blocks
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|block| block.get("events"))
            .flat_map(|events| events.as_array().unwrap())
            .collect();

        assert!(!events.is_empty(), "fixture chunk returned no events");
        for event in &events {
            assert!(
                event.get("callAddress").is_some(),
                "selected property is missing from {event}"
            );
        }
    }
}

#[cfg(feature = "storage")]
mod storage {
    use std::{collections::BTreeMap, fs::File};

    use arrow::{array::RecordBatchReader, datatypes::Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use sqd_data::solana::tables::SolanaChunkBuilder;
    use sqd_dataset::DatasetDescription;
    use sqd_storage::db::{Chunk, Database, DatabaseSettings, DatasetId, DatasetKind};

    use crate::test_fixture;

    fn get_columns_with_stats(d: &DatasetDescription, name: &str, schema: &Schema) -> Vec<usize> {
        if let Some(table_desc) = d.tables.get(name) {
            table_desc
                .options
                .column_options
                .iter()
                .filter_map(|(&name, opts)| {
                    if opts.stats_enable {
                        schema.index_of(name).ok()
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    fn create_dataset(
        db: &Database,
        name: &str,
        kind: &str,
        desc: &DatasetDescription,
        chunk_path: &str
    ) -> anyhow::Result<()> {
        let dataset_id = DatasetId::from_str(name);
        let dataset_kind = DatasetKind::from_str(kind);

        db.create_dataset(dataset_id, dataset_kind)?;

        let mut tables = BTreeMap::new();

        for item_result in std::fs::read_dir(chunk_path)? {
            let item = item_result?.file_name();
            let item_name = item.to_str().unwrap();

            if let Some(table) = item_name.strip_suffix(".parquet") {
                let mut reader =
                    ParquetRecordBatchReaderBuilder::try_new(File::open(format!("{}/{}", chunk_path, item_name))?)?
                        .with_batch_size(500)
                        .build()?;

                let mut builder = db.new_table_builder(reader.schema());

                builder.set_stats(get_columns_with_stats(desc, table, &reader.schema()))?;

                while let Some(record_batch) = reader.next().transpose()? {
                    builder.write_record_batch(&record_batch)?;
                }

                tables.insert(table.to_string(), builder.finish()?);
            }
        }

        db.insert_chunk(
            dataset_id,
            &Chunk::V0 {
                first_block: 0,
                last_block: 0,
                last_block_hash: "hello".to_string(),
                parent_block_hash: "".to_string(),
                tables
            }
        )?;

        Ok(())
    }

    #[test]
    fn test_fixtures() -> anyhow::Result<()> {
        let db_dir = tempfile::tempdir()?;
        let db = DatabaseSettings::default().open(db_dir.path())?;

        create_dataset(
            &db,
            "solana",
            "solana",
            &SolanaChunkBuilder::dataset_description(),
            "fixtures/solana/chunk"
        )?;

        let snapshot = db.snapshot();

        let chunk = snapshot
            .list_chunks(DatasetId::from_str("solana"), 0, None)
            .next()
            .expect("chunk must be present")?;

        let chunk_reader = snapshot.create_chunk_reader(chunk);

        let queries = glob::glob("fixtures/solana/queries/*/query.json")?.collect::<Result<Vec<_>, _>>()?;

        assert!(queries.len() > 0, "no solana queries found");

        for q in queries {
            println!("query: {}", q.to_str().unwrap());
            test_fixture(&chunk_reader, q);
        }

        Ok(())
    }
}
