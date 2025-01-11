use sqd_query::{Chunk, JsonArrayWriter, Query};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};


fn execute_query(chunk: &dyn Chunk, query_file: impl AsRef<Path>) -> anyhow::Result<Vec<u8>> {
    let query = Query::from_json_bytes(
        &std::fs::read(query_file)?
    )?;
    let plan = query.compile();
    let mut result = plan.execute(chunk)?;
    let data = Vec::with_capacity(4 * 1024 * 1024);
    let mut writer = JsonArrayWriter::new(data);
    writer.write_blocks(&mut result)?;
    Ok(writer.finish()?)
}


fn test_fixture(chunk: &dyn Chunk,  query_file: PathBuf) {
    let case_dir = query_file.parent().unwrap();
    let result_file = case_dir.join("result.json");

    let actual_bytes = execute_query(chunk, &query_file).unwrap();
    let actual: serde_json::Value = serde_json::from_slice(&actual_bytes).unwrap();

    let expected: serde_json::Value = match std::fs::read(&result_file) {
        Ok(expected_bytes) => serde_json::from_slice(&expected_bytes).unwrap(),
        Err(err) if err.kind() == ErrorKind::NotFound => {
            serde_json::to_writer_pretty(
                std::fs::File::create(case_dir.join("actual.temp.json")).unwrap(),
                &actual
            ).unwrap();
            return;
        }
        Err(err) => panic!("{:?}", err)
    };

    if expected != actual {
        serde_json::to_writer_pretty(
            std::fs::File::create(case_dir.join("actual.temp.json")).unwrap(),
            &actual
        ).unwrap();
        panic!("actual != expected")
    }
}


#[cfg(feature = "parquet")]
mod parquet {
    use std::path::PathBuf;

    use rstest::rstest;

    use sqd_query::ParquetChunk;

    use crate::test_fixture;


    #[rstest]
    fn query(#[files("fixtures/*/queries/*/query.json")] query_file: PathBuf) {
        let case_dir = query_file.parent().unwrap();
        let chunk_dir = case_dir.parent().unwrap().parent().unwrap().join("chunk");
        let chunk = ParquetChunk::new(chunk_dir.to_str().unwrap());
        test_fixture(&chunk, query_file)
    }
}


#[cfg(feature = "storage")]
mod storage {
    use crate::test_fixture;
    use arrow::array::RecordBatchReader;
    use arrow::datatypes::Schema;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use sqd_data::solana::tables::SolanaChunkBuilder;
    use sqd_dataset::DatasetDescription;
    use sqd_storage::db::{Chunk, Database, DatabaseSettings, DatasetId, DatasetKind};
    use std::collections::BTreeMap;
    use std::fs::File;


    fn get_columns_with_stats(d: &DatasetDescription, name: &str, schema: &Schema) -> Vec<usize> {
        if let Some(table_desc) = d.tables.get(name) {
            table_desc.options.column_options.iter()
                .filter_map(|(&name, opts)| {
                    opts.stats_enable.then(|| {
                        schema.index_of(name).unwrap()
                    })
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
    ) -> anyhow::Result<()>
    {
        let dataset_id = DatasetId::from_str(name);
        let dataset_kind = DatasetKind::from_str(kind);

        db.create_dataset(dataset_id, dataset_kind)?;

        let mut tables = BTreeMap::new();

        for item_result in std::fs::read_dir(chunk_path)? {
            let item = item_result?.file_name();
            let item_name = item.to_str().unwrap();

            if let Some(table) = item_name.strip_suffix(".parquet") {
                let mut reader = ParquetRecordBatchReaderBuilder::try_new(
                    File::open(format!("{}/{}", chunk_path, item_name))?
                )?.with_batch_size(500).build()?;

                let mut builder = db.new_table_builder(reader.schema());
                
                builder.set_stats(
                    get_columns_with_stats(
                        desc,
                        table,
                        &reader.schema()
                    )
                )?;

                while let Some(record_batch) = reader.next().transpose()? {
                    builder.write_record_batch(&record_batch)?;
                }

                tables.insert(table.to_string(), builder.finish()?);
            }
        }

        db.insert_chunk(dataset_id, &Chunk::V0 {
            first_block: 0,
            last_block: 0,
            last_block_hash: "hello".to_string(),
            parent_block_hash: "".to_string(),
            tables
        })?;

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

        let queries = glob::glob("fixtures/solana/queries/*/query.json")?
            .collect::<Result<Vec<_>, _>>()?;

        assert!(queries.len() > 0, "no solana queries found");

        for q in queries {
            println!("query: {}", q.to_str().unwrap());
            test_fixture(&chunk_reader, q);
        }

        Ok(())
    }
}