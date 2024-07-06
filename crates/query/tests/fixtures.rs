use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use rstest::rstest;

use sqd_query::{JsonArrayWriter, ParquetChunk, Query};


fn execute_query(chunk_path: impl AsRef<Path>, query_file: impl AsRef<Path>) -> anyhow::Result<Vec<u8>> {
    let query = Query::from_json_bytes(
        &std::fs::read(query_file)?
    )?;
    let plan = query.compile();
    let chunk = ParquetChunk::new(chunk_path.as_ref().to_str().unwrap());
    let mut result = plan.execute(&chunk)?;
    let data = Vec::with_capacity(4 * 1024 * 1024);
    let mut writer = JsonArrayWriter::new(data);
    writer.write_blocks(&mut result)?;
    Ok(writer.finish()?)
}


#[rstest]
fn query(#[files("fixtures/*/queries/*/query.json")] query_file: PathBuf) {
    let case_dir = query_file.parent().unwrap();
    let chunk = case_dir.parent().unwrap().parent().unwrap().join("chunk");
    let result_file = case_dir.join("result.json");

    let actual_bytes = execute_query(&chunk, &query_file).unwrap();
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