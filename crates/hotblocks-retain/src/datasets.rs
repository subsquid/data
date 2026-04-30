use crate::types::DatasetId;
use serde::Deserialize;
use std::collections::HashMap;
use url::Url;

#[derive(Deserialize)]
struct Dataset {
    name: String,
    id: DatasetId,
}

#[derive(Deserialize)]
struct DatasetsFile {
    #[serde(rename = "sqd-network-datasets")]
    sqd_network_datasets: Vec<Dataset>,
}

/// Downloads the datasets manifest and returns a map from dataset name to dataset ID.
pub async fn get_name_to_id(
    client: &reqwest::Client,
    url: &Url,
) -> anyhow::Result<HashMap<DatasetId, String>> {
    let bytes = client
        .get(url.as_str())
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    let file: DatasetsFile = serde_yaml::from_slice(&bytes)?;

    let map = file
        .sqd_network_datasets
        .into_iter()
        .map(|d| (d.name, d.id))
        .collect();

    Ok(map)
}
