use serde::Deserialize;
use sqd_primitives::BlockNumber;

#[derive(Debug, Deserialize)]
pub struct SchedulingStatus {
    pub datasets: Vec<DatasetStatus>,
}

#[derive(Debug, Deserialize)]
pub struct DatasetStatus {
    pub id: String,
    pub height: Option<BlockNumber>,
}

pub async fn get_status(client: &reqwest::Client, url: &str) -> anyhow::Result<SchedulingStatus> {
    let status = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json::<SchedulingStatus>()
        .await?;
    Ok(status)
}
