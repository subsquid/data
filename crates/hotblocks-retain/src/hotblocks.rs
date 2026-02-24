use reqwest::Client;
use sqd_primitives::BlockNumber;
use url::Url;

pub async fn set_retention(
    client: &Client,
    base_url: &Url,
    dataset: &str,
    from_block: BlockNumber,
) -> anyhow::Result<()> {
    let retention_url = base_url.join(&format!("/datasets/{dataset}/retention"))?;

    client
        .post(retention_url)
        .json(&serde_json::json!({"FromBlock": {"number": from_block}}))
        .send()
        .await?
        .error_for_status()?;

    Ok(())
}
