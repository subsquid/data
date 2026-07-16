//! The instance header is a cross-repo contract: the portal reads it to attribute a response
//! to a replica, and strips every `x-internal-*` header before answering a client. Nothing on
//! the portal side can tell that hotblocks stopped sending it — the field just reads "-".

use std::time::Duration;

use anyhow::Result;
use sqd_hotblocks_harness::sut::{Sut, SutConfig};

#[tokio::test(flavor = "multi_thread")]
async fn every_response_names_the_serving_instance() -> Result<()> {
    let sut = Sut::start(SutConfig {
        bin: env!("CARGO_BIN_EXE_sqd-hotblocks").into(),
        datasets: vec![],
        args: vec![],
        rust_log: "error".to_owned(),
        startup_timeout: Duration::from_secs(30)
    })
    .await?;

    // `/` needs no dataset: the header comes from the middleware, so any route proves it.
    let response = reqwest::get(sut.base_url()).await?;

    let instance = response
        .headers()
        .get("x-internal-hotblocks-instance")
        .expect("every response must name its instance");

    // HOSTNAME is the pod name under Kubernetes; a dev machine may not set it at all, and the
    // fallback is what keeps the header present rather than absent.
    assert!(
        !instance.is_empty(),
        "the instance header must never be empty: the portal cannot distinguish it from absent"
    );

    Ok(())
}
