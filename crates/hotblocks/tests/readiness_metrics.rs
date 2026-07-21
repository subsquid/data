//! `/ready` is a rotation gate for the orchestrator, not part of the served API. Its 503s are
//! deliberate, so they must not reach `http_status` -- `middleware` classifies every unlabelled
//! 5xx as `error_class="UNCLASSIFIED"`, which is what error-rate alerting reads, and a kubelet
//! probing through the whole grace window would forge a burst of faults on every termination.

use std::time::Duration;

use anyhow::{Result, ensure};
use reqwest::StatusCode;
use sqd_hotblocks_harness::sut::{Sut, SutConfig};

/// Long enough that the whole assertion runs inside the grace window.
const PRE_DRAIN_GRACE: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread")]
async fn readiness_probes_are_not_counted_as_http_traffic() -> Result<()> {
    let mut sut = Sut::start(SutConfig {
        bin: env!("CARGO_BIN_EXE_sqd-hotblocks").into(),
        datasets: vec![],
        args: vec![
            "--pre-drain-grace-secs".to_owned(),
            PRE_DRAIN_GRACE.as_secs().to_string(),
        ],
        rust_log: "error".to_owned(),
        startup_timeout: Duration::from_secs(30)
    })
    .await?;

    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()?;
    let ready_url = format!("{}/ready", sut.base_url());
    let metrics_url = format!("{}/metrics", sut.base_url());

    assert_eq!(http.get(&ready_url).send().await?.status(), StatusCode::OK);

    sut.signal_shutdown()?;
    await_unready(&http, &ready_url).await?;

    // Probe a few more times: inside the middleware each of these would add to the counter.
    for _ in 0..4 {
        assert_eq!(
            http.get(&ready_url).send().await?.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    // Serving is unaffected during the grace window -- that is the point of the phase.
    let metrics = http.get(&metrics_url).send().await?.text().await?;
    let counted: Vec<&str> = metrics
        .lines()
        .filter(|line| line.starts_with("hotblocks_http_status_total"))
        .filter(|line| line.contains(r#"status="503""#) || line.contains("error_class"))
        .collect();

    assert!(
        counted.is_empty(),
        "readiness probes leaked into the HTTP error metrics:\n{}",
        counted.join("\n")
    );

    Ok(())
}

async fn await_unready(http: &reqwest::Client, url: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if let Ok(response) = http.get(url).send().await
            && response.status() == StatusCode::SERVICE_UNAVAILABLE
        {
            return Ok(());
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "SIGTERM did not withdraw {url} from rotation"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
