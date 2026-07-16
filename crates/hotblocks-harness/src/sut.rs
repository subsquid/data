//! The system under test — the real `sqd-hotblocks` binary, driven as a black box.
//!
//! Owning the process, not a library handle, is what makes the crash-recovery (CT-2) and
//! shutdown (GAP-17) classes expressible at all.

use std::{
    fs::{File, OpenOptions},
    io::Write,
    mem::MaybeUninit,
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::{Duration, Instant}
};

use anyhow::{Context, Result, bail, ensure};
use tempfile::TempDir;
use tokio::process::{Child, Command};

const TARGET_NOFILE_LIMIT: libc::rlim_t = 8_192;
const MIN_NOFILE_LIMIT: libc::rlim_t = 1_024;

/// The dataset-config retention strategies (see `hotblocks/src/dataset_config.rs`).
#[derive(Clone, Debug)]
pub enum Retention {
    /// Ingest starts exactly here — the deterministic choice for structural tests.
    FromBlock { number: u64, parent_hash: Option<String> },
    /// A moving window of `n` blocks; the start position is probed from the source's head.
    Head(u64),
    /// Driven at runtime via `POST /datasets/{id}/retention` (External, DEF-9). The dataset
    /// ingests nothing until the first instruction arrives.
    Api,
    /// Never trim. On an empty database this dataset never ingests anything at all.
    None
}

#[derive(Clone, Debug)]
pub struct DatasetSpec {
    pub id: String,
    pub kind: String,
    pub retention: Retention,
    /// Preserve response-aligned chunks for tests that exercise physical layout boundaries.
    pub disable_compaction: bool,
    pub sources: Vec<String>
}

#[derive(Clone, Debug)]
pub struct SutConfig {
    /// Path to the built binary — tests get it from `env!("CARGO_BIN_EXE_sqd-hotblocks")`.
    pub bin: PathBuf,
    pub datasets: Vec<DatasetSpec>,
    pub args: Vec<String>,
    pub rust_log: String,
    /// Generous by design: serving is gated on full initialization (GAP-7). A test that cares
    /// measures [`Sut::last_startup`] instead.
    pub startup_timeout: Duration
}

pub struct Sut {
    cfg: SutConfig,
    dir: TempDir,
    port: u16,
    child: Option<Child>,
    /// How long the last boot took to accept connections (SLI-5).
    pub last_startup: Duration
}

#[derive(Debug)]
pub struct ShutdownReport {
    pub status: ExitStatus,
    pub took: Duration
}

impl Sut {
    /// Write the config, start the process, wait until it accepts connections.
    pub async fn start(cfg: SutConfig) -> Result<Self> {
        let dir = TempDir::new().context("failed to create the SUT working directory")?;
        let mut sut = Self {
            port: free_port()?,
            cfg,
            dir,
            child: None,
            last_startup: Duration::ZERO
        };
        sut.write_config()?;
        sut.boot().await?;
        Ok(sut)
    }

    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub fn db_dir(&self) -> PathBuf {
        self.dir.path().join("db")
    }

    pub fn log_path(&self) -> PathBuf {
        self.dir.path().join("sut.log")
    }

    /// Boot a stopped process again against the same database — the CT-2 restart primitive.
    pub async fn restart(&mut self) -> Result<()> {
        if self.child.is_some() {
            self.stop().await?;
        }
        self.boot().await
    }

    /// SIGKILL: the process loses everything not committed (CN-6, `P-DUR-PROCESS = 0`).
    pub async fn crash(&mut self) -> Result<()> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        child.kill().await.context("failed to kill the SUT")?;
        self.child = None;
        Ok(())
    }

    /// SIGTERM and wait for the drain-and-exit (LIV-12, `P-SHUTDOWN`).
    pub async fn stop(&mut self) -> Result<ShutdownReport> {
        let Some(mut child) = self.child.take() else {
            bail!("the SUT is not running")
        };
        let pid = child.id().context("the SUT has no pid")? as i32;
        let started = Instant::now();

        // SAFETY: `pid` names a child of this process that has not been reaped.
        unsafe { libc::kill(pid, libc::SIGTERM) };

        match tokio::time::timeout(Duration::from_secs(60), child.wait()).await {
            Ok(status) => Ok(ShutdownReport {
                status: status.context("failed to wait for the SUT")?,
                took: started.elapsed()
            }),
            Err(_) => {
                child.kill().await.ok();
                bail!("the SUT did not exit within 60s of SIGTERM")
            }
        }
    }

    /// The tail of the service log, minus the HTTP access log — the harness's own polling makes
    /// that the loudest thing in there and never the interesting one.
    pub fn log_tail(&self, lines: usize) -> String {
        let Ok(log) = std::fs::read_to_string(self.log_path()) else {
            return "<no log>".to_string();
        };
        let all: Vec<&str> = log
            .lines()
            .filter(|line| !line.contains(r#""target":"http_request""#))
            .collect();
        all[all.len().saturating_sub(lines)..].join("\n")
    }

    async fn boot(&mut self) -> Result<()> {
        let started = Instant::now();
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_path())
            .context("failed to open the SUT log")?;

        let mut command = Command::new(&self.cfg.bin);
        command
            .arg("--datasets")
            .arg(self.config_path())
            .arg("--db")
            .arg(self.db_dir())
            .arg("--port")
            .arg(self.port.to_string())
            .args(&self.cfg.args)
            .env("RUST_LOG", &self.cfg.rust_log)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .kill_on_drop(true);
        configure_nofile_limit(&mut command)?;

        let child = command
            .spawn()
            .with_context(|| format!("failed to spawn {}", self.cfg.bin.display()))?;

        self.child = Some(child);
        self.await_ready(started).await?;
        self.last_startup = started.elapsed();
        Ok(())
    }

    async fn await_ready(&mut self, started: Instant) -> Result<()> {
        let http = reqwest::Client::builder().timeout(Duration::from_secs(2)).build()?;
        let deadline = started + self.cfg.startup_timeout;
        loop {
            if let Some(status) = self.child.as_mut().expect("just spawned").try_wait()? {
                bail!(
                    "the SUT exited during startup with {status}\n--- log tail ---\n{}",
                    self.log_tail(60)
                );
            }
            if let Ok(res) = http.get(self.base_url()).send().await
                && res.status().is_success()
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                bail!(
                    "the SUT did not accept connections within {:?} (LIV-5a)\n--- log tail ---\n{}",
                    self.cfg.startup_timeout,
                    self.log_tail(60)
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn config_path(&self) -> PathBuf {
        self.dir.path().join("datasets.yaml")
    }

    /// Renders `BTreeMap<DatasetId, DatasetConfig>` as the service's YAML loader expects it
    /// (`serde_yaml` with `singleton_map_recursive`; see `hotblocks/src/dataset_config.rs`).
    fn write_config(&self) -> Result<()> {
        let mut yaml = String::new();
        for ds in &self.cfg.datasets {
            yaml.push_str(&format!("{}:\n", ds.id));
            yaml.push_str(&format!("  kind: {}\n", ds.kind));
            match &ds.retention {
                Retention::FromBlock { number, parent_hash } => {
                    yaml.push_str("  retention_strategy:\n    FromBlock:\n");
                    yaml.push_str(&format!("      number: {number}\n"));
                    match parent_hash {
                        Some(hash) => yaml.push_str(&format!("      parent_hash: \"{hash}\"\n")),
                        None => yaml.push_str("      parent_hash: null\n")
                    }
                }
                Retention::Head(n) => yaml.push_str(&format!("  retention_strategy:\n    Head: {n}\n")),
                Retention::Api => yaml.push_str("  retention_strategy: Api\n"),
                Retention::None => yaml.push_str("  retention_strategy: None\n")
            }
            yaml.push_str(&format!("  disable_compaction: {}\n", ds.disable_compaction));
            yaml.push_str("  data_sources:\n");
            for src in &ds.sources {
                yaml.push_str(&format!("    - \"{src}\"\n"));
            }
        }
        File::create(self.config_path())?
            .write_all(yaml.as_bytes())
            .context("failed to write the dataset config")
    }
}

/// Raise the SUT's soft descriptor limit without changing the test runner or the hard limit.
/// EVM chunk processors keep one temporary file per physical Arrow buffer, so the macOS default
/// of 256 descriptors is insufficient even for a single small chunk.
fn configure_nofile_limit(command: &mut Command) -> Result<()> {
    let mut limits = MaybeUninit::<libc::rlimit>::uninit();
    // SAFETY: `limits` points to writable storage for one `rlimit`, and `getrlimit` initializes
    // it on success before `assume_init` is called.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limits.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error()).context("failed to read RLIMIT_NOFILE");
    }
    // SAFETY: the successful `getrlimit` call above initialized `limits`.
    let limits = unsafe { limits.assume_init() };
    let child_soft_limit = limits.rlim_max.min(TARGET_NOFILE_LIMIT);
    ensure!(
        child_soft_limit >= MIN_NOFILE_LIMIT,
        "the SUT needs RLIMIT_NOFILE >= {MIN_NOFILE_LIMIT}, but the hard limit is {}",
        limits.rlim_max
    );

    // SAFETY: the closure only calls the async-signal-safe `setrlimit` syscall between `fork`
    // and `exec`, does not access shared state, and preserves the inherited hard limit.
    unsafe {
        command.as_std_mut().pre_exec(move || {
            let child_limits = libc::rlimit {
                rlim_cur: child_soft_limit,
                rlim_max: limits.rlim_max
            };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &child_limits) == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
    }
    Ok(())
}

impl Drop for Sut {
    fn drop(&mut self) {
        // The child dies with the handle (`kill_on_drop`); a failing test needs the log.
        if std::thread::panicking() {
            eprintln!(
                "\n--- SUT log tail ({}) ---\n{}\n",
                self.log_path().display(),
                self.log_tail(80)
            );
        }
    }
}

/// Ask the kernel for a free port and hand it to the child. The gap before the child binds it is
/// a race in principle, not in practice.
fn free_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").context("no free port")?;
    Ok(listener.local_addr()?.port())
}

impl SutConfig {
    pub fn new(bin: impl AsRef<Path>, datasets: Vec<DatasetSpec>) -> Self {
        Self {
            bin: bin.as_ref().to_path_buf(),
            datasets,
            args: vec![
                "--data-cache-size".into(),
                "16".into(),
                // Direct I/O differs across the platforms tests run on and nothing structural
                // depends on it. CT-6/CT-7 must drop this flag — they measure the engine.
                "--rocksdb-disable-direct-io".into(),
            ],
            rust_log: "info".into(),
            startup_timeout: Duration::from_secs(60)
        }
    }
}
