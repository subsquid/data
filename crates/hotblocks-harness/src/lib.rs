//! # Hotblocks test harness
//!
//! A black-box test harness built against the **hotblocks behavioral specification** — the
//! implementation-free statement of what the service must do. The `INV-*` / `RP-*` / `CT-*` /
//! `GAP-*` identifiers cited throughout this crate are its vocabulary; the spec documents
//! themselves are maintained separately. The pieces:
//!
//! | Piece | Spec | Module |
//! |---|---|---|
//! | source simulator (scripted chain, fork/finality signals) | 13 §7, DEF-12 | [`sim`] |
//! | reference model (the oracle) | 12 §2 | [`model`] |
//! | client driver + structural validators | 04 §7, 12 §4 | [`driver`] |
//! | quiescence comparator | 12 §1 | [`compare`] |
//! | SUT process supervisor (spawn / crash / restart) | — | [`sut`] |
//! | kind-parametric payloads | DEF-5 | [`chain`] |
//! | glue: script → simulator + model in lockstep | 12 §1 | [`harness`] |
//!
//! The system under test is the real `sqd-hotblocks` binary, driven over HTTP. Nothing here
//! links against its internals — an assertion that cannot be made through the binding is an
//! assertion a client could not rely on either.
//!
//! ## Writing a test
//!
//! Tests live in `crates/hotblocks/tests/`: only a test inside that package gets
//! `env!("CARGO_BIN_EXE_sqd-hotblocks")`, the `bin` path below.
//!
//! ```no_run
//! # use std::{path::Path, sync::Arc};
//! # use sqd_hotblocks_harness::{chain::HlFills, harness::{Harness, HarnessConfig}};
//! # async fn example(bin: &Path) -> anyhow::Result<()> {
//! let mut h = Harness::start(HarnessConfig::from_block(bin, Arc::new(HlFills), 1_000)).await?;
//!
//! h.produce(50)?;             // the source mints 50 blocks; the model EXTENDs
//! h.finalize_with_lag(5)?;    // finality trails the tip
//! h.settle().await?;          // wait for the service to converge
//! h.assert_conforms().await?; // diff every observable against the model
//! # Ok(())
//! # }
//! ```

pub mod chain;
pub mod compare;
pub mod driver;
pub mod harness;
pub mod model;
pub mod sim;
pub mod soak;
pub mod sut;
pub mod types;

/// `P-CONFLICT-WINDOW` (14) — how many ancestors a CONFLICT/ForkSignal payload carries.
pub const P_CONFLICT_WINDOW: u64 = 100;

pub use chain::{Chain, Evm, HlFills, Solana};
pub use compare::Quiescence;
pub use driver::{Client, Emitted, FollowStep, Follower, Outcome};
pub use harness::{Harness, HarnessConfig};
pub use model::{Finalize, ForkResolution, Model, Predicted};
pub use sim::{Numbering, SourceSim};
pub use soak::{ChurnSoakConfig, ChurnSoakReport, SpaceSample, StallProbe, StallReport};
pub use sut::{DatasetSpec, Retention, Sut, SutConfig};
pub use types::{Anchor, Block, BlockNumber, BlockRef};
