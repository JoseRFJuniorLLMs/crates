//! heraclitus-core — shared types, IDs, errors and config.
//!
//! Design thesis #1: the log is the truth. Everything in this crate exists to
//! describe what goes *into* the log (`Episode`), what is *derived* from it
//! (`Fact`), and how the rest of the system is configured.

pub mod config;
pub mod error;
pub mod event;
pub mod hlc;
pub mod id;
pub mod vm;

pub use config::{FsyncPolicy, HeraclitusConfig};
pub use error::HeraclitusError;
pub use event::{Episode, EventKind, Fact, ProductPoint};
pub use hlc::Hlc;
pub use id::{EventId, FactId, Lsn, SegmentId};
