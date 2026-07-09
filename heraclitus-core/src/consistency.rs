//! SPEC-019 — temporal read consistency levels.
//!
//! Every analytical query runs under one of these snapshot isolation levels.
//! All are lock-free: readers pin an LSN and never see partial background
//! Optimize/Freeze work (that isolation is enforced by the view layer's
//! `Arc`-shared frozen state, not by locks).

use crate::Lsn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// `AS OF SNAPSHOT n` — anchored to a frozen historical manifest.
    HistoricalSnapshot(Lsn),
    /// Fixed at session open; every sub-query sees the same topology.
    RepeatableSnapshot(Lsn),
    /// Union of all `Frozen`/`Archived` segments as of `head`.
    ReadCommittedSnapshot(Lsn),
    /// Frozen state plus a live tail subscription (lowest latency).
    StreamingSnapshot(Lsn),
}

impl IsolationLevel {
    /// The exclusive upper-bound LSN this level reads up to.
    pub fn target_lsn(&self) -> Lsn {
        match *self {
            IsolationLevel::HistoricalSnapshot(l)
            | IsolationLevel::RepeatableSnapshot(l)
            | IsolationLevel::ReadCommittedSnapshot(l)
            | IsolationLevel::StreamingSnapshot(l) => l,
        }
    }

    /// Whether the level couples a live tail (only `StreamingSnapshot`).
    pub fn is_streaming(&self) -> bool {
        matches!(self, IsolationLevel::StreamingSnapshot(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_and_streaming() {
        assert_eq!(IsolationLevel::HistoricalSnapshot(7).target_lsn(), 7);
        assert!(IsolationLevel::StreamingSnapshot(9).is_streaming());
        assert!(!IsolationLevel::RepeatableSnapshot(9).is_streaming());
    }
}
