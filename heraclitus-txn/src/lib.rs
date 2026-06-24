//! heraclitus-txn — MVCC snapshots over log offsets (§3.11).
//!
//! A snapshot *is* an LSN. All reads carry it; views answer "as of ≤ LSN"
//! via their watermark + memtable merge. Writes are single-writer appends;
//! `compare_and_append` provides optimistic CAS workflows. There are NO
//! interactive multi-statement write transactions in v0.x — documented
//! honestly in docs/CONSISTENCY.md.

use heraclitus_core::{Episode, HeraclitusError, Lsn};
use heraclitus_log::Log;
use std::sync::Arc;

/// A consistent read point: every event with `lsn < self.lsn()` is inside
/// the snapshot; everything at or after the captured head is invisible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot(Lsn);

impl Snapshot {
    /// The head LSN captured at snapshot time (exclusive upper bound).
    pub fn lsn(&self) -> Lsn {
        self.0
    }

    pub fn contains(&self, lsn: Lsn) -> bool {
        lsn < self.0
    }
}

pub struct TxnManager {
    log: Arc<Log>,
}

impl TxnManager {
    pub fn new(log: Arc<Log>) -> Self {
        Self { log }
    }

    /// `begin_snapshot() -> Lsn` (§3.11): captures the current head. The
    /// snapshot sees exactly the events with `lsn < head`.
    pub fn begin_snapshot(&self) -> Snapshot {
        Snapshot(self.log.head())
    }

    /// Read everything visible in `snap` within `[from, to)`.
    pub fn read_at(
        &self,
        snap: Snapshot,
        from: Lsn,
        to: Lsn,
    ) -> Result<Vec<(Lsn, Episode)>, HeraclitusError> {
        self.log.scan(from, to.min(snap.lsn()))
    }

    /// Plain append (the log serializes writers).
    pub fn append(&self, episode: Episode) -> Result<Lsn, HeraclitusError> {
        self.log.append(episode)
    }

    /// Optimistic compare-and-append: succeeds only if the head still equals
    /// `expected` (typically `snapshot.lsn()`), i.e. nothing was written
    /// since the snapshot was taken.
    pub fn compare_and_append(
        &self,
        expected: Lsn,
        episode: Episode,
    ) -> Result<Lsn, HeraclitusError> {
        self.log.append_cas(expected, episode)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use heraclitus_core::{EventKind, FsyncPolicy};

    fn ep(s: &str) -> Episode {
        Episode::new("txn", EventKind::Observation, s.into())
    }

    fn open() -> (tempfile::TempDir, TxnManager) {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(Log::open(dir.path(), 1 << 20, FsyncPolicy::Always).unwrap());
        (dir, TxnManager::new(log))
    }

    #[test]
    fn snapshot_isolation() {
        let (_d, txn) = open();
        for i in 0..5 {
            txn.append(ep(&format!("e{i}"))).unwrap();
        }
        let snap = txn.begin_snapshot();
        // Writes after the snapshot are invisible to it.
        txn.append(ep("after")).unwrap();
        txn.append(ep("after2")).unwrap();

        let visible = txn.read_at(snap, 0, u64::MAX).unwrap();
        assert_eq!(visible.len(), 5);
        assert!(visible.iter().all(|(l, _)| snap.contains(*l)));

        // A fresh snapshot sees everything.
        let snap2 = txn.begin_snapshot();
        assert_eq!(txn.read_at(snap2, 0, u64::MAX).unwrap().len(), 7);
    }

    #[test]
    fn cas_workflow() {
        let (_d, txn) = open();
        let snap = txn.begin_snapshot();
        // First CAS at the snapshot head succeeds…
        txn.compare_and_append(snap.lsn(), ep("mine")).unwrap();
        // …a second writer using the same stale snapshot must conflict.
        let err = txn.compare_and_append(snap.lsn(), ep("stale")).unwrap_err();
        assert!(matches!(err, HeraclitusError::CasConflict { .. }));
    }
}
