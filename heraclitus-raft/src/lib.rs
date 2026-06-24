//! heraclitus-raft — replication (§3.13).
//!
//! v0 (RFC-003): **single-leader log shipping with anti-entropy catch-up.**
//! The log *is* the state machine input; followers pull batches from the
//! leader's head and replay them into their own log (preserving LSN + HLC),
//! and their views replay locally. Full openraft consensus (leader election,
//! quorum commit) is the planned upgrade behind the `replication` feature;
//! until the turmoil leader-kill suite is green we do NOT claim automatic
//! failover — we claim, and test, that a partitioned follower converges to
//! every leader-acked event after healing, losing nothing.

use heraclitus_core::{Episode, HeraclitusError, Lsn};
use heraclitus_log::Log;
use std::sync::Arc;

/// Transport boundary: how a follower fetches batches from a leader.
/// Implementations: in-process (tests), TCP (sim/turmoil), gRPC Subscribe.
pub trait LogTransport {
    fn fetch(&mut self, from: Lsn, max: usize) -> Result<Vec<(Lsn, Episode)>, HeraclitusError>;
}

/// In-process transport over a shared leader log (reference + tests).
pub struct LocalTransport {
    pub leader: Arc<Log>,
}

impl LogTransport for LocalTransport {
    fn fetch(&mut self, from: Lsn, max: usize) -> Result<Vec<(Lsn, Episode)>, HeraclitusError> {
        let mut batch = self.leader.scan(from, from.saturating_add(max as u64))?;
        batch.truncate(max);
        Ok(batch)
    }
}

/// A pull-based follower. `sync_once` is idempotent and safe to call in a
/// loop; contiguity is enforced by `append_replicated`.
pub struct Follower {
    pub log: Arc<Log>,
    pub batch: usize,
}

impl Follower {
    pub fn new(log: Arc<Log>) -> Self {
        Self { log, batch: 256 }
    }

    /// Pull until the transport has nothing newer. Returns events applied.
    pub fn sync_once(&self, transport: &mut dyn LogTransport) -> Result<u64, HeraclitusError> {
        let mut applied = 0u64;
        loop {
            let from = self.log.head();
            let batch = transport.fetch(from, self.batch)?;
            if batch.is_empty() {
                return Ok(applied);
            }
            let mut progressed = 0u64;
            for (lsn, ep) in batch {
                if lsn < self.log.head() {
                    continue; // duplicate delivery — idempotent skip
                }
                self.log.append_replicated(lsn, ep)?;
                applied += 1;
                progressed += 1;
            }
            // Audit #3: a non-empty batch that applied nothing means the
            // transport is replaying stale data — exit instead of spinning.
            if progressed == 0 {
                return Ok(applied);
            }
        }
    }
}

/// Compare two logs for byte-level payload equivalence over `[0, head)`.
/// Used by the sim suite to prove zero acked-event loss after healing.
pub fn logs_equivalent(a: &Log, b: &Log) -> Result<bool, HeraclitusError> {
    let (ea, eb) = (a.scan(0, u64::MAX)?, b.scan(0, u64::MAX)?);
    if ea.len() != eb.len() {
        return Ok(false);
    }
    Ok(ea.iter().zip(&eb).all(|((la, xa), (lb, xb))| {
        la == lb && xa.id == xb.id && xa.ts_hlc == xb.ts_hlc && xa.content == xb.content
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use heraclitus_core::{EventKind, FsyncPolicy};

    fn ep(s: &str) -> Episode {
        Episode::new("leader", EventKind::Observation, s.into())
    }

    #[test]
    fn follower_replicates_and_converges() {
        let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let leader = Arc::new(Log::open(d1.path(), 1 << 20, FsyncPolicy::Always).unwrap());
        let follower_log = Arc::new(Log::open(d2.path(), 1 << 20, FsyncPolicy::Always).unwrap());

        for i in 0..50 {
            leader.append(ep(&format!("e{i}"))).unwrap();
        }
        let follower = Follower::new(follower_log.clone());
        let mut t = LocalTransport {
            leader: leader.clone(),
        };
        assert_eq!(follower.sync_once(&mut t).unwrap(), 50);

        // More writes land while the follower is "away"; it catches up.
        for i in 50..80 {
            leader.append(ep(&format!("e{i}"))).unwrap();
        }
        assert_eq!(follower.sync_once(&mut t).unwrap(), 30);
        assert!(logs_equivalent(&leader, &follower_log).unwrap());
        // HLC stamps preserved bit-for-bit (append_replicated does not re-stamp).
        let (la, lb) = (
            leader.scan(0, u64::MAX).unwrap(),
            follower_log.scan(0, u64::MAX).unwrap(),
        );
        assert_eq!(la[7].1.ts_hlc, lb[7].1.ts_hlc);
    }

    #[test]
    fn duplicate_delivery_is_idempotent() {
        let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let leader = Arc::new(Log::open(d1.path(), 1 << 20, FsyncPolicy::Always).unwrap());
        let flog = Arc::new(Log::open(d2.path(), 1 << 20, FsyncPolicy::Always).unwrap());
        for i in 0..10 {
            leader.append(ep(&format!("e{i}"))).unwrap();
        }
        /// A transport that maliciously re-delivers from LSN 0 every time.
        struct Dup(Arc<Log>, bool);
        impl LogTransport for Dup {
            fn fetch(
                &mut self,
                from: Lsn,
                max: usize,
            ) -> Result<Vec<(Lsn, Episode)>, HeraclitusError> {
                if !self.1 {
                    self.1 = true;
                    return self.0.scan(0, max as u64); // duplicates!
                }
                self.0.scan(from, from + max as u64)
            }
        }
        let follower = Follower::new(flog.clone());
        follower.sync_once(&mut Dup(leader.clone(), false)).unwrap();
        follower.sync_once(&mut Dup(leader.clone(), false)).unwrap();
        assert!(logs_equivalent(&leader, &flog).unwrap());
    }
}
