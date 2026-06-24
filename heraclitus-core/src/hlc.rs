use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Hybrid logical clock. Layout: physical millis << 16 | logical counter.
///
/// Guarantees strict monotonicity within a process even when the wall clock
/// stalls or steps backwards. Distributed merging (`observe`) takes the max.
#[derive(Debug, Default)]
pub struct Hlc {
    last: AtomicU64,
}

impl Hlc {
    pub fn new() -> Self {
        Self::default()
    }

    fn physical_now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Produce the next timestamp, strictly greater than any previous one.
    pub fn now(&self) -> u64 {
        let phys = Self::physical_now() << 16;
        loop {
            let last = self.last.load(Ordering::SeqCst);
            let next = if phys > last { phys } else { last + 1 };
            if self
                .last
                .compare_exchange(last, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return next;
            }
        }
    }

    /// Merge a timestamp observed from a remote peer.
    pub fn observe(&self, remote: u64) {
        self.last.fetch_max(remote, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic() {
        let hlc = Hlc::new();
        let mut prev = 0;
        for _ in 0..10_000 {
            let t = hlc.now();
            assert!(t > prev);
            prev = t;
        }
    }

    #[test]
    fn observe_advances() {
        let hlc = Hlc::new();
        let far_future = (Hlc::physical_now() + 1_000_000) << 16;
        hlc.observe(far_future);
        assert!(hlc.now() > far_future);
    }
}
