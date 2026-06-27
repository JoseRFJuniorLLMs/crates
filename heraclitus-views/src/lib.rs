//! heraclitus-views — the replay engine (§3.5).
//!
//! Every index in HeraclitusDB is a [`View`]: derived, asynchronous and
//! rebuildable from LSN 0 by deterministic replay. View application must be
//! deterministic — no wall-clock reads, no unseeded RNG.
//!
//! v0 persistence note (RFC-002): watermarks and checkpoints are stored as
//! plain files under `<data_dir>/views/`. RocksDB-backed checkpoints are a
//! planned optimization; correctness never depends on them, because the
//! recovery story is *always* "rebuild from LSN 0".

use heraclitus_core::{Episode, HeraclitusError, Lsn};
use heraclitus_log::Log;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// A materialized view over the log.
pub trait View: Send + Sync {
    fn name(&self) -> &str;
    /// Apply one event. MUST be deterministic in (lsn, event).
    fn apply(&mut self, lsn: Lsn, event: &Episode);
    /// Highest LSN applied.
    fn watermark(&self) -> Lsn;
    /// Persist derived state (optional; views may be RAM-only).
    fn checkpoint(&self, _dir: &Path) -> Result<(), HeraclitusError> {
        Ok(())
    }
    /// Reset internal state ahead of a rebuild from `lsn`.
    fn reset(&mut self);
}

/// Owns the registered views, their watermarks and the replay loop.
pub struct ViewRegistry {
    dir: PathBuf,
    views: Vec<Box<dyn View>>,
    watermarks: HashMap<String, Lsn>,
}

impl ViewRegistry {
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self, HeraclitusError> {
        let dir = data_dir.into().join("views");
        std::fs::create_dir_all(&dir)?;
        let wm_path = dir.join("watermarks.json");
        let watermarks = match std::fs::read_to_string(&wm_path) {
            Ok(raw) => serde_json::from_str(&raw)
                .map_err(|e| HeraclitusError::Serialization(e.to_string()))?,
            Err(_) => HashMap::new(),
        };
        Ok(Self {
            dir,
            views: Vec::new(),
            watermarks,
        })
    }

    pub fn register(&mut self, view: Box<dyn View>) {
        self.views.push(view);
    }

    pub fn view_names(&self) -> Vec<String> {
        self.views.iter().map(|v| v.name().to_string()).collect()
    }

    /// Apply one live tail event to every view.
    pub fn apply(&mut self, lsn: Lsn, event: &Episode) {
        for v in self.views.iter_mut() {
            v.apply(lsn, event);
            self.watermarks.insert(v.name().to_string(), lsn);
        }
    }

    /// Minimum watermark across views (safe prune point for the memtable).
    pub fn min_watermark(&self) -> Lsn {
        self.views
            .iter()
            .map(|v| self.watermarks.get(v.name()).copied().unwrap_or(0))
            .min()
            .unwrap_or(0)
    }

    /// On startup: replay `(watermark, head]` for each view.
    pub fn catch_up(&mut self, log: &Log) -> Result<u64, HeraclitusError> {
        let from = self
            .views
            .iter()
            .map(|v| self.watermarks.get(v.name()).map(|w| w + 1).unwrap_or(0))
            .min()
            .unwrap_or(0);
        // Paginado: varre o log em janelas de 100k (NÃO materializa milhões de
        // episódios num único Vec — limita o pico de RAM do arranque, que era o
        // `alloc` gigante que estourava em logs grandes).
        let head = log.head();
        let mut applied = 0u64;
        let mut cur = from;
        while cur <= head {
            let batch = log.scan_capped(cur, head + 1, 100_000)?;
            if batch.is_empty() {
                break;
            }
            let last = batch.last().unwrap().0;
            for (lsn, ep) in &batch {
                for v in self.views.iter_mut() {
                    let wm = self.watermarks.get(v.name()).copied();
                    if wm.is_none() || *lsn > wm.unwrap() {
                        v.apply(*lsn, ep);
                        self.watermarks.insert(v.name().to_string(), *lsn);
                        applied += 1;
                    }
                }
            }
            cur = last + 1;
        }
        self.persist_watermarks()?;
        Ok(applied)
    }

    /// `heraclitus-cli view rebuild --view X` — must always work from LSN 0.
    pub fn rebuild(&mut self, log: &Log, view_name: Option<&str>) -> Result<(), HeraclitusError> {
        for v in self.views.iter_mut() {
            if view_name.map(|n| n == v.name()).unwrap_or(true) {
                v.reset();
                self.watermarks.remove(v.name());
            }
        }
        let events = log.scan(0, u64::MAX)?;
        for (lsn, ep) in &events {
            for v in self.views.iter_mut() {
                if view_name.map(|n| n == v.name()).unwrap_or(true) {
                    v.apply(*lsn, ep);
                    self.watermarks.insert(v.name().to_string(), *lsn);
                }
            }
        }
        self.persist_watermarks()?;
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<(), HeraclitusError> {
        for v in &self.views {
            v.checkpoint(&self.dir)?;
        }
        self.persist_watermarks()
    }

    fn persist_watermarks(&self) -> Result<(), HeraclitusError> {
        let raw = serde_json::to_string_pretty(&self.watermarks)
            .map_err(|e| HeraclitusError::Serialization(e.to_string()))?;
        // Audit #8: atomic write — tmp + fsync + rename. A power cut can
        // never leave a half-written watermarks.json behind.
        let tmp = self.dir.join("watermarks.json.tmp");
        {
            use std::io::Write as _;
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(raw.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, self.dir.join("watermarks.json"))?;
        Ok(())
    }

    /// Borrow a registered view for querying.
    pub fn get(&self, name: &str) -> Option<&dyn View> {
        self.views
            .iter()
            .find(|v| v.name() == name)
            .map(|v| v.as_ref())
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut Box<dyn View>> {
        self.views.iter_mut().find(|v| v.name() == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use heraclitus_core::{EventKind, FsyncPolicy};

    use std::sync::{Arc, Mutex};

    /// Toy deterministic view: counts events and folds their LSNs into a
    /// state cell shared with the test.
    struct CountView {
        state: Arc<Mutex<(u64, u64)>>, // (count, fold)
        wm: Lsn,
    }

    impl View for CountView {
        fn name(&self) -> &str {
            "count"
        }
        fn apply(&mut self, lsn: Lsn, _e: &Episode) {
            let mut s = self.state.lock().unwrap();
            s.0 += 1;
            s.1 = s.1.wrapping_mul(31).wrapping_add(lsn);
            self.wm = lsn;
        }
        fn watermark(&self) -> Lsn {
            self.wm
        }
        fn reset(&mut self) {
            *self.state.lock().unwrap() = (0, 0);
            self.wm = 0;
        }
    }

    #[test]
    fn wipe_and_replay_is_deterministic() {
        // M2 acceptance gate: rebuild from LSN 0 yields bit-identical state.
        let dir = tempfile::tempdir().unwrap();
        let log = Log::open(dir.path().join("log"), 1 << 20, FsyncPolicy::Always).unwrap();
        for i in 0..50 {
            log.append(Episode::new(
                "a",
                EventKind::Observation,
                format!("e{i}").into_bytes(),
            ))
            .unwrap();
        }

        let state = Arc::new(Mutex::new((0u64, 0u64)));
        let mut reg = ViewRegistry::open(dir.path()).unwrap();
        reg.register(Box::new(CountView {
            state: state.clone(),
            wm: 0,
        }));
        reg.catch_up(&log).unwrap();
        let first = *state.lock().unwrap();

        reg.rebuild(&log, Some("count")).unwrap();
        let second = *state.lock().unwrap();

        assert_eq!(first.0, 50);
        assert_eq!(first, second, "replay must be deterministic");
    }
}
