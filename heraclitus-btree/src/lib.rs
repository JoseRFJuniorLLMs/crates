//! heraclitus-btree — a write-optimized **Bᵋ-tree** ("Fractal Tree") core,
//! milestone **M20.2.0** (see `docs/md/M20_hvm_fractal_gpu.md`).
//!
//! A Bᵋ-tree trades a little read work for a lot less write I/O: instead of
//! writing each update straight to its leaf (a small random write), updates are
//! appended to a **message buffer**; when the buffer fills it is **flushed** to
//! the leaves in one batched, sequential sweep. Reads check the buffer first
//! (the newest truth) and fall through to the range-owning leaf.
//!
//! This is the **in-memory core**: it implements the buffer + batched-flush +
//! range-partitioned-leaves + split mechanic, and a canonical Blake3
//! `state_hash` (path- and buffer-independent — the determinism the spec asks
//! for). The actual I/O amortization lands when this is backed by disk
//! (mmap + atomic flush, blake3 per node) — that is **M20.2.1**, not done here.
//! Nothing here is wired into the engine yet; it is a standalone, tested unit.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::Path;

/// Keys and values are opaque bytes — the same domain as the H-VM's
/// `memory_layers` (`BTreeMap<Vec<u8>, Vec<u8>>`), which this is meant to back.
pub type Key = Vec<u8>;
pub type Val = Vec<u8>;

/// A pending message buffered before it reaches a leaf.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Msg {
    Upsert(Val),
    Delete,
}

/// A write-optimized Bᵋ-tree: a message buffer over range-partitioned leaves.
///
/// Invariant: `leaves.len() == pivots.len() + 1`, `pivots` is sorted ascending,
/// and `pivots[i]` is the smallest key owned by `leaves[i+1]` (so `leaves[0]`
/// owns every key below `pivots[0]`).
pub struct BEpsilonTree {
    buffer: BTreeMap<Key, Msg>,
    pivots: Vec<Key>,
    leaves: Vec<BTreeMap<Key, Val>>,
    buffer_cap: usize,
    leaf_cap: usize,
}

impl Default for BEpsilonTree {
    fn default() -> Self {
        Self::with_caps(1024, 1024)
    }
}

impl BEpsilonTree {
    /// A tree with default fan-out (buffer 1024, leaf 1024).
    pub fn new() -> Self {
        Self::default()
    }

    /// A tree with explicit capacities. `buffer_cap >= 1`, `leaf_cap >= 2`
    /// (a leaf must hold at least two keys to be splittable).
    pub fn with_caps(buffer_cap: usize, leaf_cap: usize) -> Self {
        assert!(buffer_cap >= 1, "buffer_cap must be >= 1");
        assert!(leaf_cap >= 2, "leaf_cap must be >= 2");
        Self {
            buffer: BTreeMap::new(),
            pivots: Vec::new(),
            leaves: vec![BTreeMap::new()],
            buffer_cap,
            leaf_cap,
        }
    }

    /// Insert or replace `key`'s value (buffered; may trigger a flush).
    pub fn upsert(&mut self, key: Key, val: Val) {
        self.push(key, Msg::Upsert(val));
    }

    /// Remove `key` (buffered; may trigger a flush).
    pub fn delete(&mut self, key: &[u8]) {
        self.push(key.to_vec(), Msg::Delete);
    }

    fn push(&mut self, key: Key, msg: Msg) {
        // Newest message per key wins while still buffered.
        self.buffer.insert(key, msg);
        if self.buffer.len() > self.buffer_cap {
            self.flush();
        }
    }

    /// Look up `key`: the buffer (newest) shadows the leaves.
    pub fn get(&self, key: &[u8]) -> Option<Val> {
        if let Some(msg) = self.buffer.get(key) {
            return match msg {
                Msg::Upsert(v) => Some(v.clone()),
                Msg::Delete => None,
            };
        }
        let i = self.leaf_of(key);
        self.leaves[i].get(key).cloned()
    }

    /// Drain the whole buffer into the leaves in one batched sweep. Idempotent
    /// when the buffer is already empty.
    pub fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        // Sorted drain (BTreeMap iterates in key order), so the sweep visits
        // leaves left-to-right — sequential when this is disk-backed.
        let msgs = std::mem::take(&mut self.buffer);
        for (key, msg) in msgs {
            let i = self.leaf_of(&key);
            match msg {
                Msg::Upsert(v) => {
                    self.leaves[i].insert(key, v);
                }
                Msg::Delete => {
                    self.leaves[i].remove(&key);
                }
            }
            if self.leaves[i].len() > self.leaf_cap {
                self.split_leaf(i);
            }
        }
    }

    /// The canonical live key→value map (buffer overlaid on leaves).
    pub fn materialize(&self) -> BTreeMap<Key, Val> {
        let mut m = BTreeMap::new();
        for leaf in &self.leaves {
            for (k, v) in leaf {
                m.insert(k.clone(), v.clone());
            }
        }
        for (k, msg) in &self.buffer {
            match msg {
                Msg::Upsert(v) => {
                    m.insert(k.clone(), v.clone());
                }
                Msg::Delete => {
                    m.remove(k);
                }
            }
        }
        m
    }

    /// Number of live keys.
    pub fn len(&self) -> usize {
        self.materialize().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of leaves (1 until the first split) — exposed for tests/metrics.
    pub fn leaf_count(&self) -> usize {
        self.leaves.len()
    }

    /// Canonical Blake3 hash of the live content. Independent of insertion order
    /// and of whether the buffer has been flushed — the spec's "node_hash
    /// canônico, imune a thread-racing". Two trees with the same logical
    /// key→value set hash identically.
    pub fn state_hash(&self) -> [u8; 32] {
        let mut h = blake3::Hasher::new();
        for (k, v) in self.materialize() {
            h.update(&(k.len() as u64).to_be_bytes());
            h.update(&k);
            h.update(&(v.len() as u64).to_be_bytes());
            h.update(&v);
        }
        *h.finalize().as_bytes()
    }

    /// Index of the leaf owning `key`: the number of pivots `<= key`.
    fn leaf_of(&self, key: &[u8]) -> usize {
        self.pivots.partition_point(|p| p.as_slice() <= key)
    }

    /// Split leaf `i` at its median, inserting a pivot. Single-level: a very
    /// wide pivot array is the structure's growth direction here (a multi-level
    /// internal index is the M20.2.1 refinement).
    fn split_leaf(&mut self, i: usize) {
        let mid = self.leaves[i].len() / 2;
        let split_key = self.leaves[i].keys().nth(mid).unwrap().clone();
        // `split_off` keeps `< split_key` in place and returns `>= split_key`.
        let upper = self.leaves[i].split_off(&split_key);
        self.pivots.insert(i, split_key);
        self.leaves.insert(i + 1, upper);
    }
}

/// Magic header for a Bᵋ-tree on-disk snapshot (M20.2.1).
const SNAPSHOT_MAGIC: &[u8; 4] = b"HBT1";

/// Read one length-prefixed (`u64` BE) byte field, advancing `pos`. Fails closed.
fn read_field(body: &[u8], pos: &mut usize) -> io::Result<Vec<u8>> {
    if *pos + 8 > body.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "btree snapshot truncated (len)"));
    }
    let n = u64::from_be_bytes(body[*pos..*pos + 8].try_into().unwrap()) as usize;
    *pos += 8;
    if *pos + n > body.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "btree snapshot truncated (data)"));
    }
    let s = body[*pos..*pos + n].to_vec();
    *pos += n;
    Ok(s)
}

/// Snapshot persistence (M20.2.1). A *snapshot* of the canonical live content
/// written atomically and integrity-checked with Blake3. This is the durable,
/// crash-safe checkpoint; a fully paged on-disk Bᵋ-tree (mmap, per-node hashing)
/// is a later refinement — the public API here stays the same either way.
impl BEpsilonTree {
    /// Bulk-build a tree from an existing map (e.g. the H-VM's `memory_layers`).
    pub fn from_map(map: BTreeMap<Key, Val>) -> Self {
        let mut t = Self::new();
        for (k, v) in map {
            t.upsert(k, v);
        }
        t
    }

    /// Serialize the canonical live content: magic + count + length-prefixed
    /// `(key, val)` pairs in sorted order + a Blake3 footer over all of the
    /// above. Deterministic — identical content always yields identical bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let map = self.materialize();
        let mut body = Vec::new();
        body.extend_from_slice(SNAPSHOT_MAGIC);
        body.extend_from_slice(&(map.len() as u64).to_be_bytes());
        for (k, v) in &map {
            body.extend_from_slice(&(k.len() as u64).to_be_bytes());
            body.extend_from_slice(k);
            body.extend_from_slice(&(v.len() as u64).to_be_bytes());
            body.extend_from_slice(v);
        }
        let digest = blake3::hash(&body);
        body.extend_from_slice(digest.as_bytes());
        body
    }

    /// Reconstruct from [`to_bytes`] output. Fails closed (`InvalidData`) on a
    /// short buffer, bad magic, or a Blake3 mismatch — corruption is never
    /// silently loaded.
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < SNAPSHOT_MAGIC.len() + 8 + 32 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "btree snapshot too short"));
        }
        let (body, footer) = bytes.split_at(bytes.len() - 32);
        if blake3::hash(body).as_bytes() != footer {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "btree snapshot blake3 mismatch"));
        }
        if &body[0..4] != SNAPSHOT_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "btree snapshot bad magic"));
        }
        let count = u64::from_be_bytes(body[4..12].try_into().unwrap()) as usize;
        let mut pos = 12;
        let mut map = BTreeMap::new();
        for _ in 0..count {
            let k = read_field(body, &mut pos)?;
            let v = read_field(body, &mut pos)?;
            map.insert(k, v);
        }
        Ok(Self::from_map(map))
    }

    /// Atomically persist a snapshot to `path` (write `*.tmp`, fsync, rename) —
    /// crash-safe in the M0 sense: a kill mid-write never corrupts `path`.
    pub fn save(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let path = path.as_ref();
        let tmp = path.with_extension("hbt.tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&self.to_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, path)
    }

    /// Load a snapshot written by [`save`].
    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::from_bytes(&std::fs::read(path)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn buffer_shadows_leaves_and_delete_hides() {
        let mut t = BEpsilonTree::with_caps(2, 2);
        t.upsert(b"a".to_vec(), b"1".to_vec());
        assert_eq!(t.get(b"a"), Some(b"1".to_vec()));
        t.upsert(b"a".to_vec(), b"2".to_vec());
        assert_eq!(t.get(b"a"), Some(b"2".to_vec()), "newest buffered wins");
        t.delete(b"a");
        assert_eq!(t.get(b"a"), None);
        assert!(t.get(b"absent").is_none());
    }

    #[test]
    fn flush_preserves_content() {
        let mut t = BEpsilonTree::with_caps(4, 4);
        for k in 0u8..50 {
            t.upsert(vec![k], vec![k, k]);
        }
        let before = t.state_hash();
        let mat_before = t.materialize();
        t.flush();
        assert_eq!(t.state_hash(), before, "flush must not change content");
        assert_eq!(t.materialize(), mat_before);
        for k in 0u8..50 {
            assert_eq!(t.get(&[k]), Some(vec![k, k]));
        }
    }

    #[test]
    fn splitting_grows_the_tree() {
        let mut t = BEpsilonTree::with_caps(2, 2);
        for k in 0u8..40 {
            t.upsert(vec![k], vec![k]);
        }
        t.flush();
        assert!(t.leaf_count() > 1, "many keys must split into several leaves");
        assert_eq!(t.len(), 40);
    }

    #[test]
    fn state_hash_is_path_independent() {
        // Same logical content built two different ways (order + caps) hashes the
        // same — and survives a flush. Mutating then reverting returns the hash.
        let mut a = BEpsilonTree::with_caps(4, 4);
        let mut b = BEpsilonTree::with_caps(2, 2);
        for k in 0u8..30 {
            a.upsert(vec![k], vec![k]);
        }
        for k in (0u8..30).rev() {
            b.upsert(vec![k], vec![k]);
        }
        b.flush();
        assert_eq!(a.state_hash(), b.state_hash());

        let h = a.state_hash();
        a.upsert(vec![5], vec![99]);
        assert_ne!(a.state_hash(), h);
        a.upsert(vec![5], vec![5]);
        assert_eq!(a.state_hash(), h);
    }

    #[test]
    fn bytes_roundtrip() {
        let mut t = BEpsilonTree::with_caps(4, 4);
        for k in 0u8..40 {
            t.upsert(vec![k], vec![k, k, k]);
        }
        t.delete(&[7]);
        let back = BEpsilonTree::from_bytes(&t.to_bytes()).unwrap();
        assert_eq!(back.state_hash(), t.state_hash());
        assert_eq!(back.materialize(), t.materialize());
    }

    #[test]
    fn save_load_roundtrip_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt.hbt");
        let mut t = BEpsilonTree::new();
        for k in 0u8..100 {
            t.upsert(vec![k], vec![k]);
        }
        t.save(&path).unwrap();
        let loaded = BEpsilonTree::load(&path).unwrap();
        assert_eq!(loaded.state_hash(), t.state_hash());
        assert_eq!(loaded.materialize(), t.materialize());
    }

    #[test]
    fn corruption_fails_closed() {
        let mut t = BEpsilonTree::new();
        t.upsert(b"k".to_vec(), b"v".to_vec());
        let mut bytes = t.to_bytes();
        // Flip a byte → Blake3 mismatch.
        let i = bytes.len() / 2;
        bytes[i] ^= 0xFF;
        assert!(BEpsilonTree::from_bytes(&bytes).is_err());
        // Truncation → too short.
        assert!(BEpsilonTree::from_bytes(&t.to_bytes()[..10]).is_err());
    }

    proptest! {
        /// THE M20.2 GATE: against a reference `BTreeMap`, a Bᵋ-tree driven by an
        /// arbitrary upsert/delete sequence (with tiny caps to force heavy
        /// flushing and splitting) agrees on every key, materializes the same map,
        /// and stays correct after a full flush.
        #[test]
        fn matches_btreemap_reference(
            ops in proptest::collection::vec((0u8..24, proptest::option::of(0u8..6)), 0..600)
        ) {
            let mut tree = BEpsilonTree::with_caps(8, 4);
            let mut reference: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            for (k, v) in &ops {
                let key = vec![*k];
                match v {
                    Some(val) => {
                        tree.upsert(key.clone(), vec![*val]);
                        reference.insert(key, vec![*val]);
                    }
                    None => {
                        tree.delete(&key);
                        reference.remove(&key);
                    }
                }
            }
            for k in 0u8..24 {
                prop_assert_eq!(tree.get(&[k]), reference.get(&vec![k]).cloned());
            }
            prop_assert_eq!(tree.materialize(), reference.clone());

            tree.flush();
            prop_assert_eq!(tree.materialize(), reference);
        }
    }
}
