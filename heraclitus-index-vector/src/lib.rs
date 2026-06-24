//! heraclitus-index-vector — in-crate HNSW (§3.6).
//!
//! We deliberately do NOT depend on an external HNSW crate: the metric is a
//! custom product-manifold distance and we need RoaringBitmap filter
//! push-down. The index is derived state: losing it means replay, not data
//! loss.

use heraclitus_core::{Episode, EventId, Lsn, ProductPoint};
use heraclitus_manifold::ProductMetric;
use heraclitus_views::View;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use roaring::RoaringBitmap;
use std::collections::{BinaryHeap, HashMap, HashSet};

const DEFAULT_M: usize = 16;
const DEFAULT_EF_CONSTRUCTION: usize = 200;

#[derive(Clone)]
struct Node {
    point: ProductPoint,
    level: usize,
    /// neighbors[level] = ids
    neighbors: Vec<Vec<u32>>,
}

/// Min-ordering helper for the search heaps.
#[derive(PartialEq)]
struct Candidate {
    dist: f64,
    id: u32,
}
impl Eq for Candidate {}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.dist.total_cmp(&self.dist) // reversed: BinaryHeap becomes min-heap
    }
}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct VectorIndex {
    metric: ProductMetric,
    m: usize,
    ef_construction: usize,
    nodes: Vec<Node>,
    entry: Option<u32>,
    by_event: HashMap<EventId, u32>,
    ids: Vec<EventId>,
    lsns: Vec<Lsn>,
    watermark: Lsn,
    rng: StdRng,
}

#[derive(Debug, Clone)]
pub struct VectorHit {
    pub id: EventId,
    pub lsn: Lsn,
    pub dist: f32,
}

impl VectorIndex {
    pub fn new(metric: ProductMetric) -> Self {
        Self {
            metric,
            m: DEFAULT_M,
            ef_construction: DEFAULT_EF_CONSTRUCTION,
            nodes: Vec::new(),
            entry: None,
            by_event: HashMap::new(),
            ids: Vec::new(),
            lsns: Vec::new(),
            watermark: 0,
            // Determinism requirement (§3.5): RNG seeded from a constant; the
            // level sequence is then a pure function of insertion order.
            rng: StdRng::seed_from_u64(0x48524B4C),
        }
    }

    fn dist(&self, a: u32, b: &ProductPoint) -> f64 {
        self.metric.dist(&self.nodes[a as usize].point, b)
    }

    fn random_level(&mut self) -> usize {
        let ml = 1.0 / (self.m as f64).ln();
        let r: f64 = self.rng.gen_range(f64::MIN_POSITIVE..1.0);
        ((-r.ln()) * ml).floor() as usize
    }

    /// Greedy search at one level, returning up to `ef` nearest candidates.
    ///
    /// Filter push-down (audit02 #3): traversal explores the whole reachable
    /// graph for connectivity, but only `filter`-passing nodes are admitted to
    /// `results`. So a selective filter keeps expanding until it has `ef`
    /// filtered hits (or the reachable set is exhausted) instead of returning
    /// fewer than `k` after a post-hoc filter. `filter = None` is identical to
    /// the unfiltered behavior.
    fn search_layer(
        &self,
        query: &ProductPoint,
        entry: u32,
        level: usize,
        ef: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Vec<Candidate> {
        let passes = |id: u32| filter.map(|f| f.contains(id)).unwrap_or(true);
        let mut visited: HashSet<u32> = HashSet::from([entry]);
        let d0 = self.dist(entry, query);
        // `candidates` drives traversal over every reachable node; `results`
        // keeps only filter-passing nodes (the ones we may return).
        let mut candidates = BinaryHeap::from([Candidate { dist: d0, id: entry }]);
        let mut results: Vec<Candidate> = Vec::new();
        if passes(entry) {
            results.push(Candidate { dist: d0, id: entry });
        }

        while let Some(c) = candidates.pop() {
            let worst = results.iter().map(|r| r.dist).fold(f64::MIN, f64::max);
            // Stop only once we have ef filtered hits AND cannot improve them.
            if results.len() >= ef && c.dist > worst {
                break;
            }
            for &n in &self.nodes[c.id as usize].neighbors
                [level.min(self.nodes[c.id as usize].neighbors.len() - 1)]
            {
                if visited.insert(n) {
                    let d = self.dist(n, query);
                    let worst = results.iter().map(|r| r.dist).fold(f64::MIN, f64::max);
                    // Keep exploring while we still need filtered hits, or while
                    // n could improve the current frontier.
                    if results.len() < ef || d < worst {
                        candidates.push(Candidate { dist: d, id: n });
                        if passes(n) {
                            results.push(Candidate { dist: d, id: n });
                            if results.len() > ef {
                                // drop the worst filtered result
                                let (idx, _) = results
                                    .iter()
                                    .enumerate()
                                    .max_by(|a, b| a.1.dist.total_cmp(&b.1.dist))
                                    .unwrap();
                                results.swap_remove(idx);
                            }
                        }
                    }
                }
            }
        }
        results.sort_by(|a, b| a.dist.total_cmp(&b.dist));
        results
    }

    pub fn insert(&mut self, event_id: EventId, lsn: Lsn, point: ProductPoint) {
        if self.by_event.contains_key(&event_id) {
            return; // idempotent replay
        }
        let id = self.nodes.len() as u32;
        let level = self.random_level();
        // The node is inserted (with empty adjacency) BEFORE any back-links
        // are created: a search during connection may already traverse it.
        self.nodes.push(Node {
            point: point.clone(),
            level,
            neighbors: vec![Vec::new(); level + 1],
        });
        self.by_event.insert(event_id, id);
        self.ids.push(event_id);
        self.lsns.push(lsn);

        let old_entry = self.entry;
        if let Some(mut ep) = old_entry {
            let top = self.nodes[ep as usize].level;
            // descend greedily above the new node's level
            for l in ((level + 1)..=top).rev() {
                let best =
                    self.search_layer(&point, ep, l.min(self.nodes[ep as usize].level), 1, None);
                ep = best[0].id;
            }
            // connect at each level from min(level, top) down to 0
            for l in (0..=level.min(top)).rev() {
                let neighbors = self.search_layer(&point, ep, l, self.ef_construction, None);
                let selected: Vec<u32> = neighbors
                    .iter()
                    .filter(|c| c.id != id)
                    .take(self.m)
                    .map(|c| c.id)
                    .collect::<Vec<u32>>();
                for &n in &selected {
                    let nl = self.nodes[n as usize].neighbors.len();
                    if l < nl {
                        self.nodes[n as usize].neighbors[l].push(id);
                        if self.nodes[n as usize].neighbors[l].len() > self.m * 2 {
                            // prune: keep the m*2 closest
                            let np = self.nodes[n as usize].point.clone();
                            let mut nb = std::mem::take(&mut self.nodes[n as usize].neighbors[l]);
                            nb.sort_by(|&a, &b| self.dist(a, &np).total_cmp(&self.dist(b, &np)));
                            nb.truncate(self.m * 2);
                            self.nodes[n as usize].neighbors[l] = nb;
                        }
                    }
                }
                self.nodes[id as usize].neighbors[l] = selected;
                ep = neighbors[0].id;
            }
            if level > top {
                self.entry = Some(id);
            }
        } else {
            self.entry = Some(id);
        }
    }

    /// Search top-k. `filter`: only internal ids present in the bitmap are
    /// returned (push-down happens during result selection). Results carry
    /// the LSN they are valid at.
    pub fn search(
        &self,
        query: &ProductPoint,
        k: usize,
        ef: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Vec<VectorHit> {
        let Some(mut ep) = self.entry else {
            return Vec::new();
        };
        let top = self.nodes[ep as usize].level;
        // Entry-point descent ignores the filter (we want the best entry into
        // level 0 regardless); the filter is pushed down only at level 0.
        for l in (1..=top).rev() {
            let best = self.search_layer(query, ep, l, 1, None);
            ep = best[0].id;
        }
        let ef = ef.max(k);
        let candidates =
            self.search_layer(query, ep, 0, ef.max(self.ef_construction.min(64)), filter);
        candidates
            .into_iter()
            .take(k)
            .map(|c| VectorHit {
                id: self.ids[c.id as usize],
                lsn: self.lsns[c.id as usize],
                dist: c.dist as f32,
            })
            .collect()
    }

    /// Internal id for an event (to build filter bitmaps).
    pub fn internal_id(&self, event: &EventId) -> Option<u32> {
        self.by_event.get(event).copied()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

impl View for VectorIndex {
    fn name(&self) -> &str {
        "vector"
    }

    fn apply(&mut self, lsn: Lsn, event: &Episode) {
        if let Some(emb) = &event.embedding {
            self.insert(event.id, lsn, emb.clone());
        }
        self.watermark = lsn;
    }

    fn watermark(&self) -> Lsn {
        self.watermark
    }

    fn reset(&mut self) {
        *self = VectorIndex::new(self.metric.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pt(hyp: Vec<f32>) -> ProductPoint {
        ProductPoint {
            hyp,
            sph: vec![],
            euc: vec![],
        }
    }

    #[test]
    fn finds_nearest_in_small_set() {
        let mut idx = VectorIndex::new(ProductMetric::default());
        let mut ids = Vec::new();
        for i in 0..200 {
            let x = (i as f32) / 250.0;
            let id = EventId::new();
            ids.push(id);
            idx.insert(id, i as u64, pt(vec![x, 0.1]));
        }
        let hits = idx.search(&pt(vec![0.4, 0.1]), 5, 64, None);
        assert_eq!(hits.len(), 5);
        // exact nearest is i=100 (x=0.4)
        assert_eq!(hits[0].id, ids[100]);
    }

    #[test]
    fn filter_push_down() {
        let mut idx = VectorIndex::new(ProductMetric::default());
        let mut keep = RoaringBitmap::new();
        let mut kept_ids = Vec::new();
        for i in 0..100 {
            let id = EventId::new();
            idx.insert(id, i as u64, pt(vec![(i as f32) / 200.0, 0.0]));
            if i % 2 == 0 {
                keep.insert(idx.internal_id(&id).unwrap());
                kept_ids.push(id);
            }
        }
        let hits = idx.search(&pt(vec![0.25, 0.0]), 10, 128, Some(&keep));
        assert!(!hits.is_empty());
        for h in &hits {
            assert!(kept_ids.contains(&h.id), "filtered-out id leaked");
        }
    }

    #[test]
    fn filter_push_down_recall_under_high_selectivity() {
        // Regression (auditoria02 #3): with a highly selective filter (5% kept),
        // post-filtering an ef≈64 pool around the query returns fewer than k.
        // Push-down must keep expanding until it has k filtered hits.
        let mut idx = VectorIndex::new(ProductMetric::default());
        let mut keep = RoaringBitmap::new();
        let mut kept = Vec::new();
        for i in 0..500u64 {
            let id = EventId(ulid::Ulid::from_parts(i, i as u128));
            idx.insert(id, i, pt(vec![(i as f32) / 600.0, 0.02]));
            if i % 20 == 0 {
                keep.insert(idx.internal_id(&id).unwrap());
                kept.push(id);
            }
        }
        let hits = idx.search(&pt(vec![300.0 / 600.0, 0.02]), 5, 64, Some(&keep));
        assert_eq!(hits.len(), 5, "push-down must return k even under a 5% filter");
        for h in &hits {
            assert!(kept.contains(&h.id), "filtered-out id leaked");
        }
    }

    #[test]
    fn deterministic_replay() {
        let build = || {
            let mut idx = VectorIndex::new(ProductMetric::default());
            for i in 0..100u64 {
                let id = EventId(ulid::Ulid::from_parts(i, i as u128));
                idx.insert(id, i, pt(vec![(i as f32) / 150.0, 0.05]));
            }
            let hits = idx.search(&pt(vec![0.3, 0.05]), 10, 64, None);
            hits.iter().map(|h| h.id).collect::<Vec<_>>()
        };
        assert_eq!(build(), build(), "same input order must give same index");
    }
}
