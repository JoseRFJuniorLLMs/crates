//! heraclitus-index-graph — derived adjacency / property indexes (§3.6).
//!
//! Adjacency comes from `Episode.parents` (causal provenance edges).
//! Property index maps `(attr_key, attr_value) -> bitmap of internal ids`
//! for ANN filter push-down.

use dashmap::DashMap;
use heraclitus_core::{Episode, EventId, Lsn};
use heraclitus_views::View;
use roaring::RoaringBitmap;
use std::collections::HashMap;

pub mod adaptive; // M17: regras aprendidas de feedback (threshold tuning)
pub mod decision; // M15: regras que agem (Action events no log)
pub mod entity; // M11: entity resolution determinística e temporal
pub mod temporal; // M8: grafo temporal + probabilístico (RFC-004/005/006/007)

#[derive(Default)]
pub struct GraphIndex {
    /// parent -> children
    out: DashMap<EventId, Vec<EventId>>,
    /// child -> parents
    inn: DashMap<EventId, Vec<EventId>>,
    /// (key=value) -> internal id bitmap
    attr_idx: DashMap<String, RoaringBitmap>,
    /// event -> dense internal id (assigned in LSN order: deterministic)
    internal: HashMap<EventId, u32>,
    by_internal: Vec<EventId>,
    /// event -> lsn (snapshot reads)
    lsn_of: HashMap<EventId, Lsn>,
    watermark: Lsn,
}

impl GraphIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn children(&self, id: &EventId) -> Vec<EventId> {
        self.out.get(id).map(|v| v.clone()).unwrap_or_default()
    }

    pub fn parents(&self, id: &EventId) -> Vec<EventId> {
        self.inn.get(id).map(|v| v.clone()).unwrap_or_default()
    }

    /// Walk provenance ancestors up to `depth` hops (PROVENANCE(fact), §3.12).
    pub fn ancestors(&self, id: &EventId, depth: usize) -> Vec<EventId> {
        let mut frontier = vec![*id];
        let mut seen = vec![];
        for _ in 0..depth {
            let mut next = Vec::new();
            for f in &frontier {
                for p in self.parents(f) {
                    if !seen.contains(&p) {
                        seen.push(p);
                        next.push(p);
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
        seen
    }

    /// Bitmap of internal ids where `attrs[key] == value` (Eq filter).
    pub fn filter_eq(&self, key: &str, value: &str) -> RoaringBitmap {
        self.attr_idx
            .get(&format!("{key}={value}"))
            .map(|b| b.clone())
            .unwrap_or_default()
    }

    pub fn internal_id(&self, id: &EventId) -> Option<u32> {
        self.internal.get(id).copied()
    }

    pub fn event_of_internal(&self, internal: u32) -> Option<EventId> {
        self.by_internal.get(internal as usize).copied()
    }

    pub fn lsn_of(&self, id: &EventId) -> Option<Lsn> {
        self.lsn_of.get(id).copied()
    }

    pub fn len(&self) -> usize {
        self.by_internal.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_internal.is_empty()
    }
}

/// Snapshot serializável (fast boot). DashMaps viram Vec de pares; bitmaps
/// roaring vão no formato nativo (portável); `internal` reconstrói-se de
/// `by_internal` no restore.
#[derive(serde::Serialize, serde::Deserialize)]
struct GraphSnapshot {
    out: Vec<(EventId, Vec<EventId>)>,
    inn: Vec<(EventId, Vec<EventId>)>,
    attr: Vec<(String, Vec<u8>)>,
    by_internal: Vec<EventId>,
    lsn_of: Vec<(EventId, Lsn)>,
    watermark: Lsn,
}

impl View for GraphIndex {
    fn name(&self) -> &str {
        "graph"
    }

    fn checkpoint(&self, dir: &std::path::Path) -> Result<(), heraclitus_core::HeraclitusError> {
        let mut attr = Vec::with_capacity(self.attr_idx.len());
        for e in self.attr_idx.iter() {
            let mut bytes = Vec::with_capacity(e.value().serialized_size());
            e.value()
                .serialize_into(&mut bytes)
                .map_err(|err| heraclitus_core::HeraclitusError::Serialization(err.to_string()))?;
            attr.push((e.key().clone(), bytes));
        }
        heraclitus_views::ckpt::save(
            dir,
            "graph",
            &GraphSnapshot {
                out: self
                    .out
                    .iter()
                    .map(|e| (*e.key(), e.value().clone()))
                    .collect(),
                inn: self
                    .inn
                    .iter()
                    .map(|e| (*e.key(), e.value().clone()))
                    .collect(),
                attr,
                by_internal: self.by_internal.clone(),
                lsn_of: self.lsn_of.iter().map(|(k, v)| (*k, *v)).collect(),
                watermark: self.watermark,
            },
        )
    }

    fn restore(&mut self, dir: &std::path::Path) -> Result<bool, heraclitus_core::HeraclitusError> {
        let Some(snap) = heraclitus_views::ckpt::load::<GraphSnapshot>(dir, "graph")? else {
            return Ok(false);
        };
        self.out = snap.out.into_iter().collect();
        self.inn = snap.inn.into_iter().collect();
        self.attr_idx = snap
            .attr
            .into_iter()
            .map(|(k, bytes)| {
                RoaringBitmap::deserialize_from(&bytes[..])
                    .map(|b| (k, b))
                    .map_err(|e| heraclitus_core::HeraclitusError::Serialization(e.to_string()))
            })
            .collect::<Result<_, _>>()?;
        self.internal = snap
            .by_internal
            .iter()
            .enumerate()
            .map(|(i, id)| (*id, i as u32))
            .collect();
        self.by_internal = snap.by_internal;
        self.lsn_of = snap.lsn_of.into_iter().collect();
        self.watermark = snap.watermark;
        Ok(true)
    }

    fn apply(&mut self, lsn: Lsn, event: &Episode) {
        // Audit #9: idempotent replay must bail out entirely — continuing
        // would duplicate adjacency rows and index a wrong internal id.
        if self.internal.contains_key(&event.id) {
            self.watermark = lsn;
            return;
        }
        let internal = self.by_internal.len() as u32;
        self.internal.insert(event.id, internal);
        self.by_internal.push(event.id);
        self.lsn_of.insert(event.id, lsn);
        for parent in &event.parents {
            self.out.entry(*parent).or_default().push(event.id);
            self.inn.entry(event.id).or_default().push(*parent);
        }
        for (k, v) in &event.attrs {
            self.attr_idx
                .entry(format!("{k}={v}"))
                .or_default()
                .insert(internal);
        }
        self.watermark = lsn;
    }

    fn watermark(&self) -> Lsn {
        self.watermark
    }

    fn reset(&mut self) {
        *self = GraphIndex::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use heraclitus_core::EventKind;

    #[test]
    fn adjacency_and_attrs() {
        let mut g = GraphIndex::new();
        let mut a = Episode::new("x", EventKind::Observation, b"a".to_vec());
        a.attrs.insert("topic".into(), "rivers".into());
        let mut b = Episode::new("x", EventKind::Observation, b"b".to_vec());
        b.parents.push(a.id);
        g.apply(0, &a);
        g.apply(1, &b);

        assert_eq!(g.children(&a.id), vec![b.id]);
        assert_eq!(g.parents(&b.id), vec![a.id]);
        assert_eq!(g.ancestors(&b.id, 3), vec![a.id]);
        assert!(g
            .filter_eq("topic", "rivers")
            .contains(g.internal_id(&a.id).unwrap()));
    }
}
