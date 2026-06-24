//! The execution boundary. The query crate never owns indexes — it asks a
//! [`QueryBackend`]. The server composes the real backend (HNSW + BM25 +
//! activation + retrieval); [`LogBackend`] is the reference implementation
//! over the raw log: exact, slow, and always correct (it is also what view
//! rebuilds are checked against).

use crate::ast::{SimulateOp, Value};
use crate::fusion::{FusedHit, FusionInput, FusionWeights};
use heraclitus_core::{Episode, EventKind, HeraclitusError, Lsn};
use heraclitus_index_graph::adaptive::{self, LabeledFlag, PolicyEval};
use heraclitus_index_graph::decision::{self, DecisionPolicy};
use heraclitus_index_graph::entity::EntityResolver;
use heraclitus_index_graph::temporal::{Edge, EdgeType, EdgeVersion, TemporalGraph};
use heraclitus_log::Log;
use std::collections::BTreeMap;
use std::sync::Arc;

/// One neighbor row returned by `NEIGHBORS` (M8). `belief` is the aggregated
/// edge confidence (RFC-004); `weight = belief × temporal decay` (RFC-006).
#[derive(Debug, Clone)]
pub struct NeighborRow {
    pub edge_id: String,
    pub to: String,
    pub etype: String,
    pub belief: f32,
    pub weight: f32,
    /// LSN at which the edge to this neighbor appeared (M10 fusion carries it so
    /// graph-only candidates get a real lsn, not 0).
    pub lsn: Lsn,
}

/// One edge row returned by the relationship MATCH `(a)-[r]->(b)` (M9).
#[derive(Debug, Clone)]
pub struct EdgeRow {
    pub edge_id: String,
    pub from: String,
    pub to: String,
    pub etype: String,
    pub belief: f32,
}

/// One competing hypothesis about an edge (M12).
#[derive(Debug, Clone)]
pub struct HypothesisRow {
    pub hypothesis_id: String,
    pub confidence: f32,
    pub polarity: f32,
    pub source: String,
}

/// All hypotheses about one edge as of a snapshot, plus the aggregated belief
/// (M12). The query surfaces the competing claims; the agent chooses.
#[derive(Debug, Clone)]
pub struct EdgeHypotheses {
    pub edge_id: String,
    pub alive: bool,
    pub belief: f32,
    pub versions: Vec<HypothesisRow>,
}

/// The outcome of an `ADAPT` evaluation (M17): the learned threshold plus the
/// before/after precision-recall, so the impact is measurable.
#[derive(Debug, Clone)]
pub struct AdaptReport {
    pub rule: String,
    pub samples: usize,
    pub learned_threshold: f32,
    pub default: PolicyEval,
    pub adapted: PolicyEval,
}

/// One action emitted by the decision engine (M15).
#[derive(Debug, Clone)]
pub struct ActionRecord {
    pub action_id: String,
    pub rule: String,
    pub subject: String,
    pub reason: String,
    pub lsn: Lsn,
}

/// The outcome of a `DECIDE` evaluation (M15): which actions were newly emitted
/// and which were skipped because their `action_id` was already in the log.
#[derive(Debug, Clone, Default)]
pub struct DecisionReport {
    pub fired: Vec<ActionRecord>,
    pub skipped: Vec<String>,
}

/// The community of a node plus its members (M14).
#[derive(Debug, Clone)]
pub struct CommunityResult {
    pub node: String,
    pub community: String,
    pub members: Vec<String>,
}

/// Per-node graph metrics (M14): degree, normalized centrality, anomaly score
/// (degree z-score) and the node's community.
#[derive(Debug, Clone)]
pub struct MetricsResult {
    pub node: String,
    pub community: String,
    pub degree: u32,
    pub centrality: f32,
    pub anomaly_score: f32,
}

/// One node of a causal trace (M13): an event, its depth from the target, and
/// its direct causes (the provenance parents present in the snapshot).
#[derive(Debug, Clone, PartialEq)]
pub struct CausalStep {
    pub id: String,
    pub depth: usize,
    pub causes: Vec<String>,
}

/// The answer to `WHY X` (M13): the minimal causal chain behind `target` —
/// exactly the provenance-ancestor closure, deduplicated to each node's
/// shortest depth — plus the root causes (events with no provenance).
#[derive(Debug, Clone, PartialEq)]
pub struct Trace {
    pub target: String,
    pub steps: Vec<CausalStep>,
    pub roots: Vec<String>,
}

/// Query guard: the maximum number of episodes a single user `MATCH` scan will
/// materialize. A query over a huge log is capped here so it cannot exhaust
/// memory and crash the server. Time-windowed queries (`WHERE n.lsn` bounds)
/// prune to a small set of segments and stay well under this.
pub const QUERY_SCAN_CAP: usize = 250_000;

pub trait QueryBackend {
    /// All events visible at `as_of` (lsn < as_of) or everything when None.
    fn scan(&self, as_of: Option<Lsn>) -> Result<Vec<(Lsn, Episode)>, HeraclitusError>;

    /// Scan the LSN window `[from, to)`, capped at [`QUERY_SCAN_CAP`] rows — the
    /// scalable, crash-safe path used by `MATCH` (the planner pushes any
    /// `n.lsn` bounds in the `WHERE` down to `from`/`to`). The default filters
    /// over `scan`; real backends override to prune segments in the log.
    fn scan_range(&self, from: Lsn, to: Lsn) -> Result<Vec<(Lsn, Episode)>, HeraclitusError> {
        Ok(self
            .scan(Some(to))?
            .into_iter()
            .filter(|(l, _)| *l >= from)
            .take(QUERY_SCAN_CAP)
            .collect())
    }

    /// Index-backed exact lookup `field == value` over the WHOLE log
    /// (O(postings), not a capped scan). Returns matching `(lsn, episode)` rows
    /// bounded by `as_of` (lsn < as_of). `None` ⇒ this backend has no attribute
    /// index, so the planner falls back to `scan_range`. Default: no index.
    fn attr_lookup(
        &self,
        _field: &str,
        _value: &str,
        _as_of: Option<Lsn>,
    ) -> Result<Option<Vec<(Lsn, Episode)>>, HeraclitusError> {
        Ok(None)
    }

    /// M18: the consistency point — the next LSN the backend would assign, i.e.
    /// one past the highest applied LSN. `REQUIRE LSN >= X` is met iff
    /// `head() >= X`. (Views here apply synchronously, so this equals the log
    /// head; an async replica would return its view watermark instead.)
    fn head(&self) -> Result<Lsn, HeraclitusError> {
        Ok(self.scan(None)?.last().map(|(l, _)| l + 1).unwrap_or(0))
    }

    /// The derived temporal graph this backend serves (M16). The default
    /// rebuilds it from the log; a counterfactual `VirtualBackend` overrides this
    /// to return its overlay, so nested `SIMULATE` composes mutations instead of
    /// rebuilding from the real log and losing the outer one.
    fn graph(&self) -> Result<TemporalGraph, HeraclitusError> {
        let mut g = TemporalGraph::new();
        for (lsn, e) in self.scan(None)? {
            g.apply_episode(lsn, &e);
        }
        Ok(g)
    }
    /// Text retrieval, scored. Real impl: two-stage RRF; reference: tf scan.
    fn recall(
        &self,
        text: &str,
        k: usize,
        as_of: Option<Lsn>,
    ) -> Result<Vec<(Lsn, Episode, f32)>, HeraclitusError>;
    /// Vector retrieval. Real impl: HNSW; reference: brute force.
    fn nearest(
        &self,
        vector: &[f32],
        k: usize,
        as_of: Option<Lsn>,
    ) -> Result<Vec<(Lsn, Episode, f32)>, HeraclitusError>;
    /// Parent ids (provenance pointers) of the given event id.
    fn provenance(&self, id: &str) -> Result<Vec<String>, HeraclitusError>;
    /// M8: outgoing neighbors of `node` in the derived graph, optionally
    /// filtered by edge type, snapshot-bounded by `as_of`, thresholded by
    /// `min_confidence`. Real impl: incremental view; reference: replay.
    fn neighbors(
        &self,
        node: &str,
        etype: Option<&str>,
        as_of: Option<Lsn>,
        min_confidence: f32,
    ) -> Result<Vec<NeighborRow>, HeraclitusError>;
    /// M8: deterministic BFS from `start` up to `max_depth` hops over the
    /// derived graph. Returns `(node, depth)` in discovery order.
    fn traverse(
        &self,
        start: &str,
        max_depth: usize,
        as_of: Option<Lsn>,
        min_confidence: f32,
    ) -> Result<Vec<(String, usize)>, HeraclitusError>;
    /// M9: relationship MATCH `(a)-[r]->(b) AS OF X`. Returns the edges alive at
    /// `as_of` matching the optional source / type / destination filters.
    fn match_edges(
        &self,
        src: Option<&str>,
        etype: Option<&str>,
        dst: Option<&str>,
        as_of: Option<Lsn>,
    ) -> Result<Vec<EdgeRow>, HeraclitusError>;

    /// M14: the community of `node` (connected component) and its members as of
    /// `as_of`. `None` if the node is in no alive edge.
    fn community(
        &self,
        node: &str,
        as_of: Option<Lsn>,
    ) -> Result<Option<CommunityResult>, HeraclitusError>;
    /// M14: degree / centrality / anomaly / community of `node` as of `as_of`.
    fn node_metrics(
        &self,
        node: &str,
        as_of: Option<Lsn>,
    ) -> Result<Option<MetricsResult>, HeraclitusError>;

    /// M12: all competing hypotheses about edge `(from)-[etype]->(to)` as of
    /// `as_of`, with the aggregated belief. `None` if the edge does not exist.
    fn edge_hypotheses(
        &self,
        from: &str,
        to: &str,
        etype: &str,
        as_of: Option<Lsn>,
    ) -> Result<Option<EdgeHypotheses>, HeraclitusError>;

    /// M13: `WHY target` — the minimal causal chain behind `target`, walked over
    /// the provenance DAG (`Episode.parents`) up to `max_depth` hops, bounded by
    /// `as_of`. Composed from `scan`, so every backend gets it identically and
    /// the result is checkable against raw provenance / distill output.
    fn why(
        &self,
        target: &str,
        max_depth: usize,
        as_of: Option<Lsn>,
    ) -> Result<Trace, HeraclitusError> {
        // Provenance restricted to the snapshot: an event present at `as_of`
        // maps to the parents that are *also* present (causality cannot cite a
        // cause the snapshot has not yet seen).
        let events = self.scan(as_of)?;
        let present: std::collections::BTreeSet<String> =
            events.iter().map(|(_, e)| e.id.to_string()).collect();
        let mut parents: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (_, e) in &events {
            let ps: Vec<String> = e
                .parents
                .iter()
                .map(|p| p.to_string())
                .filter(|p| present.contains(p))
                .collect();
            parents.insert(e.id.to_string(), ps);
        }
        Ok(trace_causes(&parents, target, max_depth))
    }

    /// M15: evaluate the decision policy over the snapshot at `as_of` and emit a
    /// new `Action` event into the log for every proposed action whose
    /// `action_id` is not already present. Idempotent: re-running emits nothing
    /// new. Composed from `scan` + `append`, so the decision IS a log event and
    /// every backend behaves identically.
    fn decide(
        &self,
        policy: DecisionPolicy,
        as_of: Option<Lsn>,
    ) -> Result<DecisionReport, HeraclitusError> {
        // Build the graph from the snapshot (same derivation as the view).
        let mut g = TemporalGraph::new();
        for (lsn, e) in self.scan(as_of)? {
            g.apply_episode(lsn, &e);
        }
        let decisions = decision::evaluate(&g, u64::MAX, &policy);

        // Idempotency key set: every action_id ever emitted (whole log).
        let existing: std::collections::BTreeSet<String> = self
            .scan(None)?
            .into_iter()
            .filter(|(_, e)| e.kind == EventKind::Action)
            .filter_map(|(_, e)| e.attrs.get("action_id").cloned())
            .collect();

        let mut report = DecisionReport::default();
        for d in decisions {
            if existing.contains(&d.action_id) {
                report.skipped.push(d.action_id);
                continue;
            }
            // The decision becomes an Action event in the log.
            let props = [
                ("action_id".to_string(), Value::Str(d.action_id.clone())),
                ("rule".to_string(), Value::Str(d.rule.clone())),
                ("subject".to_string(), Value::Str(d.subject.clone())),
                ("reason".to_string(), Value::Str(d.reason.clone())),
            ];
            let lsn = self.append(Some("action"), &props)?;
            report.fired.push(ActionRecord {
                action_id: d.action_id,
                rule: d.rule,
                subject: d.subject,
                reason: d.reason,
                lsn,
            });
        }
        Ok(report)
    }

    /// M17: learn the decision threshold for `flag_anomaly` from feedback events
    /// in the log (`feedback_rule` + `score` + `verdict`), reporting the
    /// before/after precision-recall. Pure and replay-stable — the new rule is
    /// just the best threshold derivable from the feedback so far.
    fn adapt(&self, as_of: Option<Lsn>) -> Result<AdaptReport, HeraclitusError> {
        let rule = "flag_anomaly";
        let samples: Vec<LabeledFlag> = self
            .scan(as_of)?
            .into_iter()
            .filter(|(_, e)| e.attrs.get("feedback_rule").map(|r| r.as_str()) == Some(rule))
            .filter_map(|(_, e)| {
                let score = e.attrs.get("score")?.parse::<f32>().ok()?;
                let confirmed = e
                    .attrs
                    .get("verdict")
                    .map(|v| v == "confirm")
                    .unwrap_or(false);
                Some(LabeledFlag { score, confirmed })
            })
            .collect();

        let default_threshold = DecisionPolicy::default().anomaly_threshold;
        let learned_threshold = adaptive::learn_threshold(&samples, default_threshold);
        Ok(AdaptReport {
            rule: rule.to_string(),
            samples: samples.len(),
            learned_threshold,
            default: adaptive::evaluate_threshold(&samples, default_threshold),
            adapted: adaptive::evaluate_threshold(&samples, learned_threshold),
        })
    }

    /// M11: resolve a key to its canonical entity id as of `as_of`.
    fn resolve_entity(
        &self,
        key: &str,
        as_of: Option<Lsn>,
    ) -> Result<Option<String>, HeraclitusError>;
    /// M11: all keys that resolve to `entity_id` as of `as_of`.
    fn entity_cluster(
        &self,
        entity_id: &str,
        as_of: Option<Lsn>,
    ) -> Result<Vec<String>, HeraclitusError>;

    /// M10: the hybrid query engine (the moat). Fuses three channels around an
    /// anchor node — graph connectivity (belief of edges out of `connected_to`),
    /// vector similarity to `vector`, and lexical relevance to `text` — into one
    /// reproducible top-K ranking. Composed from the primitive channels, so
    /// every backend gets it identically (and the reference is checkable).
    fn find_fused(
        &self,
        text: &str,
        vector: &[f32],
        connected_to: &str,
        weights: FusionWeights,
        k: usize,
        as_of: Option<Lsn>,
    ) -> Result<Vec<FusedHit>, HeraclitusError> {
        let fetch = (k * 4).max(8);

        // Raw per-channel signals, keyed later by candidate id.
        // graph: 1-hop neighbors of the anchor, signal = aggregated belief.
        // `n.lsn` is the real lsn of the candidate, so a graph-only candidate is
        // no longer reported at lsn 0 (audit bug C).
        let mut graph: Vec<(String, u64, f32)> = self
            .neighbors(connected_to, None, as_of, 0.0)?
            .into_iter()
            .map(|n| (n.to, n.lsn, n.belief))
            .collect();
        // vector: nearest, distance → similarity 1/(1+d).
        let mut vec_ch: Vec<(String, u64, f32)> = self
            .nearest(vector, fetch, as_of)?
            .into_iter()
            .map(|(l, e, d)| (e.id.to_string(), l, 1.0 / (1.0 + d)))
            .collect();
        // text: lexical recall score (tf / BM25 surrogate).
        let mut txt_ch: Vec<(String, u64, f32)> = self
            .recall(text, fetch, as_of)?
            .into_iter()
            .map(|(l, e, s)| (e.id.to_string(), l, s))
            .collect();

        // Normalize each channel independently so scale can't let one dominate.
        norm_channel(&mut graph);
        norm_channel(&mut vec_ch);
        norm_channel(&mut txt_ch);

        // Merge by candidate id, then fuse and rank.
        let mut rows: BTreeMap<String, (u64, FusionInput)> = BTreeMap::new();
        fold_channel(&mut rows, graph, 0);
        fold_channel(&mut rows, vec_ch, 1);
        fold_channel(&mut rows, txt_ch, 2);

        let mut hits: Vec<FusedHit> = rows
            .into_iter()
            .map(|(id, (lsn, input))| {
                let score = weights.fuse(&input);
                FusedHit {
                    id,
                    lsn,
                    input,
                    score,
                }
            })
            .collect();
        // Deterministic order: score desc, id asc as the tie-break.
        hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.id.cmp(&b.id)));
        hits.truncate(k);
        Ok(hits)
    }
    /// Audit #4: resolve `AS OF TIMESTAMP t` (HLC physical millis) to an
    /// LSN bound — the first LSN whose event is strictly after `t`.
    fn lsn_for_timestamp(&self, ts_ms: u64) -> Result<Lsn, HeraclitusError>;
    /// CREATE lowers to a log append.
    fn append(
        &self,
        label: Option<&str>,
        props: &[(String, Value)],
    ) -> Result<Lsn, HeraclitusError>;
}

/// Reference backend straight over the log.
pub struct LogBackend {
    log: Arc<Log>,
}

impl LogBackend {
    pub fn new(log: Arc<Log>) -> Self {
        Self { log }
    }
}

impl QueryBackend for LogBackend {
    fn scan(&self, as_of: Option<Lsn>) -> Result<Vec<(Lsn, Episode)>, HeraclitusError> {
        self.log.scan(0, as_of.unwrap_or(u64::MAX))
    }

    fn scan_range(&self, from: Lsn, to: Lsn) -> Result<Vec<(Lsn, Episode)>, HeraclitusError> {
        // Segment pruning + row cap pushed into the log.
        self.log.scan_capped(from, to, QUERY_SCAN_CAP)
    }

    fn head(&self) -> Result<Lsn, HeraclitusError> {
        Ok(self.log.head())
    }

    fn recall(
        &self,
        text: &str,
        k: usize,
        as_of: Option<Lsn>,
    ) -> Result<Vec<(Lsn, Episode, f32)>, HeraclitusError> {
        let needle = text.to_lowercase();
        let mut hits: Vec<(Lsn, Episode, f32)> = self
            .scan(as_of)?
            .into_iter()
            .filter_map(|(l, e)| {
                let body = String::from_utf8_lossy(&e.content).to_lowercase();
                let tf = body.matches(&needle).count();
                (tf > 0).then_some((l, e, tf as f32))
            })
            .collect();
        hits.sort_by(|a, b| b.2.total_cmp(&a.2).then(b.0.cmp(&a.0)));
        hits.truncate(k);
        Ok(hits)
    }

    fn nearest(
        &self,
        vector: &[f32],
        k: usize,
        as_of: Option<Lsn>,
    ) -> Result<Vec<(Lsn, Episode, f32)>, HeraclitusError> {
        let mut hits: Vec<(Lsn, Episode, f32)> = self
            .scan(as_of)?
            .into_iter()
            .filter_map(|(l, e)| {
                let emb = e.embedding.clone()?;
                // Reference distance: Euclidean over the concatenated point.
                let flat: Vec<f32> = emb
                    .hyp
                    .iter()
                    .chain(emb.sph.iter())
                    .chain(emb.euc.iter())
                    .copied()
                    .collect();
                let d: f32 = flat
                    .iter()
                    .zip(vector.iter().chain(std::iter::repeat(&0.0)))
                    .map(|(a, b)| (a - b) * (a - b))
                    .sum::<f32>()
                    .sqrt();
                Some((l, e, d))
            })
            .collect();
        hits.sort_by(|a, b| a.2.total_cmp(&b.2));
        hits.truncate(k);
        Ok(hits)
    }

    fn provenance(&self, id: &str) -> Result<Vec<String>, HeraclitusError> {
        for (_, e) in self.scan(None)? {
            if e.id.to_string() == id {
                return Ok(e.parents.iter().map(|p| p.to_string()).collect());
            }
        }
        Ok(Vec::new())
    }

    fn lsn_for_timestamp(&self, ts_ms: u64) -> Result<Lsn, HeraclitusError> {
        for (lsn, e) in self.scan(None)? {
            if (e.ts_hlc >> 16) > ts_ms {
                return Ok(lsn);
            }
        }
        Ok(u64::MAX)
    }

    fn neighbors(
        &self,
        node: &str,
        etype: Option<&str>,
        as_of: Option<Lsn>,
        min_confidence: f32,
    ) -> Result<Vec<NeighborRow>, HeraclitusError> {
        // Reference path: rebuild the whole graph from the log (a full replay),
        // then read. Exact and slow — this is what the incremental view in the
        // engine is checked against (the M8 determinism gate).
        let g = replay_graph(&self.log)?;
        Ok(neighbors_of(&g, node, etype, as_of, min_confidence))
    }

    fn traverse(
        &self,
        start: &str,
        max_depth: usize,
        as_of: Option<Lsn>,
        min_confidence: f32,
    ) -> Result<Vec<(String, usize)>, HeraclitusError> {
        let g = replay_graph(&self.log)?;
        Ok(traverse_of(&g, start, max_depth, as_of, min_confidence))
    }

    fn match_edges(
        &self,
        src: Option<&str>,
        etype: Option<&str>,
        dst: Option<&str>,
        as_of: Option<Lsn>,
    ) -> Result<Vec<EdgeRow>, HeraclitusError> {
        let g = replay_graph(&self.log)?;
        Ok(match_edges_of(&g, src, etype, dst, as_of))
    }

    fn edge_hypotheses(
        &self,
        from: &str,
        to: &str,
        etype: &str,
        as_of: Option<Lsn>,
    ) -> Result<Option<EdgeHypotheses>, HeraclitusError> {
        Ok(hypotheses_of(
            &replay_graph(&self.log)?,
            from,
            to,
            etype,
            as_of,
        ))
    }

    fn community(
        &self,
        node: &str,
        as_of: Option<Lsn>,
    ) -> Result<Option<CommunityResult>, HeraclitusError> {
        Ok(community_of(&replay_graph(&self.log)?, node, as_of))
    }

    fn node_metrics(
        &self,
        node: &str,
        as_of: Option<Lsn>,
    ) -> Result<Option<MetricsResult>, HeraclitusError> {
        Ok(node_metrics_of(&replay_graph(&self.log)?, node, as_of))
    }

    fn resolve_entity(
        &self,
        key: &str,
        as_of: Option<Lsn>,
    ) -> Result<Option<String>, HeraclitusError> {
        Ok(resolve_of(&replay_resolver(&self.log)?, key, as_of))
    }

    fn entity_cluster(
        &self,
        entity_id: &str,
        as_of: Option<Lsn>,
    ) -> Result<Vec<String>, HeraclitusError> {
        Ok(cluster_of(&replay_resolver(&self.log)?, entity_id, as_of))
    }

    fn append(
        &self,
        label: Option<&str>,
        props: &[(String, Value)],
    ) -> Result<Lsn, HeraclitusError> {
        let kind = match label {
            Some(l) if l.eq_ignore_ascii_case("action") => EventKind::Action,
            Some(l) if l.eq_ignore_ascii_case("message") => EventKind::Message,
            Some(l) if l.eq_ignore_ascii_case("observation") || l.is_empty() => {
                EventKind::Observation
            }
            Some(l) => EventKind::Custom(l.to_string()),
            None => EventKind::Observation,
        };
        let mut e = Episode::new("query", kind, Vec::new());
        for (k, v) in props {
            let s = match v {
                Value::Str(s) => s.clone(),
                Value::Num(n) => n.to_string(),
            };
            e.attrs.insert(k.clone(), s);
        }
        self.log.append(e)
    }
}

/// BFS the provenance DAG from `target`, deduplicating each node to its
/// shortest depth (M13). `parents`: event → its in-snapshot causal parents.
/// The returned trace's edges ARE the provenance pointers — that is the
/// consistency contract (it can be checked against raw provenance / distill).
pub fn trace_causes(
    parents: &BTreeMap<String, Vec<String>>,
    target: &str,
    max_depth: usize,
) -> Trace {
    use std::collections::VecDeque;
    // Unknown target → empty trace (not an error).
    if !parents.contains_key(target) {
        return Trace {
            target: target.to_string(),
            steps: Vec::new(),
            roots: Vec::new(),
        };
    }
    let mut depth_of: BTreeMap<String, usize> = BTreeMap::new();
    let mut q: VecDeque<(String, usize)> = VecDeque::new();
    depth_of.insert(target.to_string(), 0);
    q.push_back((target.to_string(), 0));

    let mut steps: Vec<CausalStep> = Vec::new();
    let mut roots: Vec<String> = Vec::new();
    while let Some((id, d)) = q.pop_front() {
        let causes = parents.get(&id).cloned().unwrap_or_default();
        if causes.is_empty() {
            roots.push(id.clone());
        }
        // Expand only while under the depth budget; causes are always reported.
        if d < max_depth {
            for p in &causes {
                if !depth_of.contains_key(p) {
                    depth_of.insert(p.clone(), d + 1);
                    q.push_back((p.clone(), d + 1));
                }
            }
        }
        steps.push(CausalStep {
            id,
            depth: d,
            causes,
        });
    }
    // Deterministic order: by depth, then id.
    steps.sort_by(|a, b| a.depth.cmp(&b.depth).then(a.id.cmp(&b.id)));
    roots.sort();
    roots.dedup();
    Trace {
        target: target.to_string(),
        steps,
        roots,
    }
}

/// Min-max normalize the score column of one fusion channel in place (M10).
fn norm_channel(items: &mut [(String, u64, f32)]) {
    let mut s: Vec<f32> = items.iter().map(|x| x.2).collect();
    crate::fusion::normalize(&mut s);
    for (it, n) in items.iter_mut().zip(s) {
        it.2 = n;
    }
}

/// Fold one normalized channel into the per-candidate fusion inputs (M10).
/// `ch`: 0 = graph, 1 = vector, 2 = text. Fills the lsn from the first channel
/// that knows it (the vector/text channels carry it; graph does not).
fn fold_channel(
    rows: &mut BTreeMap<String, (u64, FusionInput)>,
    items: Vec<(String, u64, f32)>,
    ch: usize,
) {
    for (id, lsn, sc) in items {
        let e = rows.entry(id).or_insert((0, FusionInput::default()));
        if e.0 == 0 && lsn != 0 {
            e.0 = lsn;
        }
        match ch {
            0 => e.1.graph_score = sc,
            1 => e.1.vector_score = sc,
            _ => e.1.text_score = sc,
        }
    }
}

/// Rebuild the derived temporal graph from the whole log (a full replay).
/// The reference backend and any view-consistency check share this so the
/// derivation rule has a single source of truth.
pub fn replay_graph(log: &Log) -> Result<TemporalGraph, HeraclitusError> {
    let mut g = TemporalGraph::new();
    for (lsn, e) in log.scan(0, u64::MAX)? {
        g.apply_episode(lsn, &e);
    }
    Ok(g)
}

/// Rebuild the entity resolver from the whole log (M11 reference path).
pub fn replay_resolver(log: &Log) -> Result<EntityResolver, HeraclitusError> {
    let mut r = EntityResolver::new();
    for (lsn, e) in log.scan(0, u64::MAX)? {
        r.apply_episode(lsn, &e);
    }
    Ok(r)
}

/// Resolve a key off an already-built resolver (shared so the AS OF mapping is
/// identical across backends).
pub fn resolve_of(r: &EntityResolver, key: &str, as_of: Option<Lsn>) -> Option<String> {
    r.resolve(key, as_of_point(as_of)?)
}

/// Read an entity cluster off an already-built resolver (same AS OF mapping).
pub fn cluster_of(r: &EntityResolver, entity_id: &str, as_of: Option<Lsn>) -> Vec<String> {
    let Some(point) = as_of_point(as_of) else {
        return Vec::new();
    };
    r.cluster(entity_id, point)
}

/// Map a GQL `AS OF` bound (exclusive upper LSN, like the rest of the engine —
/// `MATCH ... AS OF LSN n` sees `lsn < n`) to the temporal graph's inclusive
/// "alive at" point. `at = bound - 1` ⇒ an edge created at `lsn == bound` is not
/// yet visible, matching the snapshot you'd rebuild from `scan(bound)`.
///
/// `AS OF LSN 0` (the empty snapshot, `lsn < 0`) has no inclusive point in `u64`
/// — `bound - 1` would wrongly keep `valid_from == 0` edges — so it maps to
/// `None`, and callers short-circuit to an empty result.
fn as_of_point(as_of: Option<Lsn>) -> Option<Lsn> {
    match as_of {
        None => Some(u64::MAX),
        Some(0) => None,
        Some(b) => Some(b - 1),
    }
}

/// Read `NEIGHBORS` off an already-built graph (shared by every backend so
/// the row shape and filtering are identical regardless of how the graph was
/// materialized).
pub fn neighbors_of(
    g: &TemporalGraph,
    node: &str,
    etype: Option<&str>,
    as_of: Option<Lsn>,
    min_confidence: f32,
) -> Vec<NeighborRow> {
    let Some(point) = as_of_point(as_of) else {
        return Vec::new();
    };
    let et = etype.map(EdgeType::from_attr);
    g.neighbors(&node.to_string(), et.as_ref(), point, min_confidence, 0.0)
        .into_iter()
        .map(|n| NeighborRow {
            edge_id: n.edge_id,
            to: n.to,
            etype: n.etype.key(),
            belief: n.belief,
            weight: n.weight,
            lsn: n.lsn,
        })
        .collect()
}

/// Read `TRAVERSE` off an already-built graph (shared so AS OF semantics match
/// `neighbors_of` exactly).
pub fn traverse_of(
    g: &TemporalGraph,
    start: &str,
    max_depth: usize,
    as_of: Option<Lsn>,
    min_confidence: f32,
) -> Vec<(String, usize)> {
    let Some(point) = as_of_point(as_of) else {
        return Vec::new();
    };
    g.traverse(&start.to_string(), max_depth, point, min_confidence, 0.0)
}

/// Read a node's community off an already-built graph (M14). `None` if the node
/// is in no alive edge. Same AS OF mapping as the other readers.
pub fn community_of(g: &TemporalGraph, node: &str, as_of: Option<Lsn>) -> Option<CommunityResult> {
    let a = g.analyze(as_of_point(as_of)?, 0.0);
    let community = a.community.get(node)?.clone();
    let members = a.members(&community);
    Some(CommunityResult {
        node: node.to_string(),
        community,
        members,
    })
}

/// Read a node's metrics off an already-built graph (M14).
pub fn node_metrics_of(g: &TemporalGraph, node: &str, as_of: Option<Lsn>) -> Option<MetricsResult> {
    let a = g.analyze(as_of_point(as_of)?, 0.0);
    let m = a.metrics.get(node)?;
    let community = a.community.get(node).cloned().unwrap_or_default();
    Some(MetricsResult {
        node: node.to_string(),
        community,
        degree: m.degree,
        centrality: m.centrality,
        anomaly_score: m.anomaly_score,
    })
}

/// Read the competing hypotheses of one edge off an already-built graph (M12).
/// `None` if the edge id was never asserted. AS OF mapping matches the others.
pub fn hypotheses_of(
    g: &TemporalGraph,
    from: &str,
    to: &str,
    etype: &str,
    as_of: Option<Lsn>,
) -> Option<EdgeHypotheses> {
    let point = as_of_point(as_of)?;
    let et = EdgeType::from_attr(etype);
    let edge_id = TemporalGraph::edge_id(from, to, &et);
    let edge = g.edges.get(&edge_id)?;
    let alive = edge.alive_at(point);
    let belief = g.belief_at(&edge_id, point);
    let versions = g
        .hypotheses_at(&edge_id, point)
        .into_iter()
        .map(|v| HypothesisRow {
            hypothesis_id: v.hypothesis_id,
            confidence: v.confidence,
            polarity: v.polarity,
            source: v.source,
        })
        .collect();
    Some(EdgeHypotheses {
        edge_id,
        alive,
        belief,
        versions,
    })
}

/// Read a relationship MATCH off an already-built graph (shared by every
/// backend; same AS OF mapping as `neighbors_of`/`traverse_of`).
pub fn match_edges_of(
    g: &TemporalGraph,
    src: Option<&str>,
    etype: Option<&str>,
    dst: Option<&str>,
    as_of: Option<Lsn>,
) -> Vec<EdgeRow> {
    let Some(point) = as_of_point(as_of) else {
        return Vec::new();
    };
    let et = etype.map(EdgeType::from_attr);
    g.match_edges(src, et.as_ref(), dst, point, 0.0)
        .into_iter()
        .map(|m| EdgeRow {
            edge_id: m.edge_id,
            from: m.from,
            to: m.to,
            etype: m.etype.key(),
            belief: m.belief,
        })
        .collect()
}

// ---- M16: counterfactual engine (SIMULATE) ----

/// Build the derived temporal graph a backend serves — the base for a
/// counterfactual overlay. Delegates to `QueryBackend::graph`, so a virtual
/// backend contributes its overlay (nested `SIMULATE` composes).
pub fn graph_snapshot(be: &dyn QueryBackend) -> Result<TemporalGraph, HeraclitusError> {
    be.graph()
}

/// Materialize a **virtual** graph = `base` with one edge added or removed
/// (M16). Returns a fresh graph; `base` (and therefore the log) is untouched —
/// the divergence is isolated to the returned copy.
pub fn materialize_virtual(
    base: &TemporalGraph,
    op: SimulateOp,
    from: &str,
    to: &str,
    etype: &str,
) -> TemporalGraph {
    let et = EdgeType::from_attr(etype);
    let edge_id = TemporalGraph::edge_id(from, to, &et);
    let remove = op == SimulateOp::RemoveEdge;

    let mut g = TemporalGraph::new();
    for (id, edge) in &base.edges {
        if remove && *id == edge_id {
            continue; // the counterfactual removal
        }
        g.upsert_edge(
            edge.clone(),
            base.versions.get(id).cloned().unwrap_or_default(),
        );
    }
    if op == SimulateOp::AddEdge {
        let version = EdgeVersion {
            hypothesis_id: edge_id.clone(),
            confidence: 1.0,
            source: "simulate".into(),
            provenance: vec![],
            polarity: et.polarity(),
            valid_from_lsn: 0,
        };
        g.upsert_edge(
            Edge {
                id: edge_id,
                from: from.to_string(),
                to: to.to_string(),
                etype: et,
                valid_from_lsn: 0,
                valid_to_lsn: None,
            },
            vec![version],
        );
    }
    g.watermark = base.watermark;
    g
}

/// A counterfactual view: graph reads hit a virtual overlay; everything else
/// (text, vector, entity resolution, the log itself) delegates to the real
/// backend. `append` is a no-op so a `SIMULATE ... THEN` query can never alter
/// the log — the whole point of M16.
pub struct VirtualBackend<'a> {
    base: &'a dyn QueryBackend,
    graph: TemporalGraph,
}

impl<'a> VirtualBackend<'a> {
    pub fn new(base: &'a dyn QueryBackend, graph: TemporalGraph) -> Self {
        Self { base, graph }
    }
}

impl QueryBackend for VirtualBackend<'_> {
    // --- delegated, unaffected by an edge counterfactual ---
    fn scan(&self, as_of: Option<Lsn>) -> Result<Vec<(Lsn, Episode)>, HeraclitusError> {
        self.base.scan(as_of)
    }
    fn scan_range(&self, from: Lsn, to: Lsn) -> Result<Vec<(Lsn, Episode)>, HeraclitusError> {
        self.base.scan_range(from, to)
    }
    fn recall(
        &self,
        t: &str,
        k: usize,
        a: Option<Lsn>,
    ) -> Result<Vec<(Lsn, Episode, f32)>, HeraclitusError> {
        self.base.recall(t, k, a)
    }
    fn nearest(
        &self,
        v: &[f32],
        k: usize,
        a: Option<Lsn>,
    ) -> Result<Vec<(Lsn, Episode, f32)>, HeraclitusError> {
        self.base.nearest(v, k, a)
    }
    fn provenance(&self, id: &str) -> Result<Vec<String>, HeraclitusError> {
        self.base.provenance(id)
    }
    fn lsn_for_timestamp(&self, ts: u64) -> Result<Lsn, HeraclitusError> {
        self.base.lsn_for_timestamp(ts)
    }
    fn resolve_entity(&self, key: &str, a: Option<Lsn>) -> Result<Option<String>, HeraclitusError> {
        self.base.resolve_entity(key, a)
    }
    fn entity_cluster(&self, id: &str, a: Option<Lsn>) -> Result<Vec<String>, HeraclitusError> {
        self.base.entity_cluster(id, a)
    }
    /// No-op: a counterfactual never writes to the real log.
    fn append(
        &self,
        _label: Option<&str>,
        _props: &[(String, Value)],
    ) -> Result<Lsn, HeraclitusError> {
        Ok(Lsn::MAX)
    }
    /// Expose the overlay so a nested SIMULATE materializes on top of it.
    fn graph(&self) -> Result<TemporalGraph, HeraclitusError> {
        Ok(self.graph.clone())
    }

    // --- graph reads served from the virtual overlay ---
    fn neighbors(
        &self,
        node: &str,
        etype: Option<&str>,
        a: Option<Lsn>,
        mc: f32,
    ) -> Result<Vec<NeighborRow>, HeraclitusError> {
        Ok(neighbors_of(&self.graph, node, etype, a, mc))
    }
    fn traverse(
        &self,
        start: &str,
        d: usize,
        a: Option<Lsn>,
        mc: f32,
    ) -> Result<Vec<(String, usize)>, HeraclitusError> {
        Ok(traverse_of(&self.graph, start, d, a, mc))
    }
    fn match_edges(
        &self,
        src: Option<&str>,
        et: Option<&str>,
        dst: Option<&str>,
        a: Option<Lsn>,
    ) -> Result<Vec<EdgeRow>, HeraclitusError> {
        Ok(match_edges_of(&self.graph, src, et, dst, a))
    }
    fn edge_hypotheses(
        &self,
        f: &str,
        t: &str,
        et: &str,
        a: Option<Lsn>,
    ) -> Result<Option<EdgeHypotheses>, HeraclitusError> {
        Ok(hypotheses_of(&self.graph, f, t, et, a))
    }
    fn community(
        &self,
        node: &str,
        a: Option<Lsn>,
    ) -> Result<Option<CommunityResult>, HeraclitusError> {
        Ok(community_of(&self.graph, node, a))
    }
    fn node_metrics(
        &self,
        node: &str,
        a: Option<Lsn>,
    ) -> Result<Option<MetricsResult>, HeraclitusError> {
        Ok(node_metrics_of(&self.graph, node, a))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pmap(pairs: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(k, ps)| (k.to_string(), ps.iter().map(|s| s.to_string()).collect()))
            .collect()
    }

    #[test]
    fn trace_dedups_diamond_to_min_depth() {
        // Diamond:  X <- a,b ;  a <- r ; b <- r ; r is the root cause.
        let parents = pmap(&[("X", &["a", "b"]), ("a", &["r"]), ("b", &["r"]), ("r", &[])]);
        let t = trace_causes(&parents, "X", 10);
        // Each node appears once, at its shortest depth.
        assert_eq!(t.steps.len(), 4);
        let depth = |id: &str| t.steps.iter().find(|s| s.id == id).unwrap().depth;
        assert_eq!(depth("X"), 0);
        assert_eq!(depth("a"), 1);
        assert_eq!(depth("b"), 1);
        assert_eq!(
            depth("r"),
            2,
            "shared ancestor deduped to its shortest depth"
        );
        // The single root cause is r.
        assert_eq!(t.roots, vec!["r"]);
        // Causes ARE the provenance pointers (the consistency contract).
        let x = t.steps.iter().find(|s| s.id == "X").unwrap();
        assert_eq!(x.causes, vec!["a", "b"]);
    }

    #[test]
    fn trace_respects_depth_budget() {
        let parents = pmap(&[("X", &["a"]), ("a", &["b"]), ("b", &["c"]), ("c", &[])]);
        // depth 1: X and its direct cause a; b/c not expanded.
        let t = trace_causes(&parents, "X", 1);
        let ids: Vec<&str> = t.steps.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, vec!["X", "a"]);
        // No root reached within the budget.
        assert!(t.roots.is_empty());
    }

    #[test]
    fn trace_unknown_target_is_empty() {
        let parents = pmap(&[("X", &[])]);
        let t = trace_causes(&parents, "ZZZ", 10);
        assert!(t.steps.is_empty() && t.roots.is_empty());
    }

    #[test]
    fn trace_root_target_is_its_own_cause() {
        let parents = pmap(&[("X", &[])]);
        let t = trace_causes(&parents, "X", 10);
        assert_eq!(t.steps.len(), 1);
        assert_eq!(t.roots, vec!["X"]);
    }
}
