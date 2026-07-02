//! The execution boundary. The query crate never owns indexes — it asks a
//! [`QueryBackend`]. The server composes the real backend (HNSW + BM25 +
//! activation + retrieval); [`LogBackend`] is the reference implementation
//! over the raw log: exact, lock-free on reads, and completely idempotent.

use crate::ast::{SimulateOp, Value};
use crate::fusion::{FusedHit, FusionInput, FusionWeights};
use heraclitus_core::{Episode, EventId, EventKind, HeraclitusError, Lsn};
use heraclitus_index_graph::adaptive::{self, LabeledFlag, PolicyEval};
use heraclitus_index_graph::decision::{self, DecisionPolicy};
use heraclitus_index_graph::temporal::{Edge, EdgeType, EdgeVersion, TemporalGraph};
use heraclitus_log::Log;
use std::collections::{BTreeMap, HashMap, BinaryHeap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::cmp::{Ordering, Reverse};
use arc_swap::ArcSwap;

// --- Estruturas Públicas de Linha e Resultados de Query (API M8-M17) ---

#[derive(Debug, Clone)]
pub struct NeighborRow {
    pub edge_id: String,
    pub to: String,
    pub etype: String,
    pub belief: f32,
    pub weight: f32,
    pub lsn: Lsn,
}

#[derive(Debug, Clone)]
pub struct EdgeRow {
    pub edge_id: String,
    pub from: String,
    pub to: String,
    pub etype: String,
    pub belief: f32,
}

#[derive(Debug, Clone)]
pub struct HypothesisRow {
    pub hypothesis_id: String,
    pub confidence: f32,
    pub polarity: f32,
    pub source: String,
}

#[derive(Debug, Clone)]
pub struct EdgeHypotheses {
    pub edge_id: String,
    pub alive: bool,
    pub belief: f32,
    pub versions: Vec<HypothesisRow>,
}

#[derive(Debug, Clone)]
pub struct AdaptReport {
    pub rule: String,
    pub samples: usize,
    pub learned_threshold: f32,
    pub default: PolicyEval,
    pub adapted: PolicyEval,
}

#[derive(Debug, Clone)]
pub struct ActionRecord {
    pub action_id: String,
    pub rule: String,
    pub subject: String,
    pub reason: String,
    pub lsn: Lsn,
}

#[derive(Debug, Clone, Default)]
pub struct DecisionReport {
    pub fired: Vec<ActionRecord>,
    pub skipped: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CommunityResult {
    pub node: String,
    pub community: String,
    pub members: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct MetricsResult {
    pub node: String,
    pub community: String,
    pub degree: u32,
    pub centrality: f32,
    pub anomaly_score: f32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CausalStep {
    pub id: String,
    pub depth: usize,
    pub causes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Trace {
    pub target: String,
    pub steps: Vec<CausalStep>,
    pub roots: Vec<String>,
}

pub const QUERY_SCAN_CAP: usize = 250_000;

// =========================================================================
// M29: ENTRADAS UNIFICADAS DE HEAP COM ORDENAÇÃO MÁXIMA/MÍNIMA EXPLICITA
// =========================================================================

#[derive(PartialEq)]
struct MinScoreEntry {
    score: f32,
    lsn: Lsn,
}
impl Eq for MinScoreEntry {}
impl Ord for MinScoreEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.score.total_cmp(&self.score)
            .then_with(|| other.lsn.cmp(&self.lsn))
    }
}
impl PartialOrd for MinScoreEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// =========================================================================
// M29: ENTITY RESOLVER HISTÓRICO COM DEDUP COMPACTO EM TEMPO DE EXECUÇÃO
// =========================================================================

#[derive(Clone, Default)]
pub struct EntityResolver {
    pub watermark: Lsn,
    pub mappings: HashMap<String, Arc<Vec<(Lsn, String)>>>,
    pub clusters: HashMap<String, Arc<Vec<(Lsn, String)>>>,
}

impl EntityResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_episode(&mut self, lsn: Lsn, ep: &Episode) {
        if let (Some(key), Some(entity_id)) = (ep.attrs.get("resolved_key"), ep.attrs.get("entity_id")) {
            // CoW parcial para mitigar alocações redundantes
            let map_vec = self.mappings.entry(key.clone()).or_default();
            Arc::make_mut(map_vec).push((lsn, entity_id.clone()));

            let cluster_vec = self.clusters.entry(entity_id.clone()).or_default();
            Arc::make_mut(cluster_vec).push((lsn, key.clone()));
        }
    }

    pub fn resolve(&self, key: &str, bound: Lsn) -> Option<String> {
        let history = self.mappings.get(key)?;
        let idx = match history.binary_search_by_key(&bound, |&(l, _)| l) {
            Ok(found) => found,
            Err(ins) => { if ins == 0 { return None; } ins - 1 }
        };
        history.get(idx).map(|(_, id)| id.clone())
    }

    pub fn cluster(&self, entity_id: &str, bound: Lsn) -> Vec<String> {
        let history = match self.clusters.get(entity_id) {
            Some(h) => h,
            None => return Vec::new(),
        };
        
        let idx = match history.binary_search_by_key(&bound, |&(l, _)| l) {
            Ok(found) => found + 1,
            Err(ins) => ins,
        };
        
        let mut seen = BTreeSet::new();
        let mut unique_keys = Vec::with_capacity(idx);
        for &(_, ref key) in &history[..idx] {
            if seen.insert(key) {
                unique_keys.push(key.clone());
            }
        }
        unique_keys
    }
}

// =========================================================================
// M29: VECTOR SEARCH ENGINE (HNSW SEGURO, HEURÍSTICA DE DIVERSIDADE E GRAU)
// =========================================================================

#[derive(Clone, Default)]
struct HnswIndex {
    pub nodes: HashMap<Lsn, Vec<f32>>,
    // Camadas usam Arc<Vec<Lsn>> para permitir Copy-On-Write ultra-veloz de ponteiros no sync
    pub layers: Vec<HashMap<Lsn, Arc<Vec<Lsn>>>>,
    pub entry_points: Vec<Lsn>, 
}

impl HnswIndex {
    fn compute_distance(&self, v1: &[f32], v2: &[f32]) -> f32 {
        let mut d_sq = 0.0f32;
        for i in 0..v1.len().min(v2.len()) {
            d_sq += (v1[i] - v2[i]).powi(2);
        }
        d_sq.sqrt()
    }

    fn pick_random_layer(&self, lsn: Lsn) -> usize {
        let mut seed = lsn.wrapping_mul(0xbf58476d1ce4e5b9);
        seed = (seed ^ (seed >> 30)).wrapping_mul(0x94d049bb133111eb);
        let u = (((seed & 0xFFFFFFFF) as f64) / (u32::MAX as f64)).max(1e-10);
        let m_l = 1.0 / (12.0f64.ln());
        let level = (-u.ln() * m_l).floor() as usize;
        level.min(4) 
    }

    /// Implementação de seleção heurística com base em caminhos alternativos do HNSW original
    /// Impede o colapso do grafo ANN em cliques isolados e limita a densidade local controlando $O(M \log M)$
    fn select_neighbors_heuristic(&self, base_vec: &[f32], mut candidates: Vec<Lsn>, m: usize) -> Vec<Lsn> {
        if candidates.len() <= m { return candidates; }
        
        let mut result = Vec::with_capacity(m);
        // Ordena candidatos por proximidade absoluta ao nó inserido
        candidates.sort_by(|a, b| {
            let d1 = self.nodes.get(a).map(|v| self.compute_distance(base_vec, v)).unwrap_or(f32::MAX);
            let d2 = self.nodes.get(b).map(|v| self.compute_distance(base_vec, v)).unwrap_or(f32::MAX);
            d1.total_cmp(&d2)
        });

        for c in candidates {
            if result.len() >= m { break; }
            let c_vec = match self.nodes.get(&c) { Some(v) => v, None => continue };
            let d_to_base = self.compute_distance(base_vec, c_vec);
            
            let mut keep = true;
            for r in &result {
                if let Some(r_vec) = self.nodes.get(r) {
                    let d_to_r = self.compute_distance(c_vec, r_vec);
                    // Se o candidato está mais perto de um vizinho já selecionado do que do nó base, descarta para manter diversidade espacial
                    if d_to_r < d_to_base {
                        keep = false;
                        break;
                    }
                }
            }
            if keep { result.push(c); }
        }
        result
    }

    pub fn search_layer_greedy(&self, query: &[f32], bound: Lsn) -> Option<Lsn> {
        if self.entry_points.is_empty() { return None; }
        
        let mut current_node = None;
        for &ep in self.entry_points.iter().rev() {
            if ep < bound {
                // Invariante de Camada M29: Verifica se o nó realmente existe no mapa da camada correspondente
                let layer_idx = self.entry_points.iter().position(|&x| x == ep).unwrap_or(0);
                if self.layers.get(layer_idx).map_or(false, |l| l.contains_key(&ep)) {
                    current_node = Some(ep);
                    break;
                }
            }
        }

        let mut current_node = match current_node {
            Some(node) => node,
            None => {
                let mut fallback = None;
                let mut min_dist = f32::MAX;
                for (&lsn, vec) in &self.nodes {
                    if lsn < bound {
                        let d = self.compute_distance(query, vec);
                        if d < min_dist {
                            min_dist = d;
                            fallback = Some(lsn);
                        }
                    }
                }
                return fallback;
            }
        };

        let start_layer = self.layers.len().saturating_sub(1);
        for layer_idx in (0..=start_layer).rev() {
            let mut changed = true;
            while changed {
                changed = false;
                if let Some(neighbors) = self.layers.get(layer_idx).and_then(|l| l.get(&current_node)) {
                    if let Some(current_vec) = self.nodes.get(&current_node) {
                        let mut current_dist = self.compute_distance(query, current_vec);
                        for &neighbor in neighbors.iter() {
                            if neighbor >= bound { continue; }
                            if let Some(neighbor_vec) = self.nodes.get(&neighbor) {
                                let dist = self.compute_distance(query, neighbor_vec);
                                if dist < current_dist {
                                    current_dist = dist;
                                    current_node = neighbor;
                                    changed = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        Some(current_node)
    }

    pub fn insert(&mut self, lsn: Lsn, vector: Vec<f32>) {
        self.nodes.insert(lsn, vector.clone());
        let target_layer = self.pick_random_layer(lsn);
        
        while self.layers.len() <= target_layer {
            self.layers.push(HashMap::new());
        }
        while self.entry_points.len() <= target_layer {
            self.entry_points.push(lsn);
        }

        if self.nodes.len() == 1 {
            for layer_idx in 0..=target_layer {
                self.layers[layer_idx].insert(lsn, Arc::new(Vec::new()));
                self.entry_points[layer_idx] = lsn;
            }
            return;
        }

        const M: usize = 12;
        let mut current_entry = self.entry_points[self.layers.len() - 1];
        let start_layer = self.layers.len() - 1;
        
        for layer_idx in (target_layer + 1..=start_layer).rev() {
            let mut changed = true;
            while changed {
                changed = false;
                if let Some(neighbors) = self.layers.get(layer_idx).and_then(|l| l.get(&current_entry)) {
                    if let Some(entry_vec) = self.nodes.get(&current_entry) {
                        let mut current_dist = self.compute_distance(&vector, entry_vec);
                        for &neighbor in neighbors.iter() {
                            if neighbor < lsn {
                                if let Some(n_vec) = self.nodes.get(&neighbor) {
                                    let dist = self.compute_distance(&vector, n_vec);
                                    if dist < current_dist {
                                        current_dist = dist;
                                        current_entry = neighbor;
                                        changed = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut current_node = current_entry;
        for layer_idx in (0..=target_layer).rev() {
            self.layers[layer_idx].insert(lsn, Arc::new(Vec::new()));
            
            let mut changed = true;
            while changed {
                changed = false;
                if let Some(neighbors) = self.layers.get(layer_idx).and_then(|l| l.get(&current_node)) {
                    if let Some(node_vec) = self.nodes.get(&current_node) {
                        let mut current_dist = self.compute_distance(&vector, node_vec);
                        for &neighbor in neighbors.iter() {
                            if neighbor < lsn {
                                if let Some(n_vec) = self.nodes.get(&neighbor) {
                                    let dist = self.compute_distance(&vector, n_vec);
                                    if dist < current_dist {
                                        current_node = neighbor;
                                        changed = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            let mut candidates = vec![current_node];
            if let Some(neighbors) = self.layers.get(layer_idx).and_then(|l| l.get(&current_node)) {
                candidates.extend(neighbors.iter().cloned().filter(|&n| n < lsn));
            }
            
            // Substituição do truncate burro pela poda de diversidade espacial estruturada M29
            let selected = self.select_neighbors_heuristic(&vector, candidates, M);

            for neighbor in selected {
                // Fase 1: muta self.layers e decide se é preciso podar (liberta o
                // &mut self.layers no fim do bloco). Fase 2 usa &self (nodes +
                // select_neighbors_heuristic) sem conflito de borrow.
                let pool_for_prune = if let Some(layer_map) = self.layers.get_mut(layer_idx) {
                    let node_links = layer_map.get_mut(&lsn).unwrap();
                    Arc::make_mut(node_links).push(neighbor);

                    let links = layer_map.entry(neighbor).or_default();
                    if !links.contains(&lsn) {
                        Arc::make_mut(links).push(lsn);
                    }
                    if links.len() > M {
                        let mut pool = links.to_vec();
                        pool.push(lsn);
                        Some(pool)
                    } else {
                        None
                    }
                } else {
                    None
                };

                if let Some(pool) = pool_for_prune {
                    if let Some(n_vec) = self.nodes.get(&neighbor) {
                        let pruned = self.select_neighbors_heuristic(n_vec, pool, M);
                        if let Some(layer_map) = self.layers.get_mut(layer_idx) {
                            layer_map.insert(neighbor, Arc::new(pruned));
                        }
                    }
                }
            }
        }

        // Enforcement formal de pontos de entrada estáveis e obrigatoriamente existentes na camada
        for layer_idx in 0..=target_layer {
            if let Some(layer_map) = self.layers.get(layer_idx) {
                if !layer_map.contains_key(&self.entry_points[layer_idx]) {
                    self.entry_points[layer_idx] = lsn;
                }
            }
        }
    }
}

// =========================================================================
// M29: SUBSISTEMA TEXTUAL (LSM-LIKE STRUCTURAL ARCS PARA EVITAR AMPLIFICAÇÃO)
// =========================================================================

struct PostingCursor<'a> {
    slice: &'a [(Lsn, f32)],
    cursor: usize,
    idf: f32,
    max_term_score: f32,
}

impl<'a> PostingCursor<'a> {
    fn new(slice: &'a [(Lsn, f32)], idf: f32) -> Self {
        let max_tf = slice.iter().map(|&(_, tf)| tf).fold(0.0f32, |m, v| m.max(v));
        Self {
            slice,
            cursor: 0,
            idf,
            max_term_score: max_tf * idf,
        }
    }

    #[inline(always)]
    fn current_lsn(&self) -> Option<Lsn> {
        self.slice.get(self.cursor).map(|&(lsn, _)| lsn)
    }

    #[inline(always)]
    fn current_tf(&self) -> f32 {
        self.slice.get(self.cursor).map(|&(_, tf)| tf).unwrap_or(0.0f32)
    }

    #[inline(always)]
    fn advance_to(&mut self, target: Lsn) {
        while self.cursor < self.slice.len() && self.slice[self.cursor].0 < target {
            self.cursor += 1;
        }
    }
}

// Planos de índices totalmente desacoplados de forma isolada na raiz
#[derive(Clone, Default)]
struct TextInvertedIndex {
    pub inverted_text: HashMap<String, Arc<Vec<(Lsn, f32)>>>,
    pub total_docs: usize,
}

#[derive(Clone, Default)]
struct AttributeIndex {
    pub attributes: HashMap<String, HashMap<String, Arc<Vec<Lsn>>>>,
}

// =========================================================================
// M29: TRAIT DE CONSULTA UNIFICADO COM RESOLUÇÃO DINÂMICA DE SENTINELAS
// =========================================================================

pub trait QueryBackend {
    fn scan(&self, as_of: Option<Lsn>) -> Result<Vec<(Lsn, Episode)>, HeraclitusError>;
    fn scan_range(&self, from: Lsn, to: Lsn) -> Result<Vec<(Lsn, Episode)>, HeraclitusError>;
    fn head(&self) -> Result<Lsn, HeraclitusError>;
    fn graph(&self) -> Result<TemporalGraph, HeraclitusError>;
    fn append(&self, label: Option<&str>, props: &[(String, Value)]) -> Result<Lsn, HeraclitusError>;
    
    fn attr_lookup(&self, field: &str, value: &str, as_of: Option<Lsn>) -> Result<Option<Vec<(Lsn, Episode)>>, HeraclitusError>;
    fn recall(&self, text: &str, k: usize, as_of: Option<Lsn>) -> Result<Vec<(Lsn, Episode, f32)>, HeraclitusError>;
    fn nearest(&self, vector: &[f32], k: usize, as_of: Option<Lsn>) -> Result<Vec<(Lsn, Episode, f32)>, HeraclitusError>;
    
    fn provenance(&self, id: &str) -> Result<Vec<String>, HeraclitusError>;
    fn neighbors(&self, node: &str, etype: Option<&str>, as_of: Option<Lsn>, min_confidence: f32) -> Result<Vec<NeighborRow>, HeraclitusError>;
    fn traverse(&self, start: &str, max_depth: usize, as_of: Option<Lsn>, min_confidence: f32) -> Result<Vec<(String, usize)>, HeraclitusError>;
    fn match_edges(&self, src: Option<&str>, etype: Option<&str>, dst: Option<&str>, as_of: Option<Lsn>) -> Result<Vec<EdgeRow>, HeraclitusError>;
    fn community(&self, node: &str, as_of: Option<Lsn>) -> Result<Option<CommunityResult>, HeraclitusError>;
    fn node_metrics(&self, node: &str, as_of: Option<Lsn>) -> Result<Option<MetricsResult>, HeraclitusError>;
    fn edge_hypotheses(&self, from: &str, to: &str, etype: &str, as_of: Option<Lsn>) -> Result<Option<EdgeHypotheses>, HeraclitusError>;
    fn lsn_for_timestamp(&self, ts_ms: u64) -> Result<Lsn, HeraclitusError>;
    fn resolve_entity(&self, key: &str, as_of: Option<Lsn>) -> Result<Option<String>, HeraclitusError>;
    fn entity_cluster(&self, entity_id: &str, as_of: Option<Lsn>) -> Result<Vec<String>, HeraclitusError>;

    fn why(&self, target: &str, max_depth: usize, as_of: Option<Lsn>) -> Result<Trace, HeraclitusError> {
        let bound = self.resolve_as_of_bound(as_of)?;
        let events = self.scan_range(0, bound)?;
        let present: BTreeSet<String> = events.iter().map(|(_, e)| e.id.to_string()).collect();
        let mut parents: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (_, e) in &events {
            let ps: Vec<String> = e.parents.iter().map(|p| p.to_string()).filter(|p| present.contains(p)).collect();
            parents.insert(e.id.to_string(), ps);
        }
        Ok(trace_causes(&parents, target, max_depth))
    }

    fn decide(&self, policy: DecisionPolicy, as_of: Option<Lsn>) -> Result<DecisionReport, HeraclitusError> {
        let bound = self.resolve_as_of_bound(as_of)?;
        let mut g = TemporalGraph::new();
        let mut existing = BTreeSet::new();
        
        let snapshot_events = self.scan_range(0, bound)?;
        for (lsn, e) in snapshot_events {
            g.apply_episode(lsn, &e);
            if e.kind == EventKind::Action {
                if let Some(act_id) = e.attrs.get("action_id") {
                    existing.insert(act_id.clone());
                }
            }
        }
        let decisions = decision::evaluate(&g, u64::MAX, &policy);

        let mut report = DecisionReport::default();
        for d in decisions {
            if existing.contains(&d.action_id) {
                report.skipped.push(d.action_id);
                continue;
            }
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

    fn adapt(&self, as_of: Option<Lsn>) -> Result<AdaptReport, HeraclitusError> {
        let bound = self.resolve_as_of_bound(as_of)?;
        let rule = "flag_anomaly";
        let samples: Vec<LabeledFlag> = self
            .scan_range(0, bound)?
            .into_iter()
            .filter(|(_, e)| e.attrs.get("feedback_rule").map(|r| r.as_str()) == Some(rule))
            .filter_map(|(_, e)| {
                let score = e.attrs.get("score")?.parse::<f32>().ok()?;
                let confirmed = e.attrs.get("verdict").map(|v| v == "confirm").unwrap_or(false);
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

    fn find_fused(&self, text: &str, vector: &[f32], connected_to: &str, weights: FusionWeights, k: usize, as_of: Option<Lsn>) -> Result<Vec<FusedHit>, HeraclitusError> {
        let fetch = (k * 4).max(8);
        let head_lsn = self.head()?;

        let mut graph_ch: Vec<(String, u64, f32)> = self
            .neighbors(connected_to, None, as_of, 0.0)?
            .into_iter()
            .map(|n| (n.to, n.lsn, n.belief))
            .collect();
        let mut vec_ch: Vec<(String, u64, f32)> = self
            .nearest(vector, fetch, as_of)?
            .into_iter()
            .map(|(l, e, d)| (e.id.to_string(), l, 1.0 / (1.0 + d)))
            .collect();
        let mut txt_ch: Vec<(String, u64, f32)> = self
            .recall(text, fetch, as_of)?
            .into_iter()
            .map(|(l, e, s)| (e.id.to_string(), l, s))
            .collect();

        fold_normalization(&mut graph_ch);
        fold_normalization(&mut vec_ch);
        fold_normalization(&mut txt_ch);

        let mut rows: HashMap<String, (u64, FusionInput)> = HashMap::with_capacity(fetch * 3);
        for (id, lsn, sc) in graph_ch {
            let entry = rows.entry(id).or_insert((0, FusionInput::default()));
            if lsn > entry.0 { entry.0 = lsn; }
            entry.1.graph_score = sc;
        }
        for (id, lsn, sc) in vec_ch {
            let entry = rows.entry(id).or_insert((0, FusionInput::default()));
            if lsn > entry.0 { entry.0 = lsn; }
            entry.1.vector_score = sc;
        }
        for (id, lsn, sc) in txt_ch {
            let entry = rows.entry(id).or_insert((0, FusionInput::default()));
            if lsn > entry.0 { entry.0 = lsn; }
            entry.1.text_score = sc;
        }

        let lambda = 0.0005f32;
        let mut hits: Vec<FusedHit> = rows
            .into_iter()
            .map(|(id, (lsn, input))| {
                let base_score = weights.fuse(&input);
                let delta = head_lsn.saturating_sub(lsn) as f32;
                let temporal_bias = (-lambda * delta).exp();
                FusedHit {
                    id,
                    lsn,
                    input,
                    score: base_score * temporal_bias,
                }
            })
            .collect();

        hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.id.cmp(&b.id)));
        hits.truncate(k);
        Ok(hits)
    }

    fn resolve_as_of_bound(&self, as_of: Option<Lsn>) -> Result<Lsn, HeraclitusError> {
        match as_of {
            Some(explicit_lsn) => Ok(explicit_lsn),
            None => self.head()
        }
    }
}

// =========================================================================
// M29: SNAPSHOT BUNDLE UTILIZANDO COw PARCIAL (ELIMINA CLONE $O(N+E)$ SPICES)
// =========================================================================

struct SnapshotBundle {
    lsn: Lsn,
    graph: Arc<TemporalGraph>,
    resolver: Arc<EntityResolver>,
    text_index: Arc<TextInvertedIndex>,
    attr_index: Arc<AttributeIndex>,
    vector_index: Arc<HnswIndex>,
}

pub struct LogBackend {
    log: Arc<Log>,
    bundle: ArcSwap<SnapshotBundle>,
    sync_mutex: Mutex<()>,
}

impl LogBackend {
    pub fn new(log: Arc<Log>) -> Self {
        let initial_bundle = SnapshotBundle {
            lsn: 0,
            graph: Arc::new(TemporalGraph::new()),
            resolver: Arc::new(EntityResolver::new()),
            text_index: Arc::new(TextInvertedIndex::default()),
            attr_index: Arc::new(AttributeIndex::default()),
            vector_index: Arc::new(HnswIndex::default()),
        };
        Self {
            log,
            bundle: ArcSwap::from_pointee(initial_bundle),
            sync_mutex: Mutex::new(()),
        }
    }

    fn sync_bundle(&self) -> Result<Arc<SnapshotBundle>, HeraclitusError> {
        // Captura monotônica blindada do HEAD físico real antes da janela de processamento
        let pinned_head = self.log.head();
        let current_bundle = self.bundle.load();
        
        if current_bundle.lsn >= pinned_head {
            return Ok(self.bundle.load_full());
        }

        let _guard = self.sync_mutex.lock().unwrap();
        
        let current_bundle = self.bundle.load();
        if current_bundle.lsn >= pinned_head {
            return Ok(self.bundle.load_full());
        }

        let start_lsn = current_bundle.lsn;
        
        // CORRIGIDO: Eliminação da re-materialização profunda $O(N+E)$.
        // Copiamos os top-level maps por ponteiro imutável compartilhando as sub-listas internas via Arc.
        let mut updated_graph = (*current_bundle.graph).clone(); 
        let mut updated_resolver = (*current_bundle.resolver).clone();
        let mut updated_text = (*current_bundle.text_index).clone();
        let mut updated_attr = (*current_bundle.attr_index).clone();
        let mut updated_vector = (*current_bundle.vector_index).clone();

        let delta = self.log.scan_capped(start_lsn, pinned_head + 1, usize::MAX)?;
        for (lsn, ep) in delta {
            if lsn >= start_lsn && lsn <= pinned_head {
                updated_graph.apply_episode(lsn, &ep);
                updated_resolver.apply_episode(lsn, &ep);
                
                // Ingestão incremental com CoW granular
                updated_text.total_docs += 1;
                let content_str = String::from_utf8_lossy(&ep.content).to_lowercase();
                let mut local_frequencies: HashMap<String, f32> = HashMap::new();
                let mut total_tokens = 0usize;

                for token in content_str.split(|c: char| !c.is_alphanumeric()).filter(|s| !s.is_empty()) {
                    *local_frequencies.entry(token.to_string()).or_insert(0.0f32) += 1.0f32;
                    total_tokens += 1;
                }

                if total_tokens > 0 {
                    for (token, raw_count) in local_frequencies {
                        let precomputed_tf = raw_count / (total_tokens as f32);
                        let postings = updated_text.inverted_text.entry(token).or_default();
                        Arc::make_mut(postings).push((lsn, precomputed_tf));
                    }
                }

                for (field, value) in &ep.attrs {
                    let postings = updated_attr.attributes.entry(field.clone()).or_default().entry(value.clone()).or_default();
                    Arc::make_mut(postings).push(lsn);
                }

                if let Some(emb) = &ep.embedding {
                    let mut flat_vector = Vec::with_capacity(emb.hyp.len() + emb.sph.len() + emb.euc.len());
                    flat_vector.extend(&emb.hyp);
                    flat_vector.extend(&emb.sph);
                    flat_vector.extend(&emb.euc);
                    updated_vector.insert(lsn, flat_vector);
                }
            }
        }
        
        updated_resolver.watermark = pinned_head;

        let new_bundle = SnapshotBundle {
            lsn: pinned_head + 1,
            graph: Arc::new(updated_graph),
            resolver: Arc::new(updated_resolver),
            text_index: Arc::new(updated_text),
            attr_index: Arc::new(updated_attr),
            vector_index: Arc::new(updated_vector),
        };

        self.bundle.store(Arc::new(new_bundle));
        Ok(self.bundle.load_full())
    }
}

impl QueryBackend for LogBackend {
    fn scan(&self, as_of: Option<Lsn>) -> Result<Vec<(Lsn, Episode)>, HeraclitusError> {
        let bound = self.resolve_as_of_bound(as_of)?;
        self.log.scan_capped(0, bound, QUERY_SCAN_CAP)
    }

    fn scan_range(&self, from: Lsn, to: Lsn) -> Result<Vec<(Lsn, Episode)>, HeraclitusError> {
        self.log.scan_capped(from, to, QUERY_SCAN_CAP)
    }

    fn head(&self) -> Result<Lsn, HeraclitusError> {
        Ok(self.log.head())
    }

    fn graph(&self) -> Result<TemporalGraph, HeraclitusError> {
        Ok((*self.sync_bundle()?.graph).clone())
    }

    fn attr_lookup(&self, field: &str, value: &str, as_of: Option<Lsn>) -> Result<Option<Vec<(Lsn, Episode)>>, HeraclitusError> {
        let b = self.sync_bundle()?;
        let bound = self.resolve_as_of_bound(as_of)?;

        let Some(values_map) = b.attr_index.attributes.get(field) else { return Ok(None); };
        let Some(postings) = values_map.get(value) else { return Ok(None); };

        let idx = match postings.binary_search(&bound) {
            Ok(found_idx) => found_idx,
            Err(insert_idx) => insert_idx,
        };

        let target_lsns = &postings[..idx];
        if target_lsns.is_empty() { return Ok(None); }

        let mut results = Vec::with_capacity(target_lsns.len());
        for &lsn in target_lsns {
            if let Some((_, ep)) = self.log.read(lsn)? {
                results.push((lsn, ep));
            }
        }
        Ok(Some(results))
    }

    /// CORRIGIDO: WAND determinístico sem reorder global $O(k \log k)$ e com garantia de avanço monotônico livre de skips.
    fn recall(&self, text: &str, k: usize, as_of: Option<Lsn>) -> Result<Vec<(Lsn, Episode, f32)>, HeraclitusError> {
        let b = self.sync_bundle()?;
        let bound = self.resolve_as_of_bound(as_of)?;
        let tokens: Vec<String> = text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        if tokens.is_empty() { return Ok(Vec::new()); }

        let total_docs = b.text_index.total_docs.max(1);
        let mut cursors = Vec::with_capacity(tokens.len());

        for token in &tokens {
            if let Some(postings) = b.text_index.inverted_text.get(token) {
                let idx = match postings.binary_search_by_key(&bound, |&(l, _)| l) {
                    Ok(found) => found,
                    Err(ins) => ins,
                };
                let active_slice = &postings[..idx];
                if !active_slice.is_empty() {
                    let df = active_slice.len() as f32;
                    let idf = (1.0 + ((total_docs as f32 - df + 0.5) / (df + 0.5))).ln().max(0.0001f32);
                    cursors.push(PostingCursor::new(active_slice, idf));
                }
            }
        }

        if cursors.is_empty() { return Ok(Vec::new()); }

        let mut top_k_heap: BinaryHeap<MinScoreEntry> = BinaryHeap::with_capacity(k);
        let mut score_accumulator: HashMap<Lsn, f32> = HashMap::new();
        let mut current_threshold = 0.0f32;

        loop {
            // Varredura incremental linear indexada para achar o menor LSN corrente sem quebrar a cache com sort total
            let mut min_lsn = None;
            for c in &cursors {
                if let Some(l) = c.current_lsn() {
                    min_lsn = Some(min_lsn.map_or(l, |m| std::cmp::min(m, l)));
                }
            }

            let Some(pivot_lsn) = min_lsn else { break; };

            let mut accumulated_upper_bound = 0.0f32;
            let mut met_threshold = false;

            for cursor in &cursors {
                if cursor.current_lsn().is_some() {
                    accumulated_upper_bound += cursor.max_term_score;
                    if accumulated_upper_bound >= current_threshold {
                        met_threshold = true;
                        break;
                    }
                }
            }

            if !met_threshold { break; }

            // Avaliação e avanço determinístico sincronizado: Garante progresso real descartando loops infinitos
            let mut total_score = 0.0f32;
            let mut matched = false;

            for cursor in cursors.iter_mut() {
                if cursor.current_lsn() == Some(pivot_lsn) {
                    total_score += cursor.current_tf() * cursor.idf;
                    cursor.cursor += 1;
                    matched = true;
                }
            }

            if matched {
                score_accumulator.insert(pivot_lsn, total_score);
                let entry = MinScoreEntry { score: total_score, lsn: pivot_lsn };
                
                if top_k_heap.len() < k {
                    top_k_heap.push(entry);
                } else if total_score > top_k_heap.peek().unwrap().score {
                    top_k_heap.pop();
                    top_k_heap.push(entry);
                }
                current_threshold = if top_k_heap.len() >= k { top_k_heap.peek().unwrap().score } else { 0.0f32 };
            } else {
                // Força o alinhamento monotônico de cursors atrasados para o próximo bloco válido
                for cursor in cursors.iter_mut() {
                    cursor.advance_to(pivot_lsn + 1);
                }
            }
        }

        let mut candidates: Vec<(Lsn, f32)> = score_accumulator.into_iter().collect();
        candidates.sort_by(|a, b| b.1.total_cmp(&a.1).then(b.0.cmp(&a.0)));
        candidates.truncate(k);

        let mut final_hits = Vec::with_capacity(candidates.len());
        for (lsn, score) in candidates {
            if let Some((_, e)) = self.log.read(lsn)? {
                final_hits.push((lsn, e, score));
            }
        }
        Ok(final_hits)
    }

    /// K-NN CORRIGIDO: Utilização estrita da estrutura MinScoreEntry unificada mitigando drifts de vizinhança
    fn nearest(&self, vector: &[f32], k: usize, as_of: Option<Lsn>) -> Result<Vec<(Lsn, Episode, f32)>, HeraclitusError> {
        let b = self.sync_bundle()?;
        let bound = self.resolve_as_of_bound(as_of)?;
        
        let Some(best_routing_node) = b.vector_index.search_layer_greedy(vector, bound) else {
            return Ok(Vec::new());
        };

        let mut visited = BTreeSet::new();
        let mut candidate_heap: BinaryHeap<MinScoreEntry> = BinaryHeap::new();
        let mut top_hits_heap: BinaryHeap<MinScoreEntry> = BinaryHeap::new();

        if let Some(first_vec) = b.vector_index.nodes.get(&best_routing_node) {
            let initial_dist = b.vector_index.compute_distance(vector, first_vec);
            candidate_heap.push(MinScoreEntry { score: initial_dist, lsn: best_routing_node });
            visited.insert(best_routing_node);
        }

        while let Some(MinScoreEntry { score: curr_dist, lsn: current_node }) = candidate_heap.pop() {
            let limit_dist = if top_hits_heap.len() >= k { top_hits_heap.peek().unwrap().score } else { f32::MAX };
            if curr_dist > limit_dist { break; }

            if let Some(neighbors) = b.vector_index.layers.get(0).and_then(|l| l.get(&current_node)) {
                for &neighbor in neighbors.iter() {
                    if neighbor >= bound { continue; }
                    if visited.insert(neighbor) {
                        if let Some(n_vec) = b.vector_index.nodes.get(&neighbor) {
                            let dist = b.vector_index.compute_distance(vector, n_vec);
                            let entry = MinScoreEntry { score: dist, lsn: neighbor };
                            
                            if top_hits_heap.len() < k {
                                top_hits_heap.push(entry);
                                candidate_heap.push(MinScoreEntry { score: dist, lsn: neighbor });
                            } else if dist < top_hits_heap.peek().unwrap().score {
                                top_hits_heap.pop();
                                top_hits_heap.push(entry);
                                candidate_heap.push(MinScoreEntry { score: dist, lsn: neighbor });
                            }
                        }
                    }
                }
            }
        }

        let mut raw_hits: Vec<(Lsn, f32)> = top_hits_heap.into_iter().map(|MinScoreEntry { score, lsn }| (lsn, score)).collect();
        raw_hits.sort_by(|a, b| a.1.total_cmp(&b.1));

        let mut final_hydrated_hits = Vec::with_capacity(raw_hits.len());
        for (lsn, dist) in raw_hits {
            if let Some((_, e)) = self.log.read(lsn)? {
                final_hydrated_hits.push((lsn, e, dist));
            }
        }
        Ok(final_hydrated_hits)
    }

    fn provenance(&self, id: &str) -> Result<Vec<String>, HeraclitusError> {
        let head = self.log.head();
        let mut chunk_iter = LogChunkIterator::new(self.log.clone(), 0, head);
        while let Some((_, e)) = chunk_iter.next_item()? {
            if e.id.to_string() == id {
                return Ok(e.parents.iter().map(|p| p.to_string()).collect());
            }
        }
        Ok(Vec::new())
    }

    fn neighbors(&self, node: &str, etype: Option<&str>, as_of: Option<Lsn>, min_confidence: f32) -> Result<Vec<NeighborRow>, HeraclitusError> {
        let b = self.sync_bundle()?;
        let bound = self.resolve_as_of_bound(as_of)?;
        let et = etype.map(EdgeType::from_attr);
        Ok(b.graph.neighbors(&node.to_string(), et.as_ref(), bound, min_confidence, 0.0)
            .into_iter()
            .map(|n| NeighborRow {
                edge_id: n.edge_id, to: n.to, etype: n.etype.key(),
                belief: n.belief, weight: n.weight, lsn: n.lsn,
            })
            .collect())
    }

    fn traverse(&self, start: &str, max_depth: usize, as_of: Option<Lsn>, min_confidence: f32) -> Result<Vec<(String, usize)>, HeraclitusError> {
        let b = self.sync_bundle()?;
        let bound = self.resolve_as_of_bound(as_of)?;
        Ok(b.graph.traverse(&start.to_string(), max_depth, bound, min_confidence, 0.0))
    }

    fn match_edges(&self, src: Option<&str>, etype: Option<&str>, dst: Option<&str>, as_of: Option<Lsn>) -> Result<Vec<EdgeRow>, HeraclitusError> {
        let b = self.sync_bundle()?;
        let bound = self.resolve_as_of_bound(as_of)?;
        let et = etype.map(EdgeType::from_attr);
        Ok(b.graph.match_edges(src, et.as_ref(), dst, bound, 0.0)
            .into_iter()
            .map(|m| EdgeRow {
                edge_id: m.edge_id, from: m.from, to: m.to, etype: m.etype.key(), belief: m.belief,
            })
            .collect())
    }

    fn edge_hypotheses(&self, from: &str, to: &str, etype: &str, as_of: Option<Lsn>) -> Result<Option<EdgeHypotheses>, HeraclitusError> {
        let b = self.sync_bundle()?;
        let bound = self.resolve_as_of_bound(as_of)?;
        let et = EdgeType::from_attr(etype);
        let edge_id = TemporalGraph::edge_id(from, to, &et);
        let edge = match b.graph.edges.get(&edge_id) {
            Some(e) => e,
            None => return Ok(None)
        };
        let alive = edge.alive_at(bound);
        let belief = b.graph.belief_at(&edge_id, bound);
        let versions = b.graph.hypotheses_at(&edge_id, bound)
            .into_iter()
            .map(|v| HypothesisRow {
                hypothesis_id: v.hypothesis_id, confidence: v.confidence, polarity: v.polarity, source: v.source,
            })
            .collect();
        Ok(Some(EdgeHypotheses { edge_id, alive, belief, versions }))
    }

    fn community(&self, node: &str, as_of: Option<Lsn>) -> Result<Option<CommunityResult>, HeraclitusError> {
        let b = self.sync_bundle()?;
        let bound = self.resolve_as_of_bound(as_of)?;
        let a = b.graph.analyze(bound, 0.0);
        let community = match a.community.get(node) {
            Some(c) => c.clone(),
            None => return Ok(None)
        };
        let members = a.members(&community);
        Ok(Some(CommunityResult { node: node.to_string(), community, members }))
    }

    fn node_metrics(&self, node: &str, as_of: Option<Lsn>) -> Result<Option<MetricsResult>, HeraclitusError> {
        let b = self.sync_bundle()?;
        let bound = self.resolve_as_of_bound(as_of)?;
        let a = b.graph.analyze(bound, 0.0);
        let m = match a.metrics.get(node) {
            Some(metrics) => metrics,
            None => return Ok(None)
        };
        let community = a.community.get(node).cloned().unwrap_or_default();
        Ok(Some(MetricsResult {
            node: node.to_string(), community,
            degree: m.degree, centrality: m.centrality, anomaly_score: m.anomaly_score,
        }))
    }

    fn resolve_entity(&self, key: &str, as_of: Option<Lsn>) -> Result<Option<String>, HeraclitusError> {
        let b = self.sync_bundle()?;
        let bound = self.resolve_as_of_bound(as_of)?;
        Ok(b.resolver.resolve(key, bound))
    }

    fn entity_cluster(&self, entity_id: &str, as_of: Option<Lsn>) -> Result<Vec<String>, HeraclitusError> {
        let b = self.sync_bundle()?;
        let bound = self.resolve_as_of_bound(as_of)?;
        Ok(b.resolver.cluster(entity_id, bound))
    }

    fn lsn_for_timestamp(&self, ts_ms: u64) -> Result<Lsn, HeraclitusError> {
        let head = self.log.head();
        let mut low = 0; let mut high = head; let mut ans = head;
        while low <= high {
            let mid = low + (high - low) / 2;
            match self.log.read(mid)? {
                Some((_, e)) => {
                    let e_ts = e.ts_hlc >> 16;
                    if e_ts > ts_ms {
                        ans = mid; if mid == 0 { break; }
                        high = mid - 1;
                    } else {
                        low = mid + 1;
                    }
                }
                None => { if mid == 0 { break; } high = mid - 1; }
            }
        }
        Ok(ans)
    }

    fn append(&self, label: Option<&str>, props: &[(String, Value)]) -> Result<Lsn, HeraclitusError> {
        let kind = match label {
            Some(l) if l.eq_ignore_ascii_case("action") => EventKind::Action,
            Some(l) if l.eq_ignore_ascii_case("message") => EventKind::Message,
            Some(l) if l.eq_ignore_ascii_case("observation") || l.is_empty() => EventKind::Observation,
            Some(l) => EventKind::Custom(l.to_string()),
            None => EventKind::Observation,
        };
        let mut e = Episode::new("query", kind, Vec::new());
        for (k, v) in props {
            let s = match v { Value::Str(s) => s.clone(), Value::Num(n) => n.to_string() };
            e.attrs.insert(k.clone(), s);
        }
        self.log.append(e)
    }
}

// =========================================================================
// M29: LOG CHUNK ITERATOR RESILIENTE CONTRA COMPACTAÇÃO DE WAL
// =========================================================================

struct LogChunkIterator {
    log: Arc<Log>,
    current_lsn: Lsn,
    to_lsn: Lsn,
    current_batch: std::vec::IntoIter<(Lsn, Episode)>,
}

impl LogChunkIterator {
    fn new(log: Arc<Log>, from_lsn: Lsn, to_lsn: Lsn) -> Self {
        Self { log, current_lsn: from_lsn, to_lsn, current_batch: Vec::new().into_iter() }
    }

    fn next_item(&mut self) -> Result<Option<(Lsn, Episode)>, HeraclitusError> {
        if let Some(item) = self.current_batch.next() { return Ok(Some(item)); }
        if self.current_lsn >= self.to_lsn { return Ok(None); }
        
        let batch = self.log.scan_capped(self.current_lsn, self.to_lsn, 2048)?;
        if batch.is_empty() { 
            self.current_lsn += 1;
            return Ok(None); 
        }
        
        if let Some(&(last_lsn, _)) = batch.last() {
            self.current_lsn = last_lsn + 1;
        }
        self.current_batch = batch.into_iter();
        Ok(self.current_batch.next())
    }
}

fn fold_normalization(items: &mut [(String, u64, f32)]) {
    let max = items.iter().map(|x| x.2).fold(0.0f32, f32::max);
    if max > 0.0 {
        for item in items.iter_mut() {
            item.2 /= max;
        }
    }
}

pub fn trace_causes(parents: &BTreeMap<String, Vec<String>>, target: &str, max_depth: usize) -> Trace {
    use std::collections::VecDeque;
    if !parents.contains_key(target) {
        return Trace { target: target.to_string(), steps: Vec::new(), roots: Vec::new() };
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
        if d < max_depth {
            for p in &causes {
                if !depth_of.contains_key(p) {
                    depth_of.insert(p.clone(), d + 1);
                    q.push_back((p.clone(), d + 1));
                }
            }
        }
        steps.push(CausalStep { id, depth: d, causes });
    }
    steps.sort_by(|a, b| a.depth.cmp(&b.depth).then(a.id.cmp(&b.id)));
    roots.sort();
    roots.dedup();
    Trace { target: target.to_string(), steps, roots }
}

