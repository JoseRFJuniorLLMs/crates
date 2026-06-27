//! The engine: composes log + memtable + views into one query surface.
//! All intelligence lives in the agent; this is just the riverbed.

use heraclitus_activation::ActivationStore;
use heraclitus_core::{Episode, EventKind, HeraclitusConfig, HeraclitusError, Lsn, ProductPoint};
use heraclitus_core::vm::{ConsistencyVirtualMachine, VmInstruction, VmState, VmVersion};
use heraclitus_crypto::KeyStore;
use heraclitus_index_graph::entity::EntityResolver;
use heraclitus_index_graph::temporal::TemporalGraph;
use heraclitus_index_attr::AttrIndex;
use heraclitus_index_graph::GraphIndex;
use heraclitus_index_text::TextIndex;
use heraclitus_index_vector::VectorIndex;
use heraclitus_log::vm_bridge;
use heraclitus_log::Log;
use heraclitus_manifold::ProductMetric;
use heraclitus_memtable::Memtable;
use heraclitus_query::ast::Value as GqlValue;
use heraclitus_query::backend::{
    cluster_of, community_of, hypotheses_of, match_edges_of, neighbors_of, node_metrics_of,
    resolve_of, traverse_of, CommunityResult, EdgeHypotheses, EdgeRow, MetricsResult, NeighborRow,
    QueryBackend,
};
use heraclitus_retrieval::{retrieve, LinearReranker, RecallInputs};
use heraclitus_views::{View, ViewRegistry};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct Engine {
    pub log: Arc<Log>,
    pub memtable: Arc<Memtable>,
    views: Mutex<ViewRegistry>,
    vector: Arc<Mutex<VectorIndex>>,
    text: Arc<Mutex<TextIndex>>,
    graph: Arc<Mutex<GraphIndex>>,
    tgraph: Arc<Mutex<TemporalGraph>>,
    entity: Arc<Mutex<EntityResolver>>,
    activation: Arc<Mutex<ActivationStore>>,
    /// Índice secundário de atributos (qualquer campo -> [LSN]). Persistido em
    /// `<data_dir>/views`; gerido diretamente pelo Engine (fora do ViewRegistry)
    /// para controlar o checkpoint/replay e o arranque rápido.
    attr: Arc<Mutex<AttrIndex>>,
    attr_dir: std::path::PathBuf,
    metric: ProductMetric,
    /// Per-agent key store when encryption at rest is enabled (§3.10).
    keystore: Option<Arc<KeyStore>>,
    /// Modo bulk-ingest: `append` grava SÓ no log (pula memtable/views/attr em
    /// RAM). Liga com HERACLITUS_LOG_ONLY=1 — permite cargas massivas (centenas
    /// de GB) com RAM limitada; as views se constroem depois via `view rebuild`.
    log_only: bool,
}

/// Wrapper so the same index object can be both registered as a View and
/// queried by the engine (the registry owns Box<dyn View>).
struct Shared<T>(Arc<Mutex<T>>);

impl<T: View> View for Shared<T> {
    fn name(&self) -> &str {
        // Names are static per index type.
        let g = self.0.lock().unwrap();
        // SAFETY-free trick: names are 'static string literals in all our
        // views, so returning them outlives the guard.
        match g.name() {
            "vector" => "vector",
            "text" => "text",
            "graph" => "graph",
            "tgraph" => "tgraph",
            "entity" => "entity",
            "activation" => "activation",
            _ => "view",
        }
    }
    fn apply(&mut self, lsn: Lsn, event: &Episode) {
        self.0.lock().unwrap().apply(lsn, event);
    }
    fn watermark(&self) -> Lsn {
        self.0.lock().unwrap().watermark()
    }
    fn reset(&mut self) {
        self.0.lock().unwrap().reset();
    }
}

impl Engine {
    /// Open the engine silently (tests, the CLI, embedded callers). For the
    /// narrated server boot use [`Engine::open_with_boot`].
    pub fn open(config: &HeraclitusConfig) -> Result<Self, HeraclitusError> {
        Self::open_with_boot(config, &crate::boot::Boot::silent())
    }

    /// Open the engine while narrating each subsystem through `boot`. The server
    /// passes a console reporter (banner, `[  OK  ]` lines, spinner on the slow
    /// replay phases); `open` passes a silent one so nothing leaks into tests.
    pub fn open_with_boot(
        config: &HeraclitusConfig,
        boot: &crate::boot::Boot,
    ) -> Result<Self, HeraclitusError> {
        use crate::boot::{fmt_bytes, group, sup};

        // Modo recovery para stores grandes demais p/ a RAM: pula o replay das
        // views pesadas (que vivem 100% em RAM) e a (re)construção do índice de
        // atributos. O banco sobe servindo o log (a fonte da verdade); as views
        // ficam vazias até um `view rebuild`. Liga com HERACLITUS_SKIP_VIEW_REPLAY=1.
        let truthy = |k: &str| {
            std::env::var(k)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "on" | "yes"))
                .unwrap_or(false)
        };
        // Bulk-ingest: appends gravam só no log. Implica pular o replay no boot.
        let log_only = truthy("HERACLITUS_LOG_ONLY");
        let skip_replay = log_only || truthy("HERACLITUS_SKIP_VIEW_REPLAY");

        // Encryption at rest (§3.10): when enabled, the log seals episode
        // content with a per-agent key kept under `<data_dir>/keys`.
        let keystore = if config.encryption_at_rest {
            let p = boot.phase("Cifra em repouso (keystore por agente)");
            let ks = KeyStore::open(config.data_dir.join("keys"))?;
            p.ok("ChaCha20-Poly1305 · crypto-shred pronto");
            Some(ks)
        } else {
            None
        };

        let log = {
            let p = boot.phase("Log append-only (a fonte da verdade)");
            let log = Arc::new(Log::open_with_keystore(
                config.data_dir.join("log"),
                config.segment_max_bytes,
                config.fsync.clone(),
                keystore.clone(),
            )?);
            let head = log.head();
            p.ok(format!(
                "{} eventos · head LSN {} · segmentos de {}",
                group(head),
                group(head),
                fmt_bytes(config.segment_max_bytes)
            ));
            log
        };

        // The geometry announces itself: the learned product manifold signature.
        let metric = {
            let p = boot.phase("Geometria de produto (variedade aprendida)");
            let m = ProductMetric::default();
            let s = &m.sig;
            p.ok(format!(
                "H{}⊗S{}⊗E{} · Poincaré κ={} · esfera κ=+{} · {} dims",
                sup(s.a),
                sup(s.b),
                sup(s.c),
                s.k1,
                s.k2,
                s.a + s.b + s.c
            ));
            m
        };

        let vector = {
            let p = boot.phase("Índice vetorial (HNSW hiperbólico)");
            let v = Arc::new(Mutex::new(VectorIndex::new(metric.clone())));
            p.ok("k-NN no espaço de produto");
            v
        };
        let text = {
            let p = boot.phase("Índice de texto (invertido)");
            let t = Arc::new(Mutex::new(TextIndex::new()));
            p.ok("recall em duas fases");
            t
        };
        let graph = {
            let p = boot.phase("Índice de grafo (proveniência DAG)");
            let g = Arc::new(Mutex::new(GraphIndex::new()));
            p.ok("WHY · arestas de origem");
            g
        };
        let tgraph = {
            let p = boot.phase("Grafo temporal (consultas AS OF)");
            let g = Arc::new(Mutex::new(TemporalGraph::new()));
            p.ok("arestas com intervalos de validade");
            g
        };
        let entity = {
            let p = boot.phase("Resolução de entidades");
            let e = Arc::new(Mutex::new(EntityResolver::new()));
            p.ok("merge/cluster por chave");
            e
        };
        let activation = {
            let p = boot.phase("Ativação ACT-R (memória cognitiva)");
            let a = Arc::new(Mutex::new(ActivationStore::new(config.activation_decay)));
            p.ok(format!("decaimento d={}", config.activation_decay));
            a
        };

        // The slow phase on a big log: replay the tail into every view. The
        // spinner moves here while millions of events stream through.
        let registry = {
            let p = boot.phase("Replay das views a partir do log");
            let mut registry = ViewRegistry::open(&config.data_dir)?;
            registry.register(Box::new(Shared(vector.clone())));
            registry.register(Box::new(Shared(text.clone())));
            registry.register(Box::new(Shared(graph.clone())));
            registry.register(Box::new(Shared(tgraph.clone())));
            registry.register(Box::new(Shared(entity.clone())));
            registry.register(Box::new(Shared(activation.clone())));
            if skip_replay {
                p.ok("PULADO — HERACLITUS_SKIP_VIEW_REPLAY (views vazias; use view rebuild)");
            } else {
                registry.catch_up(&log)?;
                let wm = registry.min_watermark();
                p.ok(format!("6 views materializadas @ LSN {}", group(wm)));
            }
            registry
        };

        // Índice secundário de atributos: carrega o checkpoint e replaya só a
        // cauda (arranque rápido); num log virgem constrói tudo uma vez e grava.
        let attr_dir = config.data_dir.join("views");
        let attr = {
            let p = boot.phase("Índice de atributos (campo → LSN)");
            let attr = Arc::new(Mutex::new(AttrIndex::open(&attr_dir)));
            let keys = {
                let mut idx = attr.lock().unwrap();
                if !skip_replay {
                    // Build PAGINADO: o log é varrido em janelas (não materializa os
                    // milhões de episódios de uma vez — limita a RAM do arranque).
                    let head = log.head();
                    let mut cur = if idx.is_empty() { 0 } else { idx.watermark() };
                    let mut built = false;
                    while cur <= head {
                        let batch = log.scan_capped(cur, head + 1, 100_000)?;
                        if batch.is_empty() {
                            break;
                        }
                        let last = batch.last().unwrap().0;
                        for (lsn, ep) in &batch {
                            idx.apply(*lsn, ep);
                        }
                        built = true;
                        cur = last + 1;
                    }
                    if built {
                        idx.save(&attr_dir)?;
                    }
                }
                idx.keys()
            };
            if skip_replay {
                p.ok(format!("PULADO — {} chaves do checkpoint", group(keys as u64)));
            } else {
                p.ok(format!("{} chaves indexadas", group(keys as u64)));
            }
            attr
        };

        Ok(Self {
            log,
            memtable: Arc::new(Memtable::new(config.memtable_cap)),
            views: Mutex::new(registry),
            vector,
            text,
            graph,
            tgraph,
            entity,
            activation,
            attr,
            attr_dir,
            metric,
            keystore,
            log_only,
        })
    }

    /// Grava o checkpoint do índice de atributos (o servidor pode chamar
    /// periodicamente / no shutdown para o arranque seguinte só replayar a cauda).
    pub fn checkpoint_attr(&self) -> Result<(), HeraclitusError> {
        self.attr.lock().unwrap().save(&self.attr_dir)
    }

    // ── H-VM ledger (M20) ────────────────────────────────────────────────────
    // The Sovereignty-Layer key/value ledger, reachable from the engine. Writes
    // are H-VM ISA bytecode appended to the *same* durable log as episodes
    // (`vm_bridge`, additive — the format is untouched); reads replay the log
    // through the deterministic reducer (read-your-writes via the log being the
    // truth). State is replayed on demand today; an incremental cache backed by
    // the Bᵋ-tree checkpoint is the next refinement.

    /// Append an H-VM upsert to the durable log.
    pub fn hvm_upsert(&self, key: Vec<u8>, val: Vec<u8>) -> Result<Lsn, HeraclitusError> {
        let lsn = self.log.head();
        let instr = VmInstruction::Upsert { key, val, lsn, ev_id: heraclitus_core::EventId::new() };
        vm_bridge::append_instruction(&self.log, VmVersion(1), &instr)
    }

    /// Append an H-VM delete to the durable log.
    pub fn hvm_delete(&self, key: Vec<u8>) -> Result<Lsn, HeraclitusError> {
        let lsn = self.log.head();
        let instr = VmInstruction::Delete { key, lsn, ev_id: heraclitus_core::EventId::new() };
        vm_bridge::append_instruction(&self.log, VmVersion(1), &instr)
    }

    /// Replay the H-VM ledger from the log into a deterministic [`VmState`].
    pub fn hvm_state(&self) -> Result<VmState, HeraclitusError> {
        let vm = ConsistencyVirtualMachine::new(VmVersion(1));
        vm_bridge::replay_vm(&self.log, &vm)
    }

    /// Materialize the H-VM ledger into a Bᵋ-tree (Fractal Tree) and persist it
    /// atomically as a checkpoint. Reload with `heraclitus_btree::BEpsilonTree::load`.
    pub fn hvm_checkpoint(&self, path: &std::path::Path) -> Result<(), HeraclitusError> {
        let vm = ConsistencyVirtualMachine::new(VmVersion(1));
        let tree = vm_bridge::replay_vm_to_btree(&self.log, &vm)?;
        tree.save(path)?;
        Ok(())
    }

    /// Crypto-shred (§3.10): destroy an agent's encryption key so all of its
    /// sealed content becomes permanently unreadable. The log is never mutated.
    /// Errors if encryption at rest is disabled.
    pub fn shred(&self, agent_id: &str) -> Result<bool, HeraclitusError> {
        match &self.keystore {
            Some(ks) => Ok(ks.shred(agent_id)?),
            None => Err(HeraclitusError::Config(
                "encryption at rest is disabled; nothing to shred".into(),
            )),
        }
    }

    /// Append + synchronously index into memtable AND views.
    /// Read-your-own-writes holds for every index path.
    pub fn append(&self, episode: Episode) -> Result<Lsn, HeraclitusError> {
        // Bulk-ingest: grava só no log (RAM limitada p/ cargas massivas). As
        // views/attr se reconstroem depois do log (a fonte da verdade).
        if self.log_only {
            return self.log.append(episode);
        }
        let lsn = self.log.append(episode.clone())?;
        self.memtable.apply(lsn, episode.clone());
        self.views.lock().unwrap().apply(lsn, &episode);
        self.attr.lock().unwrap().apply(lsn, &episode);
        Ok(lsn)
    }

    pub fn snapshot(&self) -> Lsn {
        self.log.head()
    }

    pub fn rebuild(&self, view: Option<&str>) -> Result<(), HeraclitusError> {
        self.views.lock().unwrap().rebuild(&self.log, view)
    }

    pub fn stats(&self) -> serde_json::Value {
        serde_json::json!({
            "head": self.log.head(),
            "memtable": self.memtable.len(),
            "vector_indexed": self.vector.lock().unwrap().len(),
            "text_indexed": self.text.lock().unwrap().len(),
            "graph_nodes": self.graph.lock().unwrap().len(),
            "tgraph_edges": self.tgraph.lock().unwrap().edges.len(),
            "entity_keys": self.entity.lock().unwrap().mappings.len(),
            "activation_tracked": self.activation.lock().unwrap().len(),
            "views": self.views.lock().unwrap().view_names(),
        })
    }

    pub fn verify(&self) -> Result<serde_json::Value, HeraclitusError> {
        let r = self.log.verify()?;
        Ok(serde_json::json!({
            "segments": r.segments, "records": r.records, "merkle_ok": r.merkle_ok
        }))
    }

    /// Two-stage recall (§3.8) over the real indexes + memtable merge.
    pub fn recall(&self, text: &str, k: usize) -> Result<serde_json::Value, HeraclitusError> {
        let now = self.log.head(); // deterministic clock surrogate for scoring
        let txt_hits: Vec<_> = {
            let idx = self.text.lock().unwrap();
            idx.search(text, heraclitus_retrieval::RECALL_N)
                .into_iter()
                .map(|h| (h.id, h.lsn, h.score))
                .collect()
        };
        let act_hits: Vec<_> = {
            let act = self.activation.lock().unwrap();
            act.top_k(now, heraclitus_retrieval::RECALL_N)
                .into_iter()
                .map(|h| (h.id, h.score))
                .collect()
        };
        let mem_hits: Vec<_> = self
            .memtable
            .text_search(text, heraclitus_retrieval::RECALL_N)
            .into_iter()
            .map(|h| (h.id, h.lsn, h.score))
            .collect();

        // Memtable hits join the text channel (freshest truth first).
        let mut text_channel = mem_hits;
        text_channel.extend(txt_hits);

        let reranker = LinearReranker {
            head_lsn: self.log.head(),
            ..Default::default()
        };
        let ranked = retrieve(
            text,
            RecallInputs {
                vector: Vec::new(), // no query embedding for raw text (no LLM in the engine)
                text: text_channel,
                activation: act_hits,
            },
            &reranker,
            k,
        );

        // Hydrate rows from the log.
        let mut rows = Vec::new();
        for (cand, score) in ranked {
            if let Some((lsn, ep)) = self.log.read(cand.lsn)?.filter(|(_, e)| e.id == cand.id) {
                rows.push(serde_json::json!({
                    "lsn": lsn,
                    "id": ep.id.to_string(),
                    "content": String::from_utf8_lossy(&ep.content),
                    "score": score,
                }));
            } else {
                rows.push(serde_json::json!({
                    "id": cand.id.to_string(), "lsn": cand.lsn, "score": score
                }));
            }
        }
        Ok(serde_json::Value::Array(rows))
    }
}

/// The engine IS the real `QueryBackend` for the GQL layer: HNSW for
/// NEAREST, two-stage for RECALL, graph index for PROVENANCE.
impl QueryBackend for Engine {
    fn scan(&self, as_of: Option<Lsn>) -> Result<Vec<(Lsn, Episode)>, HeraclitusError> {
        self.log.scan(0, as_of.unwrap_or(u64::MAX))
    }

    fn scan_range(&self, from: Lsn, to: Lsn) -> Result<Vec<(Lsn, Episode)>, HeraclitusError> {
        // Windowed + capped: segment pruning makes a time slice cheap, and the
        // QUERY_SCAN_CAP keeps a broad scan from exhausting memory (§query guard).
        self.log
            .scan_capped(from, to, heraclitus_query::backend::QUERY_SCAN_CAP)
    }

    fn attr_lookup(
        &self,
        field: &str,
        value: &str,
        as_of: Option<Lsn>,
    ) -> Result<Option<Vec<(Lsn, Episode)>>, HeraclitusError> {
        // O índice dá os LSNs exatos; cada `log.read` é O(1) via o índice de
        // offset por-LSN do log (seek directo). Hidratação = nº de matches × O(1).
        let mut lsns: Vec<Lsn> = {
            let idx = self.attr.lock().unwrap();
            idx.lookup(field, value).to_vec()
        };
        if let Some(bound) = as_of {
            lsns.retain(|l| *l < bound);
        }
        lsns.sort_unstable();
        let mut out: Vec<(Lsn, Episode)> = Vec::with_capacity(lsns.len());
        for l in lsns {
            if let Some(hit) = self.log.read(l)? {
                out.push(hit);
            }
            if out.len() >= heraclitus_query::backend::QUERY_SCAN_CAP {
                break;
            }
        }
        Ok(Some(out))
    }

    fn head(&self) -> Result<Lsn, HeraclitusError> {
        // Views apply synchronously on append, so the log head is the
        // consistency point the engine can serve.
        Ok(self.log.head())
    }

    fn recall(
        &self,
        text: &str,
        k: usize,
        as_of: Option<Lsn>,
    ) -> Result<Vec<(Lsn, Episode, f32)>, HeraclitusError> {
        // Audit #10: AS OF is honored by post-filtering on LSN (the indexes
        // are head-versioned in v0; a versioned-index time travel is the
        // planned upgrade). Over-fetch to compensate for filtered rows.
        let fetch = if as_of.is_some() { k * 4 } else { k };
        let v = Engine::recall(self, text, fetch)?;
        let empty = Vec::new();
        let mut out = Vec::new();
        for row in v.as_array().unwrap_or(&empty) {
            let lsn = row["lsn"].as_u64().unwrap_or(0);
            if let Some(bound) = as_of {
                if lsn >= bound {
                    continue;
                }
            }
            if let Some((l, e)) = self.log.read(lsn)? {
                out.push((l, e, row["score"].as_f64().unwrap_or(0.0) as f32));
            }
        }
        out.truncate(k);
        Ok(out)
    }

    fn nearest(
        &self,
        vector: &[f32],
        k: usize,
        as_of: Option<Lsn>,
    ) -> Result<Vec<(Lsn, Episode, f32)>, HeraclitusError> {
        let dims = {
            // Interpret the raw vector as the hyperbolic component (v0).
            let mut hyp = vector.to_vec();
            heraclitus_manifold::project_to_ball(&mut hyp);
            ProductPoint {
                hyp,
                sph: vec![],
                euc: vec![],
            }
        };
        // Audit #10: honor AS OF via LSN post-filter (over-fetch first).
        let fetch = if as_of.is_some() { k * 4 } else { k };
        let in_snapshot = |lsn: Lsn| as_of.map(|b| lsn < b).unwrap_or(true);
        let hits = self.vector.lock().unwrap().search(&dims, fetch, 128, None);
        let mut out = Vec::new();
        for h in hits.into_iter().filter(|h| in_snapshot(h.lsn)) {
            if let Some((l, e)) = self.log.read(h.lsn)? {
                out.push((l, e, h.dist));
            }
        }
        // Merge the memtable tail (exact) for read-your-own-writes.
        let mem = self.memtable.knn(&self.metric, &dims, fetch);
        for m in mem.into_iter().filter(|m| in_snapshot(m.lsn)) {
            if !out.iter().any(|(_, e, _)| e.id == m.id) {
                if let Some((l, e)) = self.log.read(m.lsn)? {
                    out.push((l, e, m.score));
                }
            }
        }
        out.sort_by(|a, b| a.2.total_cmp(&b.2));
        out.truncate(k);
        Ok(out)
    }

    fn provenance(&self, id: &str) -> Result<Vec<String>, HeraclitusError> {
        let parsed: Result<heraclitus_core::EventId, _> = id.parse();
        match parsed {
            Ok(eid) => Ok(self
                .graph
                .lock()
                .unwrap()
                .parents(&eid)
                .into_iter()
                .map(|p| p.to_string())
                .collect()),
            Err(_) => Ok(Vec::new()),
        }
    }

    fn lsn_for_timestamp(&self, ts_ms: u64) -> Result<Lsn, HeraclitusError> {
        for (lsn, e) in self.log.scan(0, u64::MAX)? {
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
        // Real path: read the incrementally-maintained view (no replay). The
        // M8 gate is that this matches `LogBackend`'s from-scratch replay.
        let g = self.tgraph.lock().unwrap();
        Ok(neighbors_of(&g, node, etype, as_of, min_confidence))
    }

    fn traverse(
        &self,
        start: &str,
        max_depth: usize,
        as_of: Option<Lsn>,
        min_confidence: f32,
    ) -> Result<Vec<(String, usize)>, HeraclitusError> {
        let g = self.tgraph.lock().unwrap();
        Ok(traverse_of(&g, start, max_depth, as_of, min_confidence))
    }

    fn match_edges(
        &self,
        src: Option<&str>,
        etype: Option<&str>,
        dst: Option<&str>,
        as_of: Option<Lsn>,
    ) -> Result<Vec<EdgeRow>, HeraclitusError> {
        let g = self.tgraph.lock().unwrap();
        Ok(match_edges_of(&g, src, etype, dst, as_of))
    }

    fn edge_hypotheses(
        &self,
        from: &str,
        to: &str,
        etype: &str,
        as_of: Option<Lsn>,
    ) -> Result<Option<EdgeHypotheses>, HeraclitusError> {
        Ok(hypotheses_of(&self.tgraph.lock().unwrap(), from, to, etype, as_of))
    }

    fn community(&self, node: &str, as_of: Option<Lsn>) -> Result<Option<CommunityResult>, HeraclitusError> {
        Ok(community_of(&self.tgraph.lock().unwrap(), node, as_of))
    }

    fn node_metrics(&self, node: &str, as_of: Option<Lsn>) -> Result<Option<MetricsResult>, HeraclitusError> {
        Ok(node_metrics_of(&self.tgraph.lock().unwrap(), node, as_of))
    }

    fn resolve_entity(&self, key: &str, as_of: Option<Lsn>) -> Result<Option<String>, HeraclitusError> {
        Ok(resolve_of(&self.entity.lock().unwrap(), key, as_of))
    }

    fn entity_cluster(&self, entity_id: &str, as_of: Option<Lsn>) -> Result<Vec<String>, HeraclitusError> {
        Ok(cluster_of(&self.entity.lock().unwrap(), entity_id, as_of))
    }

    fn append(
        &self,
        label: Option<&str>,
        props: &[(String, GqlValue)],
    ) -> Result<Lsn, HeraclitusError> {
        let kind = match label {
            Some(l) if l.eq_ignore_ascii_case("action") => EventKind::Action,
            Some(l) if l.eq_ignore_ascii_case("message") => EventKind::Message,
            Some(l) if l.eq_ignore_ascii_case("observation") => EventKind::Observation,
            Some(l) => EventKind::Custom(l.to_string()),
            None => EventKind::Observation,
        };
        let mut attrs = HashMap::new();
        for (k, v) in props {
            let s = match v {
                GqlValue::Str(s) => s.clone(),
                GqlValue::Num(n) => n.to_string(),
            };
            attrs.insert(k.clone(), s);
        }
        let mut e = Episode::new("gql", kind, Vec::new());
        e.attrs = attrs.into_iter().collect();
        Engine::append(self, e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use heraclitus_core::FsyncPolicy;
    use heraclitus_query::backend::{replay_graph, LogBackend};

    /// Appends a provenance chain a←b←c plus a distilled fact f from {a,b}
    /// through the engine (which maintains the tgraph view incrementally).
    fn seed_chain(engine: &Engine) -> [String; 4] {
        let mut a = Episode::new("ag", EventKind::Observation, b"a".to_vec());
        a.attrs.insert("edge_type".into(), "socio_de".into());
        let mut b = Episode::new("ag", EventKind::Observation, b"b".to_vec());
        b.attrs.insert("edge_type".into(), "pagou".into());
        b.parents.push(a.id);
        let mut c = Episode::new("ag", EventKind::Observation, b"c".to_vec());
        c.parents.push(b.id);
        let mut f = Episode::new("distill", EventKind::FactDerived, b"f".to_vec());
        f.attrs.insert("edge_type".into(), "similar_a".into());
        f.parents.push(a.id);
        f.parents.push(b.id);
        let ids = [
            a.id.to_string(),
            b.id.to_string(),
            c.id.to_string(),
            f.id.to_string(),
        ];
        for e in [a, b, c, f] {
            engine.append(e).unwrap();
        }
        ids
    }

    fn engine_in(dir: &std::path::Path) -> Engine {
        let cfg = HeraclitusConfig {
            data_dir: dir.to_path_buf(),
            fsync: FsyncPolicy::Always,
            ..Default::default()
        };
        Engine::open(&cfg).unwrap()
    }

    #[test]
    fn m20_hvm_ledger_through_engine_survives_reopen_and_checkpoints() {
        // M20 integration: the H-VM ledger is reachable from the Engine, durable
        // across a reopen (replay), and checkpointable to a Bᵋ-tree on disk.
        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("hvm.hbt");
        {
            let engine = engine_in(dir.path());
            engine.hvm_upsert(b"user:1".to_vec(), b"alice".to_vec()).unwrap();
            engine.hvm_upsert(b"user:2".to_vec(), b"bob".to_vec()).unwrap();
            engine.hvm_delete(b"user:1".to_vec()).unwrap();

            let state = engine.hvm_state().unwrap();
            assert_eq!(state.memory_layers.get(b"user:2".as_slice()), Some(&b"bob".to_vec()));
            assert!(!state.memory_layers.contains_key(b"user:1".as_slice()));

            // Checkpoint to a Bᵋ-tree on disk and verify its contents.
            engine.hvm_checkpoint(&ckpt).unwrap();
            let loaded = heraclitus_btree::BEpsilonTree::load(&ckpt).unwrap();
            assert_eq!(loaded.get(b"user:2"), Some(b"bob".to_vec()));
            assert_eq!(loaded.get(b"user:1"), None);
        }

        // Reopen over the same data dir: the ledger replays from the durable log.
        let engine2 = engine_in(dir.path());
        let state2 = engine2.hvm_state().unwrap();
        assert_eq!(state2.memory_layers.get(b"user:2".as_slice()), Some(&b"bob".to_vec()));
        assert!(!state2.memory_layers.contains_key(b"user:1".as_slice()));
    }

    #[test]
    fn m8_incremental_view_equals_replay_bit_for_bit() {
        // THE M8 GATE: the graph maintained incrementally on the append path
        // must equal the graph rebuilt from scratch by replaying the log.
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_in(dir.path());
        let _ids = seed_chain(&engine);

        let replayed = replay_graph(&engine.log).unwrap();
        let live = engine.tgraph.lock().unwrap();
        assert_eq!(
            live.state_hash(),
            replayed.state_hash(),
            "incremental view must equal from-scratch replay, byte for byte"
        );
        assert_eq!(live.edges.len(), 4);
    }

    #[test]
    fn m8_reopen_rebuilds_identical_graph() {
        // Crash/restart story: a fresh engine over the same data_dir replays
        // the log and lands on the identical graph state.
        let dir = tempfile::tempdir().unwrap();
        let hash_a = {
            let engine = engine_in(dir.path());
            seed_chain(&engine);
            let h = engine.tgraph.lock().unwrap().state_hash();
            h
        };
        let engine_b = engine_in(dir.path());
        let hash_b = engine_b.tgraph.lock().unwrap().state_hash();
        assert_eq!(hash_a, hash_b, "reopened engine must reconstruct the graph");
    }

    #[test]
    fn m8_neighbors_via_gql_matches_reference() {
        // NEIGHBORS through GQL: the real (view-backed) engine and the
        // reference (replay-backed) LogBackend must return identical rows.
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_in(dir.path());
        let ids = seed_chain(&engine);

        let be = LogBackend::new(engine.log.clone());
        let q = format!("NEIGHBORS (\"{}\")", ids[0]);
        let via_engine = heraclitus_query::execute(&q, &engine).unwrap();
        let via_log = heraclitus_query::execute(&q, &be).unwrap();
        assert_eq!(via_engine, via_log, "real backend must match the reference");
        assert_eq!(via_engine.as_array().unwrap().len(), 2);

        let qt = format!("TRAVERSE (\"{}\", 3)", ids[0]);
        let t_engine = heraclitus_query::execute(&qt, &engine).unwrap();
        let t_log = heraclitus_query::execute(&qt, &be).unwrap();
        assert_eq!(t_engine, t_log);
    }

    /// Appends explicit, mutable edges through the engine (M9): the socio edge
    /// is asserted then retracted; the pagou edge stays open.
    fn seed_mutations(engine: &Engine) {
        let mk = |from: &str, to: &str, etype: &str, op: &str| {
            let mut e = Episode::new("ag", EventKind::Observation, vec![]);
            e.attrs.insert("edge_from".into(), from.into());
            e.attrs.insert("edge_to".into(), to.into());
            e.attrs.insert("edge_type".into(), etype.into());
            e.attrs.insert("edge_op".into(), op.into());
            e
        };
        engine.append(mk("Alfa", "Maria", "socio_de", "assert")).unwrap();
        engine.append(mk("Alfa", "Beto", "pagou", "assert")).unwrap();
        engine.append(mk("Alfa", "Maria", "socio_de", "retract")).unwrap();
    }

    #[test]
    fn m9_edge_match_via_gql_matches_reference() {
        // M9 GATE: relationship MATCH with AS OF + edge mutation. The real
        // (view-backed) engine and the reference (replay-backed) LogBackend
        // must agree at every snapshot.
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_in(dir.path());
        seed_mutations(&engine);
        let be = LogBackend::new(engine.log.clone());

        for q in [
            "MATCH (a)-[r]->(b) RETURN *",
            "MATCH (a)-[r]->(b) AS OF LSN 2 RETURN *",
            "MATCH (a)-[r]->(b) AS OF LSN 1 RETURN *",
            "MATCH (a)-[r:pagou]->(b) RETURN b.id, r.type",
            "MATCH (a)-[r]->(b) WHERE b = \"Maria\" AS OF LSN 2 RETURN *",
        ] {
            let via_engine = heraclitus_query::execute(q, &engine).unwrap();
            let via_log = heraclitus_query::execute(q, &be).unwrap();
            assert_eq!(via_engine, via_log, "engine vs reference disagree on `{q}`");
        }

        // Incremental view must still equal a from-scratch replay, even with the
        // valid_to mutation in play.
        let replayed = replay_graph(&engine.log).unwrap();
        let live = engine.tgraph.lock().unwrap();
        assert_eq!(live.state_hash(), replayed.state_hash());
        // The retracted edge is closed, not deleted.
        assert_eq!(live.edges.len(), 2);
    }

    #[test]
    fn m10_fuse_runs_on_the_real_engine() {
        // FUSE is a default QueryBackend method, so the engine inherits it and
        // it flows through `execute` (and thus gRPC). Smoke-test the end-to-end
        // path on the real backend: it returns the per-channel breakdown and is
        // reproducible. (The "fusion wins" gate itself lives in the query crate
        // against the exact reference backend.)
        use heraclitus_core::ProductPoint;
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_in(dir.path());

        let anchor = Episode::new("ag", EventKind::Observation, b"anchor".to_vec());
        let a_id = anchor.id;
        engine.append(anchor).unwrap();
        let child = |conf: &str, hyp: f32, text: &str| {
            let mut e = Episode::new("ag", EventKind::Observation, text.as_bytes().to_vec());
            e.parents.push(a_id);
            e.attrs.insert("confidence".into(), conf.into());
            e.embedding = Some(ProductPoint { hyp: vec![hyp], sph: vec![], euc: vec![] });
            engine.append(e).unwrap();
        };
        child("0.7", 0.65, "fraude");
        child("1.0", 0.0, "pagamento rotineiro");
        child("0.2", 0.5, "transferencia comum");
        child("0.2", 0.95, "fraude fraude");

        let q = format!("FUSE (\"fraude\", [0.5], \"{a_id}\", 10)");
        let v = heraclitus_query::execute(&q, &engine).unwrap();
        let rows = v.as_array().unwrap();
        assert!(!rows.is_empty(), "fusion returns candidates");
        // Every row carries the audited per-channel breakdown.
        for r in rows {
            assert!(r["graph_score"].is_number());
            assert!(r["vector_score"].is_number());
            assert!(r["text_score"].is_number());
            assert!(r["score"].is_number());
        }
        let v2 = heraclitus_query::execute(&q, &engine).unwrap();
        assert_eq!(v, v2, "reproducible on the engine too");
    }

    #[test]
    fn m11_entity_resolution_view_equals_replay() {
        // M11 GATE: the incrementally maintained resolver equals a from-scratch
        // replay, and RESOLVE/CLUSTER via GQL match the reference backend.
        use heraclitus_query::backend::replay_resolver;
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_in(dir.path());

        let mention = |key: &str| {
            let mut e = Episode::new("ag", EventKind::Observation, vec![]);
            e.attrs.insert("entity_key".into(), key.into());
            e
        };
        let merge = |a: &str, b: &str| {
            let mut e = Episode::new("ag", EventKind::Observation, vec![]);
            e.attrs.insert("er_op".into(), "merge".into());
            e.attrs.insert("er_a".into(), a.into());
            e.attrs.insert("er_b".into(), b.into());
            e
        };
        engine.append(mention("CPF:111")).unwrap();
        engine.append(mention("CPF:222")).unwrap();
        engine.append(mention("CPF:333")).unwrap();
        engine.append(merge("CPF:222", "CPF:111")).unwrap();
        engine.append(merge("CPF:333", "CPF:111")).unwrap();

        // View == replay (bit-identical).
        let replayed = replay_resolver(&engine.log).unwrap();
        let live = engine.entity.lock().unwrap();
        assert_eq!(live.state_hash(), replayed.state_hash());
        drop(live);

        // GQL on the real engine matches the reference backend.
        let be = LogBackend::new(engine.log.clone());
        for q in [
            "RESOLVE (\"CPF:333\")",
            "RESOLVE (\"CPF:222\") AS OF LSN 3",
            "CLUSTER (\"CPF:111\")",
        ] {
            assert_eq!(
                heraclitus_query::execute(q, &engine).unwrap(),
                heraclitus_query::execute(q, &be).unwrap(),
                "engine vs reference disagree on `{q}`"
            );
        }
        // All three CPFs collapsed onto one entity.
        let cluster = heraclitus_query::execute("CLUSTER (\"CPF:111\")", &engine).unwrap();
        assert_eq!(cluster.as_array().unwrap().len(), 3);
    }

    #[test]
    fn m12_hypothesis_graph_via_gql_matches_reference() {
        // M12 GATE: conflicting hypotheses on one edge coexist; HYPOTHESES on the
        // real (view) engine matches the reference (replay), including AS OF.
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_in(dir.path());
        let hyp = |hid: &str, conf: &str, stance: &str| {
            let mut e = Episode::new("ag", EventKind::Observation, vec![]);
            e.attrs.insert("edge_from".into(), "X".into());
            e.attrs.insert("edge_to".into(), "Y".into());
            e.attrs.insert("edge_type".into(), "fraud_partner".into());
            e.attrs.insert("hypothesis".into(), hid.into());
            e.attrs.insert("confidence".into(), conf.into());
            e.attrs.insert("stance".into(), stance.into());
            e
        };
        engine.append(hyp("R1", "0.8", "support")).unwrap();
        engine.append(hyp("R2", "0.6", "refute")).unwrap();

        // View == replay (the extra version must be in both).
        let replayed = replay_graph(&engine.log).unwrap();
        let live = engine.tgraph.lock().unwrap();
        assert_eq!(live.state_hash(), replayed.state_hash());
        assert_eq!(live.edges.len(), 1, "one edge, two hypotheses");
        drop(live);

        let be = LogBackend::new(engine.log.clone());
        for q in [
            "HYPOTHESES (\"X\", \"Y\", \"fraud_partner\")",
            "HYPOTHESES (\"X\", \"Y\", \"fraud_partner\") AS OF LSN 1",
        ] {
            assert_eq!(
                heraclitus_query::execute(q, &engine).unwrap(),
                heraclitus_query::execute(q, &be).unwrap(),
                "engine vs reference disagree on `{q}`"
            );
        }
        let v = heraclitus_query::execute("HYPOTHESES (\"X\", \"Y\", \"fraud_partner\")", &engine).unwrap();
        assert_eq!(v["hypotheses"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn m13_why_via_gql_matches_reference() {
        // M13 GATE: WHY over the provenance DAG. The real engine and the
        // reference backend agree, and the trace bottoms out at the roots.
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_in(dir.path());
        let a = Episode::new("ag", EventKind::Observation, b"a".to_vec());
        let b = Episode::new("ag", EventKind::Observation, b"b".to_vec());
        let mut f = Episode::new("distill", EventKind::FactDerived, b"f".to_vec());
        f.parents = vec![a.id, b.id];
        let mut d = Episode::new("ag", EventKind::Action, b"d".to_vec());
        d.parents = vec![f.id];
        let did = d.id.to_string();
        for e in [a, b, f, d] {
            engine.append(e).unwrap();
        }

        let be = LogBackend::new(engine.log.clone());
        let q = format!("WHY (\"{did}\")");
        assert_eq!(
            heraclitus_query::execute(&q, &engine).unwrap(),
            heraclitus_query::execute(&q, &be).unwrap(),
            "engine vs reference disagree on WHY"
        );
        let v = heraclitus_query::execute(&q, &engine).unwrap();
        assert_eq!(v["steps"].as_array().unwrap().len(), 4);
        assert_eq!(v["roots"].as_array().unwrap().len(), 2, "two root observations");
    }

    #[test]
    fn m14_analytics_via_gql_matches_reference() {
        // M14 GATE: COMMUNITY/METRICS on the real engine match the reference and
        // detect the fraud rings consistently.
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_in(dir.path());
        let edge = |from: &str, to: &str| {
            let mut e = Episode::new("ag", EventKind::Observation, vec![]);
            e.attrs.insert("edge_from".into(), from.into());
            e.attrs.insert("edge_to".into(), to.into());
            e.attrs.insert("edge_type".into(), "socio_de".into());
            e
        };
        for (a, b) in [("A1", "A2"), ("A2", "A3"), ("A3", "A1"), ("B1", "B2")] {
            engine.append(edge(a, b)).unwrap();
        }
        let be = LogBackend::new(engine.log.clone());
        for q in ["COMMUNITY (\"A1\")", "METRICS (\"A1\")", "COMMUNITY (\"B1\")"] {
            assert_eq!(
                heraclitus_query::execute(q, &engine).unwrap(),
                heraclitus_query::execute(q, &be).unwrap(),
                "engine vs reference disagree on `{q}`"
            );
        }
        let v = heraclitus_query::execute("COMMUNITY (\"A1\")", &engine).unwrap();
        assert_eq!(v["members"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn m15_decide_emits_actions_reproducible_via_replay() {
        // M15 GATE: a decision is an Action event in the log; a fresh engine
        // replaying the same data sees the decisions; re-deciding is idempotent.
        let dir = tempfile::tempdir().unwrap();
        let edge = |from: &str, to: &str, etype: &str, conf: &str| {
            let mut e = Episode::new("ag", EventKind::Observation, vec![]);
            e.attrs.insert("edge_from".into(), from.into());
            e.attrs.insert("edge_to".into(), to.into());
            e.attrs.insert("edge_type".into(), etype.into());
            e.attrs.insert("confidence".into(), conf.into());
            e
        };
        let fired = {
            let engine = engine_in(dir.path());
            for leaf in ["L1", "L2", "L3", "L4"] {
                engine.append(edge("H", leaf, "socio_de", "1.0")).unwrap();
            }
            engine.append(edge("X", "Y", "fraud_partner", "0.9")).unwrap();
            let v = heraclitus_query::execute("DECIDE ()", &engine).unwrap();
            v["fired"].as_array().unwrap().len()
        };
        assert!(fired >= 2, "hub and fraud edge flagged");

        // Reopen: replay reconstructs the decisions (they are log events).
        let engine2 = engine_in(dir.path());
        let actions = heraclitus_query::execute("MATCH (n:Action) RETURN n", &engine2).unwrap();
        assert_eq!(actions.as_array().unwrap().len(), fired, "replay reproduces decisions");

        // Deciding again on the reopened engine is idempotent.
        let v2 = heraclitus_query::execute("DECIDE ()", &engine2).unwrap();
        assert!(v2["fired"].as_array().unwrap().is_empty(), "no duplicate actions after replay");
        assert_eq!(v2["skipped"].as_array().unwrap().len(), fired);
    }

    #[test]
    fn m16_simulate_does_not_touch_the_real_engine() {
        // M16 GATE: a counterfactual on the real engine changes the observed
        // result but leaves the base graph and the log untouched.
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_in(dir.path());
        let edge = |from: &str, to: &str| {
            let mut e = Episode::new("ag", EventKind::Observation, vec![]);
            e.attrs.insert("edge_from".into(), from.into());
            e.attrs.insert("edge_to".into(), to.into());
            e.attrs.insert("edge_type".into(), "socio_de".into());
            e
        };
        for (a, b) in [("A1", "A2"), ("A2", "A3"), ("A3", "A1"), ("B1", "B2"), ("A1", "B1")] {
            engine.append(edge(a, b)).unwrap();
        }
        let head_before = engine.snapshot();
        let real = heraclitus_query::execute("COMMUNITY (\"A1\")", &engine).unwrap();
        assert_eq!(real["members"].as_array().unwrap().len(), 5, "A1..A3 + B1,B2 joined");

        // Counterfactual removal splits the community.
        let cf = heraclitus_query::execute(
            "SIMULATE REMOVE EDGE (\"A1\", \"B1\", \"socio_de\") THEN COMMUNITY (\"A1\")",
            &engine,
        )
        .unwrap();
        assert_eq!(cf["members"].as_array().unwrap().len(), 3, "bridge removed in the counterfactual");

        // Base + log untouched.
        let real_again = heraclitus_query::execute("COMMUNITY (\"A1\")", &engine).unwrap();
        assert_eq!(real_again["members"].as_array().unwrap().len(), 5);
        assert_eq!(engine.snapshot(), head_before, "the log head did not move");
    }

    #[test]
    fn m17_adapt_learns_and_is_replay_stable() {
        // M17 GATE: ADAPT learns a better threshold from feedback on the engine,
        // and a reopened engine (replay) learns the exact same rule.
        let dir = tempfile::tempdir().unwrap();
        let feedback = |score: &str, verdict: &str| {
            let mut e = Episode::new("analyst", EventKind::Observation, vec![]);
            e.attrs.insert("feedback_rule".into(), "flag_anomaly".into());
            e.attrs.insert("score".into(), score.into());
            e.attrs.insert("verdict".into(), verdict.into());
            e
        };
        let learned = {
            let engine = engine_in(dir.path());
            for (s, v) in [("3.0", "confirm"), ("2.0", "confirm"), ("1.6", "reject"), ("1.0", "reject")] {
                engine.append(feedback(s, v)).unwrap();
            }
            let r = heraclitus_query::execute("ADAPT ()", &engine).unwrap();
            assert!(r["adapted"]["f1"].as_f64().unwrap() > r["default"]["f1"].as_f64().unwrap());
            r["learned_threshold"].as_f64().unwrap()
        };

        // Reopen and re-learn: replay yields the identical rule.
        let engine2 = engine_in(dir.path());
        let r2 = heraclitus_query::execute("ADAPT ()", &engine2).unwrap();
        assert_eq!(r2["learned_threshold"].as_f64().unwrap(), learned, "replay learns the same rule");
    }

    #[test]
    fn m18_require_lsn_contract_on_the_engine() {
        // M18 GATE: read-your-writes via the consistency contract. After N
        // appends, REQUIRE LSN >= N succeeds and REQUIRE LSN >= N+1 fails.
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_in(dir.path());
        for i in 0..3 {
            engine
                .append(Episode::new("ag", EventKind::Observation, format!("e{i}").into_bytes()))
                .unwrap();
        }
        let head = engine.snapshot();
        assert_eq!(head, 3);

        let ok = heraclitus_query::execute(&format!("REQUIRE LSN >= {head} MATCH (n) RETURN n"), &engine).unwrap();
        assert_eq!(ok.as_array().unwrap().len(), 3);

        let err = heraclitus_query::execute(
            &format!("REQUIRE LSN >= {} MATCH (n) RETURN n", head + 1),
            &engine,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("consistency requirement not met"));
    }

    #[test]
    fn attr_index_resolves_equality_and_matches_reference() {
        // O índice secundário: `MATCH (n) WHERE n.cnpj = "X"` resolve pelo índice
        // (não por scan) e devolve exatamente os mesmos nós que a referência.
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_in(dir.path());
        for i in 0..500u64 {
            let mut e = Episode::new("etl", EventKind::Observation, format!("emp {i}").into_bytes());
            let cnpj = if i % 50 == 7 { "11222333000144".to_string() } else { format!("{i:014}") };
            e.attrs.insert("cnpj".into(), cnpj);
            e.attrs.insert("uf".into(), "MG".into());
            engine.append(e).unwrap();
        }
        let q = r#"MATCH (n) WHERE n.cnpj = "11222333000144" RETURN n"#;
        let via_engine = heraclitus_query::execute(q, &engine).unwrap();
        // 10 ocorrências (i = 7,57,…,457)
        assert_eq!(via_engine.as_array().unwrap().len(), 10);

        // índice == scan de referência (mesmas linhas, mesma ordem)
        let be = LogBackend::new(engine.log.clone());
        let via_ref = heraclitus_query::execute(q, &be).unwrap();
        assert_eq!(via_engine, via_ref, "índice deve igualar o scan de referência");

        // campo arbitrário também é indexado (uf), e valor inexistente => vazio
        assert_eq!(
            heraclitus_query::execute(r#"MATCH (n) WHERE n.uf = "MG" RETURN n"#, &engine)
                .unwrap().as_array().unwrap().len(),
            500
        );
        assert!(heraclitus_query::execute(r#"MATCH (n) WHERE n.cnpj = "0000" RETURN n"#, &engine)
            .unwrap().as_array().unwrap().is_empty());
    }
}
