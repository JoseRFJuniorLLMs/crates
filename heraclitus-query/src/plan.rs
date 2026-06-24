//! Rule-based planner v0 + EXPLAIN + executor.
//!
//! Per-field counters are collected at execution time so a later cost-based
//! planner has statistics to feed on (§3.12).

use crate::ast::*;
use crate::backend::{graph_snapshot, materialize_virtual, EdgeRow, QueryBackend, VirtualBackend};
use crate::fusion::FusionWeights;
use heraclitus_core::{Episode, EventKind, HeraclitusError, Lsn};
use heraclitus_index_graph::decision::DecisionPolicy;
use serde_json::{json, Value as Json};

#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    ScanFilter {
        label: Option<String>,
        conditions: Vec<(BoolOp, Condition)>,
        as_of: Option<AsOf>,
        order_by: Option<(String, bool)>,
        limit: Option<u32>,
    },
    Recall {
        text: String,
        k: u32,
        as_of: Option<AsOf>,
    },
    Nearest {
        vector: Vec<f32>,
        k: u32,
        as_of: Option<AsOf>,
    },
    Provenance {
        id: String,
    },
    GraphMatch {
        from_var: String,
        to_var: String,
        rel_var: String,
        rel_type: Option<String>,
        conditions: Vec<(BoolOp, Condition)>,
        as_of: Option<AsOf>,
        returns: Vec<RetItem>,
        limit: Option<u32>,
    },
    Neighbors {
        node: String,
        etype: Option<String>,
        as_of: Option<AsOf>,
    },
    Traverse {
        start: String,
        max_depth: u32,
        as_of: Option<AsOf>,
    },
    Fuse {
        text: String,
        vector: Vec<f32>,
        node: String,
        k: u32,
        as_of: Option<AsOf>,
    },
    Resolve {
        key: String,
        as_of: Option<AsOf>,
    },
    Cluster {
        entity: String,
        as_of: Option<AsOf>,
    },
    Hypotheses {
        from: String,
        to: String,
        etype: String,
        as_of: Option<AsOf>,
    },
    Why {
        target: String,
        max_depth: Option<u32>,
        as_of: Option<AsOf>,
    },
    Community {
        node: String,
        as_of: Option<AsOf>,
    },
    Metrics {
        node: String,
        as_of: Option<AsOf>,
    },
    Decide {
        as_of: Option<AsOf>,
    },
    Simulate {
        op: SimulateOp,
        from: String,
        to: String,
        etype: String,
        then: Box<Plan>,
    },
    Adapt {
        as_of: Option<AsOf>,
    },
    Append {
        label: Option<String>,
        props: Vec<(String, Value)>,
    },
}

/// Audit #4: AS OF TIMESTAMP is NOT an LSN. Resolution happens at
/// execution time through `QueryBackend::lsn_for_timestamp`.
fn resolve_as_of(a: &Option<AsOf>, be: &dyn QueryBackend) -> Result<Option<Lsn>, HeraclitusError> {
    Ok(match a {
        Some(AsOf::Lsn(n)) => Some(*n),
        Some(AsOf::Timestamp(t)) => Some(be.lsn_for_timestamp(*t)?),
        None => None,
    })
}

pub fn plan(stmt: &Stmt) -> Plan {
    match stmt {
        // M9: a relationship pattern lowers to a graph match, not a scan.
        Stmt::Match(m) if m.edge.is_some() => {
            let e = m.edge.as_ref().unwrap();
            Plan::GraphMatch {
                from_var: m.var.clone(),
                to_var: e.to_var.clone(),
                rel_var: e.rel_var.clone(),
                rel_type: e.rel_type.clone(),
                conditions: m.conditions.clone(),
                as_of: m.as_of,
                returns: m.returns.clone(),
                limit: m.limit,
            }
        }
        Stmt::Match(m) => Plan::ScanFilter {
            label: m.label.clone(),
            conditions: m.conditions.clone(),
            as_of: m.as_of,
            order_by: m.order_by.clone(),
            limit: m.limit,
        },
        Stmt::Recall { text, k, as_of } => Plan::Recall {
            text: text.clone(),
            k: *k,
            as_of: *as_of,
        },
        Stmt::Nearest { vector, k, as_of } => Plan::Nearest {
            vector: vector.clone(),
            k: *k,
            as_of: *as_of,
        },
        Stmt::Provenance { id } => Plan::Provenance { id: id.clone() },
        Stmt::Neighbors { node, etype, as_of } => Plan::Neighbors {
            node: node.clone(),
            etype: etype.clone(),
            as_of: *as_of,
        },
        Stmt::Traverse {
            start,
            max_depth,
            as_of,
        } => Plan::Traverse {
            start: start.clone(),
            max_depth: *max_depth,
            as_of: *as_of,
        },
        Stmt::Fuse {
            text,
            vector,
            node,
            k,
            as_of,
        } => Plan::Fuse {
            text: text.clone(),
            vector: vector.clone(),
            node: node.clone(),
            k: *k,
            as_of: *as_of,
        },
        Stmt::Resolve { key, as_of } => Plan::Resolve {
            key: key.clone(),
            as_of: *as_of,
        },
        Stmt::Cluster { entity, as_of } => Plan::Cluster {
            entity: entity.clone(),
            as_of: *as_of,
        },
        Stmt::Hypotheses {
            from,
            to,
            etype,
            as_of,
        } => Plan::Hypotheses {
            from: from.clone(),
            to: to.clone(),
            etype: etype.clone(),
            as_of: *as_of,
        },
        Stmt::Why {
            target,
            max_depth,
            as_of,
        } => Plan::Why {
            target: target.clone(),
            max_depth: *max_depth,
            as_of: *as_of,
        },
        Stmt::Community { node, as_of } => Plan::Community {
            node: node.clone(),
            as_of: *as_of,
        },
        Stmt::Metrics { node, as_of } => Plan::Metrics {
            node: node.clone(),
            as_of: *as_of,
        },
        Stmt::Decide { as_of } => Plan::Decide { as_of: *as_of },
        Stmt::Simulate {
            op,
            from,
            to,
            etype,
            then,
        } => Plan::Simulate {
            op: *op,
            from: from.clone(),
            to: to.clone(),
            etype: etype.clone(),
            then: Box::new(plan(then)),
        },
        Stmt::Adapt { as_of } => Plan::Adapt { as_of: *as_of },
        Stmt::Create(c) => Plan::Append {
            label: c.label.clone(),
            props: c.props.clone(),
        },
    }
}

/// Render a plan as indented text — the EXPLAIN output.
pub fn render(plan: &Plan) -> String {
    match plan {
        Plan::ScanFilter {
            label,
            conditions,
            as_of,
            order_by,
            limit,
        } => {
            let mut s = String::from("ScanFilter\n");
            if let Some(l) = label {
                s += &format!("  label = {l}\n");
            }
            for (op, c) in conditions {
                s += &format!("  cond[{op:?}] {:?} {:?} {:?}\n", c.lhs, c.cmp, c.rhs);
            }
            if let Some(a) = as_of {
                s += &format!("  AsOf({a:?})\n");
            }
            if let Some((f, asc)) = order_by {
                s += &format!("  OrderBy({f}, asc={asc})\n");
            }
            if let Some(l) = limit {
                s += &format!("  Limit({l})\n");
            }
            s
        }
        Plan::Recall { text, k, as_of } => {
            format!("Recall(two-stage)\n  text = {text:?}\n  k = {k}\n  as_of = {as_of:?}\n")
        }
        Plan::Nearest { vector, k, as_of } => format!(
            "Nearest(ANN)\n  dims = {}\n  k = {k}\n  as_of = {as_of:?}\n",
            vector.len()
        ),
        Plan::Provenance { id } => format!("ProvenanceExpand\n  id = {id}\n"),
        Plan::GraphMatch {
            from_var,
            to_var,
            rel_var,
            rel_type,
            conditions,
            as_of,
            limit,
            ..
        } => {
            let mut s = format!(
                "GraphMatch\n  ({from_var})-[{rel_var}{}]->({to_var})\n",
                rel_type
                    .as_ref()
                    .map(|t| format!(":{t}"))
                    .unwrap_or_default()
            );
            for (op, c) in conditions {
                s += &format!("  cond[{op:?}] {:?} {:?} {:?}\n", c.lhs, c.cmp, c.rhs);
            }
            if let Some(a) = as_of {
                s += &format!("  AsOf({a:?})\n");
            }
            if let Some(l) = limit {
                s += &format!("  Limit({l})\n");
            }
            s
        }
        Plan::Neighbors { node, etype, as_of } => format!(
            "GraphNeighbors\n  node = {node}\n  etype = {etype:?}\n  as_of = {as_of:?}\n"
        ),
        Plan::Traverse { start, max_depth, as_of } => format!(
            "GraphTraverse(BFS)\n  start = {start}\n  max_depth = {max_depth}\n  as_of = {as_of:?}\n"
        ),
        Plan::Fuse { text, vector, node, k, as_of } => format!(
            "HybridFusion(graph+vector+text)\n  anchor = {node}\n  text = {text:?}\n  dims = {}\n  k = {k}\n  as_of = {as_of:?}\n",
            vector.len()
        ),
        Plan::Resolve { key, as_of } => {
            format!("EntityResolve\n  key = {key}\n  as_of = {as_of:?}\n")
        }
        Plan::Cluster { entity, as_of } => {
            format!("EntityCluster\n  entity = {entity}\n  as_of = {as_of:?}\n")
        }
        Plan::Hypotheses { from, to, etype, as_of } => format!(
            "HypothesisGraph\n  edge = ({from})-[{etype}]->({to})\n  as_of = {as_of:?}\n"
        ),
        Plan::Why { target, max_depth, as_of } => format!(
            "CausalTrace(WHY)\n  target = {target}\n  max_depth = {max_depth:?}\n  as_of = {as_of:?}\n"
        ),
        Plan::Community { node, as_of } => {
            format!("GraphCommunity\n  node = {node}\n  as_of = {as_of:?}\n")
        }
        Plan::Metrics { node, as_of } => {
            format!("GraphMetrics\n  node = {node}\n  as_of = {as_of:?}\n")
        }
        Plan::Decide { as_of } => format!("DecisionEngine(act)\n  as_of = {as_of:?}\n"),
        Plan::Adapt { as_of } => format!("AdaptiveLearner\n  as_of = {as_of:?}\n"),
        Plan::Simulate { op, from, to, etype, then } => {
            let inner = render(then)
                .lines()
                .map(|l| format!("    {l}"))
                .collect::<Vec<_>>()
                .join("\n");
            format!(
                "Counterfactual({op:?})\n  edge = ({from})-[{etype}]->({to})\n  then =\n{inner}\n"
            )
        }
        Plan::Append { label, props } => {
            format!(
                "Append(log)\n  label = {label:?}\n  props = {}\n",
                props.len()
            )
        }
    }
}

fn episode_to_json(lsn: Lsn, e: &Episode) -> Json {
    json!({
        "lsn": lsn,
        "id": e.id.to_string(),
        "agent_id": e.agent_id,
        "session_id": e.session_id,
        "kind": format!("{:?}", e.kind),
        "content": String::from_utf8_lossy(&e.content),
        "attrs": e.attrs,
        "ts_hlc": e.ts_hlc,
        // Provenance parents travel with the node so a windowed graph can draw
        // its edges with zero extra queries (the on-demand dashboard relies on
        // this: in-window edges are free, no per-node provenance lookup).
        "parents": e.parents.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
    })
}

/// Rótulo limpo do kind para filtros/labels: variantes conhecidas → o seu nome,
/// `Custom(s)` → a string interna `s`. Sem isto, `Custom("ItemLicitacao")`
/// formatava-se como `Custom("ItemLicitacao")` e nunca casava com `MATCH
/// (n:ItemLicitacao)` nem com `WHERE n.kind = "ItemLicitacao"` (kinds Custom
/// — gerados pela ingestão de dados — eram invisíveis às queries por tipo).
fn kind_label(k: &EventKind) -> String {
    match k {
        EventKind::Custom(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

fn field_of(lsn: Lsn, e: &Episode, field: &str) -> Option<Json> {
    match field {
        "lsn" => Some(json!(lsn)),
        "id" => Some(json!(e.id.to_string())),
        "agent_id" => Some(json!(e.agent_id)),
        "session_id" => Some(json!(e.session_id)),
        // `tipo` é alias de `kind`; ambos devolvem o rótulo limpo (ver kind_label).
        "kind" | "tipo" => Some(json!(kind_label(&e.kind))),
        "content" => Some(json!(String::from_utf8_lossy(&e.content))),
        "ts_hlc" => Some(json!(e.ts_hlc)),
        other => e.attrs.get(other).map(|v| json!(v)),
    }
}

fn eval_operand(op: &Operand, lsn: Lsn, e: &Episode) -> Option<Json> {
    match op {
        Operand::Prop(_, field) => field_of(lsn, e, field),
        Operand::Ident(field) => field_of(lsn, e, field),
        Operand::Num(n) => Some(json!(n)),
        Operand::Str(s) => Some(json!(s)),
    }
}

fn cmp_json(a: &Json, b: &Json, cmp: Cmp) -> bool {
    let ord = match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => x.partial_cmp(&y),
        _ => a.as_str().and_then(|x| b.as_str().map(|y| x.cmp(y))),
    };
    match (ord, cmp) {
        (Some(o), Cmp::Eq) => o.is_eq(),
        (Some(o), Cmp::Ne) => !o.is_eq(),
        (Some(o), Cmp::Gt) => o.is_gt(),
        (Some(o), Cmp::Lt) => o.is_lt(),
        (Some(o), Cmp::Ge) => o.is_ge(),
        (Some(o), Cmp::Le) => o.is_le(),
        (None, Cmp::Ne) => true,
        (None, _) => false,
    }
}

fn matches(conditions: &[(BoolOp, Condition)], lsn: Lsn, e: &Episode) -> bool {
    let mut acc = true;
    for (op, c) in conditions {
        let lhs = eval_operand(&c.lhs, lsn, e);
        let rhs = eval_operand(&c.rhs, lsn, e);
        let hit = match (lhs, rhs) {
            (Some(a), Some(b)) => cmp_json(&a, &b, c.cmp),
            _ => false,
        };
        acc = match op {
            BoolOp::First => hit,
            BoolOp::And => acc && hit,
            BoolOp::Or => acc || hit,
        };
    }
    acc
}

/// Push a pattern-variable equality (`var = "x"` or `var.field = "x"`) from the
/// WHERE clause down into the graph query. Returns the literal if found.
fn eq_filter(conditions: &[(BoolOp, Condition)], var: &str, field: &str) -> Option<String> {
    fn var_lit(a: &Operand, b: &Operand, var: &str, field: &str) -> Option<String> {
        let is_var = match a {
            Operand::Ident(v) => v == var,
            Operand::Prop(v, f) => v == var && f == field,
            _ => false,
        };
        match (is_var, b) {
            (true, Operand::Str(s)) => Some(s.clone()),
            _ => None,
        }
    }
    conditions.iter().find_map(|(_, c)| {
        if c.cmp != Cmp::Eq {
            return None;
        }
        var_lit(&c.lhs, &c.rhs, var, field).or_else(|| var_lit(&c.rhs, &c.lhs, var, field))
    })
}

fn is_lsn_operand(op: &Operand) -> bool {
    matches!(op, Operand::Ident(f) if f == "lsn")
        || matches!(op, Operand::Prop(_, f) if f == "lsn")
}

fn flip_cmp(c: Cmp) -> Cmp {
    match c {
        Cmp::Gt => Cmp::Lt,
        Cmp::Lt => Cmp::Gt,
        Cmp::Ge => Cmp::Le,
        Cmp::Le => Cmp::Ge,
        other => other, // Eq / Ne are symmetric
    }
}

/// Campos servidos diretamente do envelope do evento (não são `attrs`), logo
/// não passam pelo índice de atributos.
fn is_builtin_field(f: &str) -> bool {
    matches!(f, "lsn" | "id" | "agent_id" | "session_id" | "kind" | "tipo" | "content")
}

/// Se o `WHERE` for conjuntivo e fixar `n.<campo> = "valor"` (igualdade, valor
/// string, campo não-builtin), devolve `(campo, valor)` para o índice de
/// atributos resolver. `None` ⇒ usar o scan por janela.
fn attr_eq_hint(conditions: &[(BoolOp, Condition)]) -> Option<(String, String)> {
    if conditions.iter().any(|(op, _)| matches!(op, BoolOp::Or)) {
        return None; // um OR partiria a semântica de um único lookup
    }
    for (_, c) in conditions {
        if c.cmp != Cmp::Eq {
            continue;
        }
        let pair = match (&c.lhs, &c.rhs) {
            (Operand::Prop(_, f), Operand::Str(v)) => Some((f.clone(), v.clone())),
            (Operand::Str(v), Operand::Prop(_, f)) => Some((f.clone(), v.clone())),
            _ => None,
        };
        if let Some((f, v)) = pair {
            if !is_builtin_field(&f) {
                return Some((f, v));
            }
        }
    }
    None
}

/// Derive the LSN scan window `[lo, hi)` from the `WHERE` clause and the `AS OF`
/// bound, so `MATCH` pushes a time filter down to a pruned, capped scan
/// (§query guard). Only conjunctive (`AND`) numeric `n.lsn` comparisons are
/// pushed; anything with an `OR` (or no lsn bound) falls back to the full
/// `[0, bound)` window — still capped by the backend. The post-scan `matches`
/// re-checks every condition, so a too-wide window can never return wrong rows;
/// only a *too-narrow* window would, which is why the rules below are exact.
fn lsn_window(conditions: &[(BoolOp, Condition)], bound: Option<Lsn>) -> (Lsn, Lsn) {
    let mut lo: Lsn = 0;
    let mut hi: Lsn = bound.unwrap_or(u64::MAX);
    if conditions.iter().any(|(op, _)| matches!(op, BoolOp::Or)) {
        return (lo, hi);
    }
    for (_, c) in conditions {
        let (cmp, n) = if is_lsn_operand(&c.lhs) {
            match &c.rhs {
                Operand::Num(n) => (c.cmp, *n),
                _ => continue,
            }
        } else if is_lsn_operand(&c.rhs) {
            match &c.lhs {
                Operand::Num(n) => (flip_cmp(c.cmp), *n),
                _ => continue,
            }
        } else {
            continue;
        };
        let n = if n < 0.0 { 0u64 } else { n as u64 };
        match cmp {
            Cmp::Ge => lo = lo.max(n),
            Cmp::Gt => lo = lo.max(n.saturating_add(1)),
            Cmp::Le => hi = hi.min(n.saturating_add(1)),
            Cmp::Lt => hi = hi.min(n),
            Cmp::Eq => {
                lo = lo.max(n);
                hi = hi.min(n.saturating_add(1));
            }
            Cmp::Ne => {}
        }
    }
    if lo >= hi {
        (lo, lo) // empty window
    } else {
        (lo, hi)
    }
}

fn project_edge(
    r: &EdgeRow,
    returns: &[RetItem],
    from_var: &str,
    to_var: &str,
    rel_var: &str,
) -> Json {
    // Star (or an empty RETURN) yields the whole edge.
    if returns.is_empty() || returns.iter().any(|i| matches!(i, RetItem::Star)) {
        return json!({
            "from": r.from, "to": r.to,
            "edge_id": r.edge_id, "etype": r.etype, "belief": r.belief,
        });
    }
    let mut obj = serde_json::Map::new();
    for item in returns {
        match item {
            RetItem::Star => {}
            RetItem::Ident(v) => {
                let val = if v == from_var {
                    json!(r.from)
                } else if v == to_var {
                    json!(r.to)
                } else if v == rel_var {
                    json!(r.edge_id)
                } else {
                    Json::Null
                };
                obj.insert(v.clone(), val);
            }
            RetItem::Prop(v, f) => {
                let val = if v == rel_var {
                    match f.as_str() {
                        "type" => json!(r.etype),
                        "belief" => json!(r.belief),
                        "id" => json!(r.edge_id),
                        _ => Json::Null,
                    }
                } else if v == from_var && f == "id" {
                    json!(r.from)
                } else if v == to_var && f == "id" {
                    json!(r.to)
                } else {
                    Json::Null
                };
                obj.insert(format!("{v}.{f}"), val);
            }
        }
    }
    Json::Object(obj)
}

pub fn execute(plan: &Plan, be: &dyn QueryBackend) -> Result<Json, HeraclitusError> {
    match plan {
        Plan::ScanFilter {
            label,
            conditions,
            as_of,
            order_by,
            limit,
        } => {
            let bound = resolve_as_of(as_of, be)?;
            // ÍNDICE SECUNDÁRIO: se o WHERE (tudo AND) fixa `n.<campo> = "v"` num
            // campo não-builtin, resolve pelo índice de atributos (global,
            // O(postings)) em vez de varrer a janela capada. O pós-filtro
            // `matches` revalida tudo, por isso a correção nunca depende disto.
            let candidates: Vec<(Lsn, Episode)> = match attr_eq_hint(conditions) {
                Some((field, value)) => match be.attr_lookup(&field, &value, bound)? {
                    Some(hit) => hit,
                    None => {
                        let (lo, hi) = lsn_window(conditions, bound);
                        be.scan_range(lo, hi)?
                    }
                },
                None => {
                    // Push any `n.lsn` bounds down to a pruned, capped scan window.
                    let (lo, hi) = lsn_window(conditions, bound);
                    be.scan_range(lo, hi)?
                }
            };
            let mut rows: Vec<(Lsn, Episode)> = candidates
                .into_iter()
                .filter(|(_, e)| {
                    label
                        .as_ref()
                        .map(|l| kind_label(&e.kind).eq_ignore_ascii_case(l))
                        .unwrap_or(true)
                })
                .filter(|(l, e)| matches(conditions, *l, e))
                .collect();
            if let Some((field, asc)) = order_by {
                rows.sort_by(|(la, ea), (lb, eb)| {
                    let a = field_of(*la, ea, field).unwrap_or(Json::Null).to_string();
                    let b = field_of(*lb, eb, field).unwrap_or(Json::Null).to_string();
                    if *asc {
                        a.cmp(&b)
                    } else {
                        b.cmp(&a)
                    }
                });
            }
            if let Some(l) = limit {
                rows.truncate(*l as usize);
            }
            Ok(Json::Array(
                rows.iter().map(|(l, e)| episode_to_json(*l, e)).collect(),
            ))
        }
        Plan::Recall { text, k, as_of } => {
            let hits = be.recall(text, *k as usize, resolve_as_of(as_of, be)?)?;
            Ok(Json::Array(
                hits.iter()
                    .map(|(l, e, score)| {
                        let mut j = episode_to_json(*l, e);
                        j["score"] = json!(score);
                        j
                    })
                    .collect(),
            ))
        }
        Plan::Nearest { vector, k, as_of } => {
            let hits = be.nearest(vector, *k as usize, resolve_as_of(as_of, be)?)?;
            Ok(Json::Array(
                hits.iter()
                    .map(|(l, e, dist)| {
                        let mut j = episode_to_json(*l, e);
                        j["dist"] = json!(dist);
                        j
                    })
                    .collect(),
            ))
        }
        Plan::Provenance { id } => {
            let parents = be.provenance(id)?;
            Ok(Json::Array(parents.into_iter().map(|p| json!(p)).collect()))
        }
        Plan::GraphMatch {
            from_var,
            to_var,
            rel_var,
            rel_type,
            conditions,
            as_of,
            returns,
            limit,
        } => {
            let bound = resolve_as_of(as_of, be)?;
            // Pattern variables constrained in WHERE become graph filters; the
            // inline `[r:type]` label is the default edge type (a WHERE
            // `r.type = ...` overrides it).
            let src = eq_filter(conditions, from_var, "id");
            let dst = eq_filter(conditions, to_var, "id");
            let etype = rel_type
                .clone()
                .or_else(|| eq_filter(conditions, rel_var, "type"));
            let rows = be.match_edges(src.as_deref(), etype.as_deref(), dst.as_deref(), bound)?;
            let mut out: Vec<Json> = rows
                .iter()
                .map(|r| project_edge(r, returns, from_var, to_var, rel_var))
                .collect();
            if let Some(l) = limit {
                out.truncate(*l as usize);
            }
            Ok(Json::Array(out))
        }
        Plan::Neighbors { node, etype, as_of } => {
            let bound = resolve_as_of(as_of, be)?;
            let rows = be.neighbors(node, etype.as_deref(), bound, 0.0)?;
            Ok(Json::Array(
                rows.into_iter()
                    .map(|n| {
                        json!({
                            "edge_id": n.edge_id,
                            "to": n.to,
                            "etype": n.etype,
                            "belief": n.belief,
                            "weight": n.weight,
                            "lsn": n.lsn,
                        })
                    })
                    .collect(),
            ))
        }
        Plan::Traverse {
            start,
            max_depth,
            as_of,
        } => {
            let bound = resolve_as_of(as_of, be)?;
            let rows = be.traverse(start, *max_depth as usize, bound, 0.0)?;
            Ok(Json::Array(
                rows.into_iter()
                    .map(|(node, depth)| json!({ "node": node, "depth": depth }))
                    .collect(),
            ))
        }
        Plan::Fuse {
            text,
            vector,
            node,
            k,
            as_of,
        } => {
            let bound = resolve_as_of(as_of, be)?;
            let hits = be.find_fused(
                text,
                vector,
                node,
                FusionWeights::default(),
                *k as usize,
                bound,
            )?;
            Ok(Json::Array(
                hits.into_iter()
                    .map(|h| {
                        json!({
                            "id": h.id,
                            "lsn": h.lsn,
                            "score": h.score,
                            "graph_score": h.input.graph_score,
                            "vector_score": h.input.vector_score,
                            "text_score": h.input.text_score,
                        })
                    })
                    .collect(),
            ))
        }
        Plan::Resolve { key, as_of } => {
            let bound = resolve_as_of(as_of, be)?;
            let entity = be.resolve_entity(key, bound)?;
            Ok(json!({ "key": key, "entity_id": entity }))
        }
        Plan::Cluster { entity, as_of } => {
            let bound = resolve_as_of(as_of, be)?;
            let keys = be.entity_cluster(entity, bound)?;
            Ok(Json::Array(keys.into_iter().map(|k| json!(k)).collect()))
        }
        Plan::Community { node, as_of } => {
            let bound = resolve_as_of(as_of, be)?;
            match be.community(node, bound)? {
                None => Ok(Json::Null),
                Some(c) => Ok(json!({
                    "node": c.node,
                    "community": c.community,
                    "members": c.members,
                })),
            }
        }
        Plan::Metrics { node, as_of } => {
            let bound = resolve_as_of(as_of, be)?;
            match be.node_metrics(node, bound)? {
                None => Ok(Json::Null),
                Some(m) => Ok(json!({
                    "node": m.node,
                    "community": m.community,
                    "degree": m.degree,
                    "centrality": m.centrality,
                    "anomaly_score": m.anomaly_score,
                })),
            }
        }
        Plan::Simulate {
            op,
            from,
            to,
            etype,
            then,
        } => {
            // Overlay the counterfactual on a fresh copy of the graph; the base
            // graph and the log are never touched (divergence isolated).
            let base = graph_snapshot(be)?;
            let virt = materialize_virtual(&base, *op, from, to, etype);
            let vb = VirtualBackend::new(be, virt);
            execute(then, &vb)
        }
        Plan::Adapt { as_of } => {
            let bound = resolve_as_of(as_of, be)?;
            let r = be.adapt(bound)?;
            let eval = |e: &heraclitus_index_graph::adaptive::PolicyEval| {
                json!({
                    "threshold": e.threshold,
                    "precision": e.precision,
                    "recall": e.recall,
                    "f1": e.f1,
                })
            };
            Ok(json!({
                "rule": r.rule,
                "samples": r.samples,
                "learned_threshold": r.learned_threshold,
                "default": eval(&r.default),
                "adapted": eval(&r.adapted),
            }))
        }
        Plan::Decide { as_of } => {
            let bound = resolve_as_of(as_of, be)?;
            let report = be.decide(DecisionPolicy::default(), bound)?;
            Ok(json!({
                "fired": report.fired.into_iter().map(|a| json!({
                    "action_id": a.action_id,
                    "rule": a.rule,
                    "subject": a.subject,
                    "reason": a.reason,
                    "lsn": a.lsn,
                })).collect::<Vec<_>>(),
                "skipped": report.skipped,
            }))
        }
        Plan::Why {
            target,
            max_depth,
            as_of,
        } => {
            let bound = resolve_as_of(as_of, be)?;
            let trace = be.why(target, max_depth.unwrap_or(64) as usize, bound)?;
            Ok(json!({
                "target": trace.target,
                "roots": trace.roots,
                "steps": trace.steps.into_iter().map(|s| json!({
                    "id": s.id,
                    "depth": s.depth,
                    "causes": s.causes,
                })).collect::<Vec<_>>(),
            }))
        }
        Plan::Hypotheses {
            from,
            to,
            etype,
            as_of,
        } => {
            let bound = resolve_as_of(as_of, be)?;
            match be.edge_hypotheses(from, to, etype, bound)? {
                None => Ok(Json::Null),
                Some(h) => Ok(json!({
                    "edge_id": h.edge_id,
                    "alive": h.alive,
                    "belief": h.belief,
                    "hypotheses": h.versions.into_iter().map(|v| json!({
                        "hypothesis_id": v.hypothesis_id,
                        "confidence": v.confidence,
                        "polarity": v.polarity,
                        "source": v.source,
                    })).collect::<Vec<_>>(),
                })),
            }
        }
        Plan::Append { label, props } => {
            let lsn = be.append(label.as_deref(), props)?;
            Ok(json!({ "lsn": lsn }))
        }
    }
}
