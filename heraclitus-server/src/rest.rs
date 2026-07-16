//! Minimal admin REST (axum) — a thin layer over the same engine.

use crate::engine::Engine;
use axum::{
    extract::{Path, Request, State},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::get,
    Json, Router,
};
use std::sync::Arc;

/// Comparação em tempo constante (R17): o tempo não depende do prefixo
/// coincidente, fechando o side-channel de timing do `==` de strings. O
/// comprimento continua observável — inevitável e inócuo (o segredo não é o
/// comprimento). Partilhada pelo Basic (REST) e pelo Bearer (gRPC, `lib.rs`).
pub(crate) fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Base64 padrão (RFC 4648, com padding) — só para montar o valor esperado do
/// header `Authorization: Basic ...`; evita puxar uma dependência para 15 linhas.
fn b64(input: &[u8]) -> String {
    const AB: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(AB[(n >> 18) as usize & 63] as char);
        out.push(AB[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            AB[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            AB[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Constrói o router; com `basic_auth = Some("user:pass")` TODAS as rotas
/// exigem `Authorization: Basic ...` (comparação de string constante contra o
/// valor esperado — nunca se descodifica input do cliente).
pub fn router(engine: Arc<Engine>, basic_auth: Option<String>) -> Router {
    let routes = Router::new()
        .route("/healthz", get(healthz))
        .route("/stats", get(stats))
        .route("/state", get(state))
        .route("/verify", get(verify))
        .route("/verify/:segment", get(verify_segment))
        // M20 — H-VM sovereignty ledger (SPEC-025-adjacente). KV durável no log.
        .route("/hvm/state", get(hvm_state))
        .route("/hvm/upsert", axum::routing::post(hvm_upsert))
        .route("/hvm/delete", axum::routing::post(hvm_delete))
        .route("/hvm/checkpoint", axum::routing::post(hvm_checkpoint));
    // SPEC-016 (feature `analytics`): data plane Flight — o log inteiro como um
    // stream Arrow IPC, legível por pyarrow/Polars/DuckDB sem parsing por linha.
    #[cfg(feature = "analytics")]
    let routes = routes
        .route("/flight/events", get(flight_events))
        .route("/sql", axum::routing::post(sql));
    let routes = routes.with_state(engine);

    match basic_auth {
        None => routes,
        Some(creds) => {
            let expected: Arc<String> = Arc::new(format!("Basic {}", b64(creds.as_bytes())));
            routes.layer(middleware::from_fn(move |req: Request, next: Next| {
                let expected = expected.clone();
                async move {
                    let ok = req
                        .headers()
                        .get(header::AUTHORIZATION)
                        .and_then(|v| v.to_str().ok())
                        .map(|v| ct_eq(v.as_bytes(), expected.as_bytes()))
                        .unwrap_or(false);
                    if ok {
                        next.run(req).await
                    } else {
                        Response::builder()
                            .status(StatusCode::UNAUTHORIZED)
                            .header(header::WWW_AUTHENTICATE, "Basic realm=\"heraclitus\"")
                            .body("unauthorized".into())
                            .unwrap()
                    }
                }
            }))
        }
    }
}

/// `GET /flight/events[?as_of=N]` → corpo `application/vnd.apache.arrow.stream`.
#[cfg(feature = "analytics")]
async fn flight_events(
    State(engine): State<Arc<Engine>>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let as_of = q.get("as_of").and_then(|v| v.parse::<u64>().ok());
    let log = engine.log.clone();
    // Materialização em spawn_blocking: nunca no executor async.
    let body = tokio::task::spawn_blocking(move || {
        heraclitus_analytics::flight::events_as_single_ipc(&log, as_of)
    })
    .await;
    match body {
        Ok(Ok(bytes)) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/vnd.apache.arrow.stream")
            .body(bytes.into())
            .unwrap(),
        Ok(Err(e)) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(format!("flight: {e}").into())
            .unwrap(),
        Err(e) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(format!("join: {e}").into())
            .unwrap(),
    }
}

/// `POST /sql` — corpo JSON `{"sql":"SELECT ... FROM events ...","as_of":123}`
/// (`as_of` opcional). Devolve as linhas como array JSON. Feature `analytics`:
/// SQL OLAP **read-only** (DataFusion) sobre a tabela `events` materializada do
/// log — a via de agregação sancionada pela I4 (não duplicamos o DataFusion).
/// Admin-gated pela mesma Basic Auth do router.
///
/// Caveat: `LogAnalytics::from_log` materializa o log até ao head (ou `as_of`)
/// por chamada — usar `as_of` e `LIMIT`/`WHERE` para consultas grandes.
#[cfg(feature = "analytics")]
async fn sql(
    State(engine): State<Arc<Engine>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    use axum::response::IntoResponse;
    let Some(query) = body.get("sql").and_then(|v| v.as_str()).map(str::to_owned) else {
        return (StatusCode::BAD_REQUEST, "corpo requer o campo string `sql`").into_response();
    };
    let as_of = body.get("as_of").and_then(|v| v.as_u64());
    match run_sql(&engine, query, as_of).await {
        Ok(rows) => Json(serde_json::Value::Array(rows)).into_response(),
        Err((code, msg)) => (code, msg).into_response(),
    }
}

/// Núcleo testável de `POST /sql`: materializa o log em `spawn_blocking` (nunca
/// no executor async) e corre o SQL no DataFusion. Erro de SQL do utilizador =
/// 400; falha interna (scan/join) = 500.
#[cfg(feature = "analytics")]
async fn run_sql(
    engine: &Engine,
    query: String,
    as_of: Option<u64>,
) -> Result<Vec<serde_json::Value>, (StatusCode, String)> {
    let log = engine.log.clone();
    let analytics = tokio::task::spawn_blocking(move || {
        heraclitus_analytics::LogAnalytics::from_log(&log, as_of)
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("join: {e}")))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("analytics: {e}")))?;
    analytics
        .sql(&query)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("sql: {e}")))
}

// ── M20 H-VM ledger (KV soberano durável no log) ─────────────────────────────

/// Vista JSON do estado do ledger H-VM (usada pelo handler e testável sem HTTP).
/// Chaves/valores são interpretados como UTF-8 (o ledger é bytes; usa-se
/// `from_utf8_lossy`), mais os LSNs de consistência.
fn hvm_state_json(engine: &Engine) -> Result<serde_json::Value, String> {
    let state = engine.hvm_state().map_err(|e| format!("hvm: {e}"))?;
    let entries: serde_json::Map<String, serde_json::Value> = state
        .memory_layers
        .iter()
        .map(|(k, v)| {
            (
                String::from_utf8_lossy(k).into_owned(),
                serde_json::Value::String(String::from_utf8_lossy(v).into_owned()),
            )
        })
        .collect();
    Ok(serde_json::json!({
        "current_lsn": state.current_lsn,
        "max_lsn_applied": state.max_lsn_applied,
        "entries": entries,
    }))
}

/// `GET /hvm/state` → o espaço KV do ledger H-VM (M20) + os LSNs, como JSON.
async fn hvm_state(State(engine): State<Arc<Engine>>) -> Response {
    use axum::response::IntoResponse;
    // Replay do ledger é bloqueante → spawn_blocking (nunca no reactor).
    match tokio::task::spawn_blocking(move || hvm_state_json(&engine)).await {
        Ok(Ok(v)) => Json(v).into_response(),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("join: {e}")).into_response(),
    }
}

/// `POST /hvm/upsert` — corpo `{"key":"…","val":"…"}` (UTF-8) → `{"lsn":n}`.
/// Escrita no ledger (append ao log). Recusada com 409 sob replicação: o H-VM
/// ainda não passa pelo consenso e não pode divergir entre nós.
async fn hvm_upsert(State(engine): State<Arc<Engine>>, Json(body): Json<serde_json::Value>) -> Response {
    use axum::response::IntoResponse;
    if engine.is_replicated() {
        return (StatusCode::CONFLICT, "escrita H-VM não passa pelo consenso (ver P5)").into_response();
    }
    let (Some(key), Some(val)) = (
        body.get("key").and_then(|v| v.as_str()),
        body.get("val").and_then(|v| v.as_str()),
    ) else {
        return (StatusCode::BAD_REQUEST, "corpo requer os campos string `key` e `val`").into_response();
    };
    let (key, val) = (key.as_bytes().to_vec(), val.as_bytes().to_vec());
    match tokio::task::spawn_blocking(move || engine.hvm_upsert(key, val)).await {
        Ok(Ok(lsn)) => Json(serde_json::json!({ "lsn": lsn })).into_response(),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, format!("hvm: {e}")).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("join: {e}")).into_response(),
    }
}

/// `POST /hvm/delete` — corpo `{"key":"…"}` → `{"lsn":n}`.
async fn hvm_delete(State(engine): State<Arc<Engine>>, Json(body): Json<serde_json::Value>) -> Response {
    use axum::response::IntoResponse;
    if engine.is_replicated() {
        return (StatusCode::CONFLICT, "escrita H-VM não passa pelo consenso (ver P5)").into_response();
    }
    let Some(key) = body.get("key").and_then(|v| v.as_str()) else {
        return (StatusCode::BAD_REQUEST, "corpo requer o campo string `key`").into_response();
    };
    let key = key.as_bytes().to_vec();
    match tokio::task::spawn_blocking(move || engine.hvm_delete(key)).await {
        Ok(Ok(lsn)) => Json(serde_json::json!({ "lsn": lsn })).into_response(),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, format!("hvm: {e}")).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("join: {e}")).into_response(),
    }
}

/// `POST /hvm/checkpoint` (sem corpo) — materializa o ledger num Bᵋ-tree no
/// caminho do **servidor** (`<data_dir>/hvm.hbt`; NUNCA um caminho do cliente) →
/// `{"ok":true,"path":"…"}`. É o que traz o `heraclitus-btree` ao caminho vivo.
async fn hvm_checkpoint(State(engine): State<Arc<Engine>>) -> Response {
    use axum::response::IntoResponse;
    if engine.is_replicated() {
        return (StatusCode::CONFLICT, "checkpoint H-VM indisponível sob replicação (ver P5)").into_response();
    }
    match tokio::task::spawn_blocking(move || engine.hvm_checkpoint_default()).await {
        Ok(Ok(path)) => {
            Json(serde_json::json!({ "ok": true, "path": path.to_string_lossy() })).into_response()
        }
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, format!("hvm: {e}")).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("join: {e}")).into_response(),
    }
}

async fn healthz() -> &'static str {
    "panta rhei"
}

async fn stats(State(engine): State<Arc<Engine>>) -> Json<serde_json::Value> {
    Json(engine.stats())
}

/// `heraclitus_state()`: head, segmentos e watermarks — diagnóstico num GET.
async fn state(State(engine): State<Arc<Engine>>) -> Json<serde_json::Value> {
    Json(engine.state())
}

/// Verificação Merkle do log inteiro.
async fn verify(State(engine): State<Arc<Engine>>) -> Json<serde_json::Value> {
    Json(
        engine
            .verify()
            .unwrap_or_else(|e| serde_json::json!({ "error": e.to_string() })),
    )
}

/// Verificação Merkle pontual de um segmento.
async fn verify_segment(
    State(engine): State<Arc<Engine>>,
    Path(segment): Path<u64>,
) -> Json<serde_json::Value> {
    Json(
        engine
            .verify_segment(segment)
            .unwrap_or_else(|e| serde_json::json!({ "error": e.to_string() })),
    )
}

#[cfg(all(test, feature = "analytics"))]
mod sql_tests {
    use super::*;
    use heraclitus_core::{Episode, EventKind, FsyncPolicy, HeraclitusConfig};

    fn engine_in(dir: &std::path::Path) -> Engine {
        let cfg = HeraclitusConfig {
            data_dir: dir.to_path_buf(),
            fsync: FsyncPolicy::Always,
            ..Default::default()
        };
        Engine::open(&cfg).unwrap()
    }

    /// Gate: a via ligada (`run_sql`) devolve o mesmo que chamar o `LogAnalytics`
    /// de referência diretamente — o wiring nunca altera o resultado. Cobre
    /// também `as_of` e o erro 400 sem pânico.
    #[tokio::test]
    async fn post_sql_group_by_matches_reference() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_in(dir.path());
        for i in 0..12 {
            let e = Episode::new(
                if i % 3 == 0 { "alice" } else { "bob" },
                EventKind::Observation,
                format!("evento {i}").into_bytes(),
            );
            engine.append(e).unwrap();
        }
        let q = "SELECT agent_id, COUNT(*) AS n FROM events GROUP BY agent_id ORDER BY agent_id";

        // Via ligada (o que o handler POST /sql executa).
        let rows = run_sql(&engine, q.to_owned(), None).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["agent_id"], "alice");
        assert_eq!(rows[0]["n"], 4);
        assert_eq!(rows[1]["agent_id"], "bob");
        assert_eq!(rows[1]["n"], 8);

        // Referência: o mesmo SQL direto no LogAnalytics.
        let reference = {
            let log = engine.log.clone();
            let a = heraclitus_analytics::LogAnalytics::from_log(&log, None).unwrap();
            a.sql(q).await.unwrap()
        };
        assert_eq!(rows, reference, "a via ligada difere da referência");

        // Snapshot AS OF: só lsn < 6.
        let as_of = run_sql(&engine, "SELECT COUNT(*) AS n FROM events".to_owned(), Some(6))
            .await
            .unwrap();
        assert_eq!(as_of[0]["n"], 6);

        // SQL inválido = 400, nunca pânico.
        let bad = run_sql(&engine, "SELECT x FROM tabela_inexistente".to_owned(), None).await;
        assert_eq!(bad.unwrap_err().0, StatusCode::BAD_REQUEST);
    }
}

#[cfg(test)]
mod hvm_tests {
    use super::*;
    use heraclitus_core::{FsyncPolicy, HeraclitusConfig};

    fn engine_in(dir: &std::path::Path) -> Engine {
        let cfg = HeraclitusConfig {
            data_dir: dir.to_path_buf(),
            fsync: FsyncPolicy::Always,
            ..Default::default()
        };
        Engine::open(&cfg).unwrap()
    }

    /// A vista JSON que o `GET /hvm/state` serve reflete o ledger após
    /// upsert/delete — prova o núcleo do wiring do endpoint sem precisar de HTTP.
    #[test]
    fn hvm_state_json_reflects_the_ledger() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_in(dir.path());
        engine.hvm_upsert(b"user:1".to_vec(), b"alice".to_vec()).unwrap();
        engine.hvm_upsert(b"user:2".to_vec(), b"bob".to_vec()).unwrap();
        engine.hvm_delete(b"user:1".to_vec()).unwrap();

        let v = hvm_state_json(&engine).unwrap();
        assert_eq!(v["entries"]["user:2"], "bob");
        assert!(v["entries"].get("user:1").is_none(), "chave apagada não aparece");
        // 3 instruções escritas (upsert/upsert/delete), LSNs 0-indexados ⇒ 2.
        assert!(v["max_lsn_applied"].as_u64().unwrap() >= 2);
    }
}
