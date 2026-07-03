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

/// Base64 padrão (RFC 4648, com padding) — só para montar o valor esperado do
/// header `Authorization: Basic ...`; evita puxar uma dependência para 15 linhas.
fn b64(input: &[u8]) -> String {
    const AB: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(AB[(n >> 18) as usize & 63] as char);
        out.push(AB[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { AB[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { AB[n as usize & 63] as char } else { '=' });
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
        .with_state(engine);

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
                        .map(|v| v == expected.as_str())
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
    Json(engine.verify().unwrap_or_else(|e| serde_json::json!({ "error": e.to_string() })))
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
