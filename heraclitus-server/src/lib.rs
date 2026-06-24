//! heraclitus-server — gRPC (tonic) + minimal REST (axum), §3.14.
//! The server composes; the storage knows nothing about HTTP or LLMs.

pub mod engine;
pub mod grpc;
pub mod rest;

pub use engine::Engine;

use heraclitus_core::{HeraclitusConfig, HeraclitusError};
use heraclitus_proto::v1::heraclitus_server::HeraclitusServer;
use std::sync::Arc;
use tonic::{Request, Status};

/// Serve gRPC on `config.grpc_addr` and REST on `config.rest_addr` until
/// the provided shutdown future resolves.
// tonic's interceptor must return `Result<_, Status>` by value; `Status` is a
// large enum, so the `result_large_err` lint fires on the auth closure. Boxing
// is not an option (the trait signature is fixed by tonic), so allow it here.
#[allow(clippy::result_large_err)]
pub async fn serve(
    config: HeraclitusConfig,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), HeraclitusError> {
    let engine = Arc::new(Engine::open(&config)?);

    let grpc_addr: std::net::SocketAddr = config
        .grpc_addr
        .parse()
        .map_err(|e| HeraclitusError::Config(format!("grpc_addr: {e}")))?;
    let rest_addr: std::net::SocketAddr = config
        .rest_addr
        .parse()
        .map_err(|e| HeraclitusError::Config(format!("rest_addr: {e}")))?;

    // Authentication (§access control): when `auth_token` is set, every gRPC
    // call must carry `authorization: Bearer <token>`. When unset, the
    // interceptor is a no-op (default — backward compatible, localhost bind).
    let expected_auth = config.auth_token.clone().map(|t| format!("Bearer {t}"));
    if expected_auth.is_some() {
        tracing::info!("gRPC bearer-token authentication ENABLED");
    }
    let auth = move |req: Request<()>| -> Result<Request<()>, Status> {
        match &expected_auth {
            None => Ok(req),
            Some(exp) => {
                let ok = req
                    .metadata()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v == exp)
                    .unwrap_or(false);
                if ok {
                    Ok(req)
                } else {
                    Err(Status::unauthenticated("missing or invalid bearer token"))
                }
            }
        }
    };
    let svc = HeraclitusServer::with_interceptor(grpc::Service::new(engine.clone()), auth);
    let rest = rest::router(engine.clone());

    let rest_listener = tokio::net::TcpListener::bind(rest_addr).await?;
    let rest_task = tokio::spawn(async move {
        let _ = axum::serve(rest_listener, rest).await;
    });

    // Compliance daemon (RFC 3161 watermark timestamping). Off by default; never
    // on the append path. Receipts under `<data_dir>/receipts`.
    let compliance_task = if config.compliance_enabled {
        use heraclitus_compliance::{run_worker, HttpTsa, LocalTsa, TsaClient, WorkerConfig};
        use std::time::Duration;
        let tsa: std::sync::Arc<dyn TsaClient + Send + Sync> =
            if config.compliance_tsa_mode.eq_ignore_ascii_case("http") {
                std::sync::Arc::new(HttpTsa::new(
                    config.compliance_tsa_url.clone(),
                    config.compliance_tsa_policy.clone(),
                ))
            } else {
                std::sync::Arc::new(LocalTsa::generate(config.compliance_tsa_policy.clone()))
            };
        let wcfg = WorkerConfig::new(
            Duration::from_secs(config.compliance_interval_secs.max(1)),
            config.compliance_min_lsn_step,
            config.data_dir.join("receipts"),
        );
        tracing::info!(
            mode = %config.compliance_tsa_mode,
            "compliance: daemon de carimbo de tempo ATIVO"
        );
        let log = engine.log.clone();
        Some(tokio::spawn(run_worker(
            log,
            tsa,
            wcfg,
            std::future::pending::<()>(),
        )))
    } else {
        None
    };

    tracing::info!(%grpc_addr, %rest_addr, "heraclitus-server up");
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve_with_shutdown(grpc_addr, shutdown)
        .await
        .map_err(|e| HeraclitusError::Config(format!("grpc serve: {e}")))?;
    rest_task.abort();
    if let Some(t) = compliance_task {
        t.abort();
    }
    Ok(())
}
