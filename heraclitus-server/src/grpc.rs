//! The gRPC service over the engine.

use crate::engine::Engine;
use heraclitus_core::{Episode, EventKind, ProductPoint};
use heraclitus_proto::v1 as pb;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub struct Service {
    engine: Arc<Engine>,
}

impl Service {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

fn episode_json(lsn: u64, e: &Episode) -> String {
    serde_json::json!({
        "lsn": lsn,
        "id": e.id.to_string(),
        "agent_id": e.agent_id,
        "kind": format!("{:?}", e.kind),
        "content": String::from_utf8_lossy(&e.content),
        "attrs": e.attrs,
        "ts_hlc": e.ts_hlc,
    })
    .to_string()
}

#[tonic::async_trait]
impl pb::heraclitus_server::Heraclitus for Service {
    async fn append(
        &self,
        req: Request<pb::AppendRequest>,
    ) -> Result<Response<pb::AppendResponse>, Status> {
        let r = req.into_inner();
        let kind = match r.kind.as_str() {
            "" | "Observation" => EventKind::Observation,
            "Action" => EventKind::Action,
            "Message" => EventKind::Message,
            "RetrievalFeedback" => EventKind::RetrievalFeedback,
            other => EventKind::Custom(other.to_string()),
        };
        let mut e = Episode::new(r.agent_id, kind, r.content);
        e.session_id = r.session_id;
        if !(r.hyp.is_empty() && r.sph.is_empty() && r.euc.is_empty()) {
            let mut hyp = r.hyp;
            heraclitus_manifold::project_to_ball(&mut hyp);
            e.embedding = Some(ProductPoint {
                hyp,
                sph: r.sph,
                euc: r.euc,
            });
        }
        e.attrs = r.attrs.into_iter().collect();
        for p in r.parents {
            e.parents.push(
                p.parse()
                    .map_err(|_| Status::invalid_argument("bad parent ULID"))?,
            );
        }
        let lsn = self.engine.append(e).map_err(internal)?;
        Ok(Response::new(pb::AppendResponse { lsn }))
    }

    async fn query(
        &self,
        req: Request<pb::QueryRequest>,
    ) -> Result<Response<pb::QueryResponse>, Status> {
        let gql = req.into_inner().gql;
        let v = heraclitus_query::execute(&gql, self.engine.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        Ok(Response::new(pb::QueryResponse {
            json: v.to_string(),
        }))
    }

    async fn recall(
        &self,
        req: Request<pb::RecallRequest>,
    ) -> Result<Response<pb::QueryResponse>, Status> {
        let r = req.into_inner();
        let v = self
            .engine
            .recall(&r.text, r.k.max(1) as usize)
            .map_err(internal)?;
        Ok(Response::new(pb::QueryResponse {
            json: v.to_string(),
        }))
    }

    type SubscribeStream = ReceiverStream<Result<pb::EventMessage, Status>>;

    async fn subscribe(
        &self,
        req: Request<pb::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let from = req.into_inner().from_lsn;
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let engine = self.engine.clone();
        let mut live = engine.log.tail_subscribe();
        tokio::spawn(async move {
            // History first, then bridge the live tail. Audit #6: when the
            // broadcast lags (slow consumer during a burst), we fall back to
            // re-reading history by LSN — gap-free, never silent drops.
            let mut next = from;
            'catchup: loop {
                while let Ok(batch) = engine.log.scan(next, next + 256) {
                    if batch.is_empty() {
                        break;
                    }
                    for (lsn, e) in &batch {
                        next = lsn + 1;
                        let msg = pb::EventMessage {
                            lsn: *lsn,
                            episode_json: episode_json(*lsn, e),
                        };
                        if tx.send(Ok(msg)).await.is_err() {
                            return;
                        }
                    }
                }
                loop {
                    match live.recv().await {
                        Ok((lsn, e)) => {
                            if lsn < next {
                                continue;
                            }
                            if lsn > next {
                                // missed events: re-read from the log
                                continue 'catchup;
                            }
                            next = lsn + 1;
                            let msg = pb::EventMessage {
                                lsn,
                                episode_json: episode_json(lsn, &e),
                            };
                            if tx.send(Ok(msg)).await.is_err() {
                                return;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            continue 'catchup;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn snapshot(
        &self,
        _req: Request<pb::SnapshotRequest>,
    ) -> Result<Response<pb::SnapshotResponse>, Status> {
        Ok(Response::new(pb::SnapshotResponse {
            lsn: self.engine.snapshot(),
        }))
    }

    async fn admin(
        &self,
        req: Request<pb::AdminRequest>,
    ) -> Result<Response<pb::AdminResponse>, Status> {
        let r = req.into_inner();
        let (ok, message) = match r.op.as_str() {
            "stats" => (true, self.engine.stats().to_string()),
            "verify" => match self.engine.verify() {
                Ok(v) => (true, v.to_string()),
                Err(e) => (false, e.to_string()),
            },
            "rebuild" => {
                let view = if r.arg.is_empty() {
                    None
                } else {
                    Some(r.arg.as_str())
                };
                match self.engine.rebuild(view) {
                    Ok(()) => (true, "rebuilt".to_string()),
                    Err(e) => (false, e.to_string()),
                }
            }
            op if op.starts_with("shred:") => {
                let agent = op.strip_prefix("shred:").unwrap_or("");
                match self.engine.shred(agent) {
                    Ok(true) => (
                        true,
                        format!("crypto-shred: key destroyed for agent '{agent}'"),
                    ),
                    Ok(false) => (true, format!("crypto-shred: no key for agent '{agent}'")),
                    Err(e) => (false, e.to_string()),
                }
            }
            other => (false, format!("unknown admin op: {other}")),
        };
        Ok(Response::new(pb::AdminResponse { ok, message }))
    }
}
