//! heraclitus-client — Rust SDK over the gRPC surface.

use heraclitus_proto::v1 as pb;
use heraclitus_proto::v1::heraclitus_client::HeraclitusClient as Grpc;
use tonic::transport::Channel;

pub struct Client {
    inner: Grpc<Channel>,
}

#[derive(Debug, Default)]
pub struct AppendOptions {
    pub session_id: String,
    pub kind: String,
    pub hyp: Vec<f32>,
    pub attrs: std::collections::HashMap<String, String>,
    pub parents: Vec<String>,
}

impl Client {
    pub async fn connect(addr: impl Into<String>) -> Result<Self, tonic::transport::Error> {
        // Janelas de varredura podem devolver dezenas de MB (200k nós densos ≈
        // 56MB). O default do tonic é 4MB → sobe-se para 256MB nos dois sentidos.
        const MAX_MSG: usize = 256 * 1024 * 1024;
        let inner = Grpc::connect(addr.into())
            .await?
            .max_decoding_message_size(MAX_MSG)
            .max_encoding_message_size(MAX_MSG);
        Ok(Self { inner })
    }

    pub async fn append(
        &mut self,
        agent_id: &str,
        content: &[u8],
        opts: AppendOptions,
    ) -> Result<u64, tonic::Status> {
        let req = pb::AppendRequest {
            agent_id: agent_id.to_string(),
            session_id: opts.session_id,
            kind: opts.kind,
            content: content.to_vec(),
            hyp: opts.hyp,
            sph: vec![],
            euc: vec![],
            attrs: opts.attrs,
            parents: opts.parents,
        };
        Ok(self.inner.append(req).await?.into_inner().lsn)
    }

    /// Execute a GQL statement (supports EXPLAIN / AS OF / RECALL / NEAREST).
    pub async fn query(&mut self, gql: &str) -> Result<serde_json::Value, tonic::Status> {
        let resp = self
            .inner
            .query(pb::QueryRequest {
                gql: gql.to_string(),
            })
            .await?
            .into_inner();
        serde_json::from_str(&resp.json).map_err(|e| tonic::Status::internal(e.to_string()))
    }

    /// Full two-stage retrieval.
    pub async fn recall(&mut self, text: &str, k: u32) -> Result<serde_json::Value, tonic::Status> {
        let resp = self
            .inner
            .recall(pb::RecallRequest {
                text: text.to_string(),
                k,
            })
            .await?
            .into_inner();
        serde_json::from_str(&resp.json).map_err(|e| tonic::Status::internal(e.to_string()))
    }

    pub async fn snapshot(&mut self) -> Result<u64, tonic::Status> {
        Ok(self
            .inner
            .snapshot(pb::SnapshotRequest {})
            .await?
            .into_inner()
            .lsn)
    }

    pub async fn admin(&mut self, op: &str, arg: &str) -> Result<(bool, String), tonic::Status> {
        let r = self
            .inner
            .admin(pb::AdminRequest {
                op: op.into(),
                arg: arg.into(),
            })
            .await?
            .into_inner();
        Ok((r.ok, r.message))
    }

    /// Subscribe to the tail from `from_lsn`; returns the raw stream.
    pub async fn subscribe(
        &mut self,
        from_lsn: u64,
    ) -> Result<tonic::Streaming<pb::EventMessage>, tonic::Status> {
        Ok(self
            .inner
            .subscribe(pb::SubscribeRequest { from_lsn })
            .await?
            .into_inner())
    }
}
