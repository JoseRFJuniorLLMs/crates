use crate::error::HeraclitusError;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Durability policy for the append path (§3.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum FsyncPolicy {
    /// fsync on every append. Slowest, strongest.
    Always,
    /// Group commit: fsync at most once per `interval_ms`.
    GroupCommit { interval_ms: u64 },
}

impl Default for FsyncPolicy {
    fn default() -> Self {
        FsyncPolicy::GroupCommit { interval_ms: 5 }
    }
}

/// Single config struct for the whole system. Loadable from TOML with
/// `HERACLITUS_*` environment overrides.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HeraclitusConfig {
    pub data_dir: PathBuf,
    /// Segments roll at this size (default 256 MB).
    pub segment_max_bytes: u64,
    pub fsync: FsyncPolicy,
    /// Memtable holds at most this many events above the view watermark.
    pub memtable_cap: usize,
    /// CPU budget for background compaction (distill).
    pub compaction_max_cores: usize,
    /// ACT-R decay parameter `d`.
    pub activation_decay: f64,
    /// gRPC bind address.
    pub grpc_addr: String,
    /// REST (admin) bind address.
    pub rest_addr: String,
    /// Cold tier root (object_store URL or local path).
    pub cold_tier_path: PathBuf,
    /// Optional bearer token required on every gRPC call. `None` = no auth
    /// (default; the server is reachable by anyone who can reach the port).
    pub auth_token: Option<String>,
    /// Encrypt episode `content` at rest with a per-`agent_id` key (§3.10),
    /// enabling crypto-shredding. `false` = plaintext at rest (default).
    /// Keys live under `<data_dir>/keys`.
    pub encryption_at_rest: bool,

    /// Run the compliance watermark-timestamping daemon (RFC 3161 / ICP-Brasil).
    /// `false` = off (default; backward compatible). Receipts go under
    /// `<data_dir>/receipts`.
    pub compliance_enabled: bool,
    /// Daemon tick interval in seconds.
    pub compliance_interval_secs: u64,
    /// Minimum LSN advance between anchors.
    pub compliance_min_lsn_step: u64,
    /// `"local"` (in-process dev ACT) or `"http"` (real RFC 3161 ACT at
    /// `compliance_tsa_url`).
    pub compliance_tsa_mode: String,
    /// ACT endpoint when `compliance_tsa_mode = "http"`.
    pub compliance_tsa_url: String,
    /// Authority/policy name recorded in each receipt.
    pub compliance_tsa_policy: String,
}

impl Default for HeraclitusConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            segment_max_bytes: 256 * 1024 * 1024,
            fsync: FsyncPolicy::default(),
            memtable_cap: 100_000,
            compaction_max_cores: 1,
            activation_decay: 0.5,
            grpc_addr: "127.0.0.1:7474".to_string(),
            rest_addr: "127.0.0.1:7475".to_string(),
            cold_tier_path: PathBuf::from("./data/cold"),
            auth_token: None,
            encryption_at_rest: false,
            compliance_enabled: false,
            compliance_interval_secs: 300,
            compliance_min_lsn_step: 10_000,
            compliance_tsa_mode: "local".to_string(),
            compliance_tsa_url: String::new(),
            compliance_tsa_policy: "ACT-dev".to_string(),
        }
    }
}

impl HeraclitusConfig {
    /// Load from a TOML file, then apply environment overrides.
    pub fn load(path: Option<&std::path::Path>) -> Result<Self, HeraclitusError> {
        let mut cfg = match path {
            Some(p) => {
                let raw = std::fs::read_to_string(p)?;
                toml::from_str(&raw).map_err(|e| HeraclitusError::Config(e.to_string()))?
            }
            None => Self::default(),
        };
        cfg.apply_env();
        Ok(cfg)
    }

    /// `HERACLITUS_DATA_DIR`, `HERACLITUS_GRPC_ADDR`, `HERACLITUS_REST_ADDR`,
    /// `HERACLITUS_FSYNC=always|group_commit:<ms>`.
    pub fn apply_env(&mut self) {
        if let Ok(v) = std::env::var("HERACLITUS_DATA_DIR") {
            self.data_dir = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("HERACLITUS_GRPC_ADDR") {
            self.grpc_addr = v;
        }
        if let Ok(v) = std::env::var("HERACLITUS_REST_ADDR") {
            self.rest_addr = v;
        }
        if let Ok(v) = std::env::var("HERACLITUS_FSYNC") {
            if v == "always" {
                self.fsync = FsyncPolicy::Always;
            } else if let Some(ms) = v.strip_prefix("group_commit:") {
                if let Ok(ms) = ms.parse() {
                    self.fsync = FsyncPolicy::GroupCommit { interval_ms: ms };
                }
            }
        }
        if let Ok(v) = std::env::var("HERACLITUS_AUTH_TOKEN") {
            self.auth_token = if v.is_empty() { None } else { Some(v) };
        }
        if let Ok(v) = std::env::var("HERACLITUS_ENCRYPTION") {
            self.encryption_at_rest = matches!(
                v.to_ascii_lowercase().as_str(),
                "1" | "true" | "on" | "yes"
            );
        }
        if let Ok(v) = std::env::var("HERACLITUS_COMPLIANCE") {
            self.compliance_enabled =
                matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "on" | "yes");
        }
        if let Ok(v) = std::env::var("HERACLITUS_COMPLIANCE_INTERVAL") {
            if let Ok(s) = v.parse() {
                self.compliance_interval_secs = s;
            }
        }
        if let Ok(v) = std::env::var("HERACLITUS_COMPLIANCE_STEP") {
            if let Ok(s) = v.parse() {
                self.compliance_min_lsn_step = s;
            }
        }
        if let Ok(v) = std::env::var("HERACLITUS_COMPLIANCE_TSA_URL") {
            if !v.is_empty() {
                self.compliance_tsa_url = v;
                self.compliance_tsa_mode = "http".to_string();
            }
        }
        if let Ok(v) = std::env::var("HERACLITUS_COMPLIANCE_TSA_POLICY") {
            if !v.is_empty() {
                self.compliance_tsa_policy = v;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_roundtrip_toml() {
        let cfg = HeraclitusConfig::default();
        let s = toml::to_string(&cfg).unwrap();
        let back: HeraclitusConfig = toml::from_str(&s).unwrap();
        assert_eq!(back.segment_max_bytes, cfg.segment_max_bytes);
        assert_eq!(back.fsync, cfg.fsync);
    }
}
