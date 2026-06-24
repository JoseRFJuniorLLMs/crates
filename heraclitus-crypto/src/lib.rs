//! heraclitus-crypto — encryption at rest with per-agent keys + crypto-shredding (§3.10).
//!
//! Each `agent_id` owns a 32-byte key, persisted as a file **outside the
//! immutable log**. Episode content is sealed at rest with ChaCha20-Poly1305
//! (AEAD), the `agent_id` bound in as associated data. "Erasure" (LGPD/GDPR)
//! is **crypto-shredding**: destroy the key file and that agent's ciphertext
//! becomes permanently unreadable — the append-only log is never mutated.
//!
//! Backward compatibility: sealed blobs carry an 8-byte magic prefix. Legacy
//! plaintext content never starts with it, so a mixed log (old plaintext +
//! new ciphertext) reads correctly.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use dashmap::DashMap;
use rand::RngCore;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Magic prefix marking a sealed (encrypted) content blob.
pub const ENC_MAGIC: &[u8; 8] = b"HRKLENC1";
const NONCE_LEN: usize = 12;

/// Tombstone substituted for content whose key was crypto-shredded.
pub const SHREDDED: &[u8] = b"[shredded]";

/// True if `blob` looks like a sealed content blob.
pub fn is_encrypted(blob: &[u8]) -> bool {
    blob.len() >= ENC_MAGIC.len() + NONCE_LEN && blob[..ENC_MAGIC.len()] == ENC_MAGIC[..]
}

/// Seal `plaintext`: `MAGIC || nonce(12) || ciphertext+tag`. `aad` (the
/// agent_id) is authenticated but not encrypted.
pub fn seal(key: &[u8; 32], plaintext: &[u8], aad: &[u8]) -> Vec<u8> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    let mut nonce = [0u8; NONCE_LEN];
    rand::thread_rng().fill_bytes(&mut nonce);
    let ct = cipher
        .encrypt(Nonce::from_slice(&nonce), Payload { msg: plaintext, aad })
        .expect("chacha20poly1305 encrypt never fails for valid key/nonce");
    let mut out = Vec::with_capacity(ENC_MAGIC.len() + NONCE_LEN + ct.len());
    out.extend_from_slice(&ENC_MAGIC[..]);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    out
}

/// Open a sealed blob. Returns `None` if the blob is not sealed, the key is
/// wrong, or the tag fails (tamper / corruption).
pub fn open(key: &[u8; 32], blob: &[u8], aad: &[u8]) -> Option<Vec<u8>> {
    if !is_encrypted(blob) {
        return None;
    }
    let nonce = &blob[ENC_MAGIC.len()..ENC_MAGIC.len() + NONCE_LEN];
    let ct = &blob[ENC_MAGIC.len() + NONCE_LEN..];
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    cipher
        .decrypt(Nonce::from_slice(nonce), Payload { msg: ct, aad })
        .ok()
}

/// Per-agent key store. One key file per agent so a single agent can be
/// crypto-shredded by destroying exactly one file.
pub struct KeyStore {
    dir: PathBuf,
    cache: DashMap<String, [u8; 32]>,
}

impl KeyStore {
    /// Open (or create) the key directory.
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Arc<Self>> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Arc::new(Self {
            dir,
            cache: DashMap::new(),
        }))
    }

    fn key_path(&self, agent_id: &str) -> PathBuf {
        // hex-encode the agent_id so the filename is always filesystem-safe.
        let hex: String = agent_id.bytes().map(|b| format!("{b:02x}")).collect();
        self.dir.join(format!("{hex}.key"))
    }

    fn read_key(path: &Path) -> Option<[u8; 32]> {
        let bytes = std::fs::read(path).ok()?;
        if bytes.len() != 32 {
            return None;
        }
        let mut k = [0u8; 32];
        k.copy_from_slice(&bytes);
        Some(k)
    }

    /// Fetch the agent's key, generating and persisting one on first use.
    pub fn get_or_create(&self, agent_id: &str) -> io::Result<[u8; 32]> {
        if let Some(k) = self.cache.get(agent_id) {
            return Ok(*k);
        }
        let path = self.key_path(agent_id);
        let key = match Self::read_key(&path) {
            Some(k) => k,
            None => {
                let mut k = [0u8; 32];
                rand::thread_rng().fill_bytes(&mut k);
                let tmp = path.with_extension("tmp");
                std::fs::write(&tmp, k)?;
                std::fs::rename(&tmp, &path)?;
                k
            }
        };
        self.cache.insert(agent_id.to_string(), key);
        Ok(key)
    }

    /// Fetch the agent's key if it still exists (`None` if never created or
    /// already shredded).
    pub fn get(&self, agent_id: &str) -> Option<[u8; 32]> {
        if let Some(k) = self.cache.get(agent_id) {
            return Some(*k);
        }
        let k = Self::read_key(&self.key_path(agent_id))?;
        self.cache.insert(agent_id.to_string(), k);
        Some(k)
    }

    /// Crypto-shred: best-effort overwrite then delete the agent's key (file +
    /// cache). Returns whether a key was present. Idempotent; never touches the
    /// log.
    pub fn shred(&self, agent_id: &str) -> io::Result<bool> {
        self.cache.remove(agent_id);
        let path = self.key_path(agent_id);
        if !path.exists() {
            return Ok(false);
        }
        // Best-effort overwrite so the raw key bytes do not linger on disk.
        if let Ok(meta) = std::fs::metadata(&path) {
            let _ = std::fs::write(&path, vec![0u8; meta.len() as usize]);
        }
        std::fs::remove_file(&path)?;
        Ok(true)
    }

    /// Number of agents with a live key on disk.
    pub fn agent_count(&self) -> usize {
        std::fs::read_dir(&self.dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| e.path().extension().is_some_and(|x| x == "key"))
                    .count()
            })
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_open_roundtrip() {
        let key = [7u8; 32];
        let blob = seal(&key, b"segredo do agente", b"eva");
        assert!(is_encrypted(&blob));
        assert_eq!(open(&key, &blob, b"eva").unwrap(), b"segredo do agente");
        // wrong aad (agent) fails
        assert!(open(&key, &blob, b"outro").is_none());
        // wrong key fails
        assert!(open(&[9u8; 32], &blob, b"eva").is_none());
    }

    #[test]
    fn plaintext_is_not_encrypted() {
        assert!(!is_encrypted(b"empresa X trocou de socio"));
        assert!(!is_encrypted(b""));
    }

    #[test]
    fn keystore_create_get_shred() {
        let dir = tempfile::tempdir().unwrap();
        let ks = KeyStore::open(dir.path()).unwrap();
        let k1 = ks.get_or_create("eva").unwrap();
        // stable across calls
        assert_eq!(k1, ks.get_or_create("eva").unwrap());
        assert_eq!(Some(k1), ks.get("eva"));
        assert_eq!(ks.agent_count(), 1);

        // seal with the agent key, then shred -> key gone -> cannot open
        let blob = seal(&k1, b"dados pessoais", b"eva");
        assert!(ks.shred("eva").unwrap());
        assert!(ks.get("eva").is_none());
        assert!(!ks.shred("eva").unwrap()); // idempotent
        // a fresh key for the same agent cannot decrypt the old blob
        let k2 = ks.get_or_create("eva").unwrap();
        assert!(open(&k2, &blob, b"eva").is_none());
    }
}
