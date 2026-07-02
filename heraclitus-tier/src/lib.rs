//! heraclitus-tier — forgetting with receipts (§3.10).
//!
//! Demotion = remove from hot indexes + upload sealed segments to object
//! storage + append a `DemotionReceipt` log event carrying a blake3 Merkle
//! proof. **Nothing is ever deleted.** Recall-on-demand fetches and
//! re-scans cold segments. GDPR-style erasure is crypto-shredding
//! (docs/CONSISTENCY.md) — planned, not yet implemented here.

use heraclitus_core::{Episode, EventKind, HeraclitusError, Lsn, SegmentId};
use heraclitus_log::format::{Decoded, SegmentHeader, HEADER_LEN};
use heraclitus_log::{merkle_root, Log};
use object_store::{local::LocalFileSystem, path::Path as ObjPath, ObjectStore};
use serde::{Deserialize, Serialize};

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();

/// The receipt persisted to the log (kind = DemotionReceipt, JSON payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DemotionReceipt {
    pub segment_id: SegmentId,
    pub object_path: String,
    pub record_count: u64,
    pub min_lsn: Lsn,
    pub max_lsn: Lsn,
    /// Hex blake3 Merkle root over the segment's per-record leaf hashes
    /// (v2: each leaf covers the record's authenticated region, not just the
    /// payload — see `heraclitus_log::format::record_leaf`).
    pub blake3_root: String,
}

pub struct ColdTier {
    store: LocalFileSystem,
}

impl ColdTier {
    /// v0 backend: local filesystem via the `object_store` API — the same
    /// trait surface as S3/GCS, so swapping backends is configuration.
    pub fn open_local(root: impl AsRef<std::path::Path>) -> Result<Self, HeraclitusError> {
        std::fs::create_dir_all(root.as_ref())?;
        let store = LocalFileSystem::new_with_prefix(root.as_ref())
            .map_err(|e| HeraclitusError::Storage(std::io::Error::other(e)))?;
        Ok(Self { store })
    }

    /// Demote one sealed segment: upload bytes + append the receipt event.
    /// Returns the receipt and the LSN of the receipt event.
    pub async fn demote(
        &self,
        log: &Log,
        segment_id: SegmentId,
    ) -> Result<(DemotionReceipt, Lsn), HeraclitusError> {
        let meta = log
            .sealed_segments()
            .into_iter()
            .find(|s| s.id == segment_id)
            .ok_or_else(|| HeraclitusError::Query(format!("segment {segment_id} not sealed")))?;

        let bytes = std::fs::read(&meta.path)?;
        let (count, root) = scan_and_root(&bytes)?;

        let obj_path = ObjPath::from(format!("cold/{segment_id:020}.hrkl"));
        self.store
            .put(&obj_path, bytes.into())
            .await
            .map_err(|e| HeraclitusError::Storage(std::io::Error::other(e)))?;

        let receipt = DemotionReceipt {
            segment_id,
            object_path: obj_path.to_string(),
            record_count: count,
            min_lsn: meta.base_lsn,
            max_lsn: meta.max_lsn,
            blake3_root: hex(&root),
        };
        let payload = serde_json::to_vec(&receipt)
            .map_err(|e| HeraclitusError::Serialization(e.to_string()))?;
        let lsn = log.append(Episode::new("tier", EventKind::DemotionReceipt, payload))?;
        Ok((receipt, lsn))
    }

    /// Verify a receipt: fetch the cold object, recompute the Merkle root
    /// over its per-record leaf hashes, compare. M5 acceptance gate.
    pub async fn verify_receipt(&self, receipt: &DemotionReceipt) -> Result<bool, HeraclitusError> {
        let obj = self
            .store
            .get(&ObjPath::from(receipt.object_path.clone()))
            .await
            .map_err(|e| HeraclitusError::Storage(std::io::Error::other(e)))?;
        let bytes = obj
            .bytes()
            .await
            .map_err(|e| HeraclitusError::Storage(std::io::Error::other(e)))?;
        let (count, root) = scan_and_root(&bytes)?;
        Ok(count == receipt.record_count && hex(&root) == receipt.blake3_root)
    }

    /// Recall-on-demand (`INCLUDE COLD`): fetch and decode a cold segment's
    /// episodes for temporary re-indexing.
    pub async fn fetch_cold(
        &self,
        receipt: &DemotionReceipt,
    ) -> Result<Vec<(Lsn, Episode)>, HeraclitusError> {
        let obj = self
            .store
            .get(&ObjPath::from(receipt.object_path.clone()))
            .await
            .map_err(|e| HeraclitusError::Storage(std::io::Error::other(e)))?;
        let bytes = obj
            .bytes()
            .await
            .map_err(|e| HeraclitusError::Storage(std::io::Error::other(e)))?;
        let mut out = Vec::new();
        visit_records(&bytes, &mut |lsn, payload, _record| {
            let (ep, _): (Episode, usize) = bincode::serde::decode_from_slice(payload, BINCODE_CFG)
                .map_err(|e| HeraclitusError::Serialization(e.to_string()))?;
            out.push((lsn, ep));
            Ok(())
        })?;
        Ok(out)
    }
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// Callback receives `(lsn, payload, full_record_bytes)`. The full record is
// needed to recompute the version-correct Merkle leaf (which, on v2, covers the
// header, not just the payload).
type RecordVisitor<'a> = &'a mut dyn FnMut(Lsn, &[u8], &[u8]) -> Result<(), HeraclitusError>;

fn visit_records(bytes: &[u8], f: RecordVisitor<'_>) -> Result<(), HeraclitusError> {
    let version = SegmentHeader::decode(bytes)?.version;
    let mut offset = HEADER_LEN;
    while offset < bytes.len() {
        match heraclitus_log::format::decode_record(version, &bytes[offset..]) {
            Decoded::Record(lsn, _h, payload, consumed) => {
                f(lsn, payload, &bytes[offset..offset + consumed])?;
                offset += consumed;
            }
            Decoded::Footer(_) | Decoded::Torn => break,
        }
    }
    Ok(())
}

fn scan_and_root(bytes: &[u8]) -> Result<(u64, [u8; 32]), HeraclitusError> {
    let version = SegmentHeader::decode(bytes)?.version;
    let mut hashes = Vec::new();
    visit_records(bytes, &mut |_l, _payload, record| {
        // Must match the writer/recovery leaf for the segment's version, or the
        // recomputed root will not equal the receipt's.
        hashes.push(heraclitus_log::format::record_leaf(version, record));
        Ok(())
    })?;
    Ok((hashes.len() as u64, merkle_root(&hashes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use heraclitus_core::FsyncPolicy;

    fn seeded_log(dir: &std::path::Path) -> Log {
        // Tiny segments force at least one sealed segment.
        let log = Log::open(dir, 2048, FsyncPolicy::Always).unwrap();
        for i in 0..120 {
            log.append(Episode::new(
                "tier",
                EventKind::Observation,
                format!("cold candidate {i}").into_bytes(),
            ))
            .unwrap();
        }
        assert!(!log.sealed_segments().is_empty());
        log
    }

    #[tokio::test]
    async fn demotion_receipt_verifies_and_recalls() {
        // M5 acceptance gate: demotion receipt verification + recall-on-demand.
        let dir = tempfile::tempdir().unwrap();
        let log = seeded_log(&dir.path().join("log"));
        let tier = ColdTier::open_local(dir.path().join("cold")).unwrap();

        let seg = log.sealed_segments()[0].clone();
        let (receipt, receipt_lsn) = tier.demote(&log, seg.id).await.unwrap();

        // Receipt is an ordinary log event.
        let (_, ev) = log.read(receipt_lsn).unwrap().unwrap();
        assert_eq!(ev.kind, EventKind::DemotionReceipt);
        let back: DemotionReceipt = serde_json::from_slice(&ev.content).unwrap();
        assert_eq!(back.blake3_root, receipt.blake3_root);

        // Cryptographic verification passes.
        assert!(tier.verify_receipt(&receipt).await.unwrap());

        // Recall-on-demand returns every record of the demoted segment.
        let cold = tier.fetch_cold(&receipt).await.unwrap();
        assert_eq!(cold.len() as u64, receipt.record_count);
        assert_eq!(cold.first().unwrap().0, receipt.min_lsn);
        assert_eq!(cold.last().unwrap().0, receipt.max_lsn);
    }

    #[tokio::test]
    async fn tampered_cold_object_fails_verification() {
        let dir = tempfile::tempdir().unwrap();
        let log = seeded_log(&dir.path().join("log"));
        let cold_root = dir.path().join("cold");
        let tier = ColdTier::open_local(&cold_root).unwrap();
        let seg = log.sealed_segments()[0].clone();
        let (receipt, _) = tier.demote(&log, seg.id).await.unwrap();

        // Flip one payload byte deep inside the cold object.
        let obj = cold_root.join(
            receipt
                .object_path
                .replace('/', std::path::MAIN_SEPARATOR_STR),
        );
        let mut bytes = std::fs::read(&obj).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        std::fs::write(&obj, bytes).unwrap();

        assert!(
            !tier.verify_receipt(&receipt).await.unwrap(),
            "tampering must be detected by the Merkle proof"
        );
    }
}
