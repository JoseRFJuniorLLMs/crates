//! heraclitus-log — the only writer of truth.
//!
//! A segmented, append-only, immutable log of [`Episode`]s. Everything else
//! in HeraclitusDB is a materialized view over this log.
//!
//! Durability: crc32 per record, blake3 Merkle root per sealed segment,
//! torn-write recovery on open (truncate at first crc mismatch).

pub mod format;

use format::{Decoded, SegmentFooter, SegmentHeader, FOOTER_LEN, HEADER_LEN};
use heraclitus_core::{Episode, FsyncPolicy, HeraclitusError, Hlc, Lsn, SegmentId};
use heraclitus_crypto::KeyStore;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use tokio::sync::broadcast;

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();

fn segment_path(dir: &Path, id: SegmentId) -> PathBuf {
    dir.join(format!("{id:020}.hrkl"))
}

#[derive(Debug, Clone)]
pub struct SegmentMeta {
    pub id: SegmentId,
    pub path: PathBuf,
    pub min_lsn: Lsn,
    pub max_lsn: Lsn,
    pub sealed: bool,
    pub blake3_root: Option<[u8; 32]>,
}

struct Active {
    file: File,
    segment_id: SegmentId,
    bytes_written: u64,
    record_hashes: Vec<[u8; 32]>,
    min_lsn: Lsn,
    max_lsn: Lsn,
    last_sync: Instant,
}

/// The append-only log. `Log` is `Sync`: appends are serialized by an
/// internal mutex (single-writer-per-process, §3.11).
pub struct Log {
    dir: PathBuf,
    segment_max_bytes: u64,
    fsync: FsyncPolicy,
    hlc: Hlc,
    inner: Mutex<Inner>,
    tail_tx: broadcast::Sender<(Lsn, Episode)>,
    /// When set, episode `content` is sealed at rest with a per-`agent_id` key
    /// (§3.10). `None` = plaintext at rest (default; backward compatible).
    keystore: Option<Arc<KeyStore>>,
}

struct Inner {
    sealed: Vec<SegmentMeta>,
    active: Active,
    next_lsn: Lsn,
    /// Índice de offset por-LSN: `lsn -> (segmento, byte-offset)`. Construído na
    /// recuperação do `open` (de graça — já varre todos os registos) e mantido no
    /// `append`. Torna `read(lsn)` um seek O(1) em vez de varrer um segmento.
    loc: std::collections::HashMap<Lsn, (SegmentId, u64)>,
}

impl Log {
    /// Open (or create) a log in `dir`, running torn-write recovery.
    /// Content is stored in plaintext at rest (backward compatible).
    pub fn open(
        dir: impl Into<PathBuf>,
        segment_max_bytes: u64,
        fsync: FsyncPolicy,
    ) -> Result<Self, HeraclitusError> {
        Self::open_with_keystore(dir, segment_max_bytes, fsync, None)
    }

    /// Like [`Log::open`], but when `keystore` is `Some`, episode `content` is
    /// sealed at rest with a per-`agent_id` key (§3.10). Everything above the
    /// log (memtable, views, queries, the live tail) still sees plaintext.
    pub fn open_with_keystore(
        dir: impl Into<PathBuf>,
        segment_max_bytes: u64,
        fsync: FsyncPolicy,
        keystore: Option<Arc<KeyStore>>,
    ) -> Result<Self, HeraclitusError> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let hlc = Hlc::new();

        let mut ids: Vec<SegmentId> = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                name.strip_suffix(".hrkl")?.parse::<u64>().ok()
            })
            .collect();
        ids.sort_unstable();

        let mut sealed = Vec::new();
        let mut next_lsn: Lsn = 0;
        let mut tail: Option<(SegmentId, RecoveredTail)> = None;
        let mut loc: std::collections::HashMap<Lsn, (SegmentId, u64)> =
            std::collections::HashMap::new();

        for (i, id) in ids.iter().enumerate() {
            let path = segment_path(&dir, *id);
            let is_last = i == ids.len() - 1;
            let rec = recover_segment(&path, *id)?;
            // Índice de offset: registos deste segmento (de graça — já varremos).
            for (l, off) in &rec.locs {
                loc.insert(*l, (*id, *off));
            }
            next_lsn = next_lsn.max(rec.max_lsn.map(|l| l + 1).unwrap_or(next_lsn));
            if rec.sealed {
                sealed.push(SegmentMeta {
                    id: *id,
                    path,
                    min_lsn: rec.min_lsn.unwrap_or(0),
                    max_lsn: rec.max_lsn.unwrap_or(0),
                    sealed: true,
                    blake3_root: rec.blake3_root,
                });
            } else if is_last && rec.version == format::FORMAT_VERSION {
                tail = Some((*id, rec));
            } else {
                // Seal this unsealed segment now and continue in a fresh
                // current-version segment. Two cases reach here:
                //  - not the last segment: process died between roll and seal;
                //  - an older-version (v1) tail: re-sealing it means every
                //    *new* record gets the v2 header-covering CRC/Merkle rather
                //    than appending more unprotected v1 records to it.
                seal_file(&path, &rec)?;
                sealed.push(SegmentMeta {
                    id: *id,
                    path,
                    min_lsn: rec.min_lsn.unwrap_or(0),
                    max_lsn: rec.max_lsn.unwrap_or(0),
                    sealed: true,
                    // Audit02 #2: the footer we just wrote carries this root;
                    // keep the in-memory meta consistent (was erroneously None).
                    blake3_root: Some(merkle_root(&rec.record_hashes)),
                });
            }
        }

        let active = match tail {
            Some((id, rec)) => {
                let file = OpenOptions::new()
                    .append(true)
                    .open(segment_path(&dir, id))?;
                Active {
                    file,
                    segment_id: id,
                    bytes_written: rec.valid_len,
                    record_hashes: rec.record_hashes,
                    min_lsn: rec.min_lsn.unwrap_or(u64::MAX),
                    max_lsn: rec.max_lsn.unwrap_or(0),
                    last_sync: Instant::now(),
                }
            }
            None => {
                let id = sealed.last().map(|s| s.id + 1).unwrap_or(0);
                new_active(&dir, id, &hlc)?
            }
        };

        let (tail_tx, _) = broadcast::channel(4096);
        Ok(Self {
            dir,
            segment_max_bytes,
            fsync,
            hlc,
            inner: Mutex::new(Inner {
                sealed,
                active,
                next_lsn,
                loc,
            }),
            tail_tx,
            keystore,
        })
    }

    /// Append one episode. Returns its LSN. The episode's `ts_hlc` is stamped
    /// by the log's hybrid logical clock.
    pub fn append(&self, mut episode: Episode) -> Result<Lsn, HeraclitusError> {
        let mut inner = self.inner.lock().unwrap();
        self.append_locked(&mut inner, &mut episode)
    }

    /// Optimistic compare-and-append: succeeds only if the current head
    /// (next LSN) equals `expected`.
    pub fn append_cas(&self, expected: Lsn, mut episode: Episode) -> Result<Lsn, HeraclitusError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.next_lsn != expected {
            return Err(HeraclitusError::CasConflict {
                expected,
                head: inner.next_lsn,
            });
        }
        self.append_locked(&mut inner, &mut episode)
    }

    /// Replica path: append an episode shipped from a leader, preserving its
    /// HLC stamp, at exactly `lsn` (must equal the local head — contiguity).
    pub fn append_replicated(&self, lsn: Lsn, episode: Episode) -> Result<Lsn, HeraclitusError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.next_lsn != lsn {
            return Err(HeraclitusError::CasConflict {
                expected: lsn,
                head: inner.next_lsn,
            });
        }
        self.hlc.observe(episode.ts_hlc);
        let mut episode = episode;
        self.append_raw(&mut inner, &mut episode, false)
    }

    fn append_locked(
        &self,
        inner: &mut Inner,
        episode: &mut Episode,
    ) -> Result<Lsn, HeraclitusError> {
        self.append_raw(inner, episode, true)
    }

    fn append_raw(
        &self,
        inner: &mut Inner,
        episode: &mut Episode,
        stamp: bool,
    ) -> Result<Lsn, HeraclitusError> {
        if stamp {
            episode.ts_hlc = self.hlc.now();
        }
        let lsn = inner.next_lsn;
        // Encryption at rest (§3.10): seal `content` with the agent's key for
        // the on-disk payload only, then restore plaintext so the broadcast
        // tail and the returned episode (which feed memtable + views) stay
        // readable. The Merkle hash below is over the sealed bytes — correct.
        let restore = match &self.keystore {
            Some(ks) => {
                let key = ks.get_or_create(&episode.agent_id)?;
                let plain = std::mem::take(&mut episode.content);
                episode.content =
                    heraclitus_crypto::seal(&key, &plain, episode.agent_id.as_bytes());
                Some(plain)
            }
            None => None,
        };
        let payload = bincode::serde::encode_to_vec(&*episode, BINCODE_CFG)
            .map_err(|e| HeraclitusError::Serialization(e.to_string()))?;
        if let Some(plain) = restore {
            episode.content = plain;
        }
        // New records always use the current format version (v2): an
        // older-version unsealed tail is sealed on open, so the active segment
        // is never below the current version (see `open_with_keystore`).
        let record = format::encode_record(format::FORMAT_VERSION, lsn, episode.ts_hlc, &payload);

        if inner.active.bytes_written + record.len() as u64 > self.segment_max_bytes {
            self.roll(inner)?;
        }

        // Localização deste registo (após eventual roll) para o índice de offset.
        let loc_seg = inner.active.segment_id;
        let loc_off = inner.active.bytes_written;

        let active = &mut inner.active;
        // Audit #2: a failed/partial write must not leave garbage that the
        // next append (reusing this LSN) would write after. Roll the file
        // back to the last known-good offset before surfacing the error.
        if let Err(e) = active.file.write_all(&record) {
            let _ = active.file.set_len(active.bytes_written);
            return Err(e.into());
        }
        active.bytes_written += record.len() as u64;
        // Merkle leaf over the full authenticated region (v2), so the sealed
        // root commits to lsn/hlc/len, not just the payload.
        active
            .record_hashes
            .push(format::record_leaf(format::FORMAT_VERSION, &record));
        active.min_lsn = active.min_lsn.min(lsn);
        active.max_lsn = active.max_lsn.max(lsn);

        match &self.fsync {
            FsyncPolicy::Always => {
                active.file.sync_data()?;
                active.last_sync = Instant::now();
            }
            FsyncPolicy::GroupCommit { interval_ms } => {
                if active.last_sync.elapsed().as_millis() as u64 >= *interval_ms {
                    active.file.sync_data()?;
                    active.last_sync = Instant::now();
                }
            }
        }

        inner.loc.insert(lsn, (loc_seg, loc_off));
        inner.next_lsn = lsn + 1;
        let _ = self.tail_tx.send((lsn, episode.clone()));
        Ok(lsn)
    }

    /// Force an fsync of the active segment.
    pub fn flush(&self) -> Result<(), HeraclitusError> {
        let mut inner = self.inner.lock().unwrap();
        inner.active.file.sync_data()?;
        inner.active.last_sync = Instant::now();
        Ok(())
    }

    fn roll(&self, inner: &mut Inner) -> Result<(), HeraclitusError> {
        let old = &mut inner.active;
        old.file.sync_data()?;
        let footer = SegmentFooter {
            record_count: old.record_hashes.len() as u64,
            min_lsn: if old.min_lsn == u64::MAX {
                0
            } else {
                old.min_lsn
            },
            max_lsn: old.max_lsn,
            blake3_root: merkle_root(&old.record_hashes),
        };
        old.file.write_all(&footer.encode())?;
        old.file.sync_data()?;
        inner.sealed.push(SegmentMeta {
            id: old.segment_id,
            path: segment_path(&self.dir, old.segment_id),
            min_lsn: footer.min_lsn,
            max_lsn: footer.max_lsn,
            sealed: true,
            blake3_root: Some(footer.blake3_root),
        });
        let next_id = old.segment_id + 1;
        inner.active = new_active(&self.dir, next_id, &self.hlc)?;
        Ok(())
    }

    /// Next LSN to be assigned (the head).
    pub fn head(&self) -> Lsn {
        self.inner.lock().unwrap().next_lsn
    }

    /// Subscribe to the live tail. Feeds the memtable and the view engine.
    pub fn tail_subscribe(&self) -> broadcast::Receiver<(Lsn, Episode)> {
        self.tail_tx.subscribe()
    }

    /// Read a single episode by LSN. O(1) via o índice de offset por-LSN (seek
    /// directo); só recorre a um scan se o LSN não estiver no índice.
    pub fn read(&self, lsn: Lsn) -> Result<Option<(Lsn, Episode)>, HeraclitusError> {
        let at = self.inner.lock().unwrap().loc.get(&lsn).copied();
        if let Some((seg, off)) = at {
            if let Some(hit) = self.read_at(seg, off)? {
                if hit.0 == lsn {
                    return Ok(Some(hit));
                }
            }
        }
        Ok(self.scan(lsn, lsn + 1)?.into_iter().next())
    }

    /// Lê UM registo na posição exata `(segmento, byte-offset)` com seeks — não
    /// varre o segmento. Usado pelo `read` O(1) e por consultas por índice.
    pub fn read_at(
        &self,
        seg: SegmentId,
        off: u64,
    ) -> Result<Option<(Lsn, Episode)>, HeraclitusError> {
        let path = segment_path(&self.dir, seg);
        let mut f = match File::open(&path) {
            Ok(f) => f,
            Err(_) => return Ok(None),
        };
        // versão do segmento (cabeçalho)
        let mut hdr = [0u8; HEADER_LEN];
        if f.read_exact(&mut hdr).is_err() {
            return Ok(None);
        }
        let version = SegmentHeader::decode(&hdr)?.version;
        // cabeçalho do registo na posição -> comprimento do payload
        f.seek(SeekFrom::Start(off))?;
        let mut rh = [0u8; format::RECORD_HEADER_LEN];
        if f.read_exact(&mut rh).is_err() {
            return Ok(None);
        }
        let len = u32::from_le_bytes(rh[..4].try_into().unwrap()) as usize;
        if len > 512 * 1024 * 1024 {
            return Ok(None);
        }
        let mut buf = vec![0u8; format::RECORD_HEADER_LEN + len];
        buf[..format::RECORD_HEADER_LEN].copy_from_slice(&rh);
        if f.read_exact(&mut buf[format::RECORD_HEADER_LEN..]).is_err() {
            return Ok(None);
        }
        match format::decode_record(version, &buf) {
            Decoded::Record(rlsn, _hlc, payload, _) => {
                let (mut ep, _): (Episode, usize) =
                    bincode::serde::decode_from_slice(payload, BINCODE_CFG)
                        .map_err(|e| HeraclitusError::Serialization(e.to_string()))?;
                self.decrypt_in_place(&mut ep);
                Ok(Some((rlsn, ep)))
            }
            _ => Ok(None),
        }
    }

    /// Scan `[from, to)` across all segments, in LSN order.
    pub fn scan(&self, from: Lsn, to: Lsn) -> Result<Vec<(Lsn, Episode)>, HeraclitusError> {
        self.scan_capped(from, to, usize::MAX)
    }

    /// Scan `[from, to)` returning at most `max` episodes (the query guard).
    ///
    /// Two scalability levers, both keyed off the LSN window:
    /// 1. **Segment pruning** — only segments whose `[min_lsn, max_lsn]` range
    ///    overlaps `[from, to)` are read (the rest are skipped wholesale). A
    ///    narrow time window therefore touches one or two segments, not the
    ///    whole log.
    /// 2. **Row cap** — accumulation stops at `max`, so a query over a huge log
    ///    cannot materialize millions of episodes and exhaust memory (the crash
    ///    guard). `scan` passes `usize::MAX` (uncapped) for internal/replay use.
    pub fn scan_capped(
        &self,
        from: Lsn,
        to: Lsn,
        max: usize,
    ) -> Result<Vec<(Lsn, Episode)>, HeraclitusError> {
        // Flush so the read path (which goes through the filesystem) sees
        // everything appended so far.
        self.flush()?;
        let paths: Vec<(PathBuf, bool)> = {
            let inner = self.inner.lock().unwrap();
            // Segment pruning: a segment is relevant iff its [min,max] overlaps
            // the requested window [from, to). `max_lsn >= from && min_lsn < to`.
            let mut p: Vec<(PathBuf, bool)> = inner
                .sealed
                .iter()
                .filter(|s| s.max_lsn >= from && s.min_lsn < to)
                .map(|s| (s.path.clone(), true))
                .collect();
            // The active (tail) segment: include only if non-empty and its live
            // range overlaps the window.
            let amin = inner.active.min_lsn;
            let amax = inner.active.max_lsn;
            if amin != u64::MAX && amax >= from && amin < to {
                p.push((segment_path(&self.dir, inner.active.segment_id), false));
            }
            p
        };
        let mut out = Vec::new();
        'files: for (path, is_sealed) in paths {
            // Audit #1: mmap only sealed (immutable) segments. The active
            // segment is being appended concurrently — buffered read only.
            scan_file(&path, is_sealed, &mut |lsn, payload| {
                if lsn >= from && lsn < to && out.len() < max {
                    let (mut ep, _): (Episode, usize) =
                        bincode::serde::decode_from_slice(payload, BINCODE_CFG)
                            .map_err(|e| HeraclitusError::Serialization(e.to_string()))?;
                    self.decrypt_in_place(&mut ep);
                    out.push((lsn, ep));
                }
                Ok(())
            })?;
            if out.len() >= max {
                break 'files;
            }
        }
        out.sort_by_key(|(l, _)| *l);
        Ok(out)
    }

    /// Full integrity scan: every crc + every sealed footer Merkle root.
    pub fn verify(&self) -> Result<VerifyReport, HeraclitusError> {
        self.flush()?;
        let paths: Vec<(PathBuf, bool)> = {
            let inner = self.inner.lock().unwrap();
            let mut p: Vec<(PathBuf, bool)> = inner
                .sealed
                .iter()
                .map(|s| (s.path.clone(), true))
                .collect();
            p.push((segment_path(&self.dir, inner.active.segment_id), false));
            p
        };
        let mut report = VerifyReport::default();
        for (path, expect_sealed) in paths {
            // Audit #5: verify is read-only — it must NEVER truncate a file
            // (a concurrent append can look like a torn tail).
            let rec = recover_segment_opts(&path, false)?;
            report.segments += 1;
            report.records += rec.record_hashes.len() as u64;
            if expect_sealed {
                let root = merkle_root(&rec.record_hashes);
                match rec.blake3_root {
                    Some(stored) if stored == root => report.merkle_ok += 1,
                    Some(_) => {
                        return Err(HeraclitusError::Corruption {
                            context: format!("{}", path.display()),
                            detail: "blake3 merkle root mismatch".into(),
                        })
                    }
                    None => {}
                }
            }
        }
        Ok(report)
    }

    /// Sealed segment metadata (for tiering).
    pub fn sealed_segments(&self) -> Vec<SegmentMeta> {
        self.inner.lock().unwrap().sealed.clone()
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Decrypt an episode's content in place when a keystore is configured and
    /// the blob is sealed. A missing/destroyed key (crypto-shred) or a tamper
    /// yields the `[shredded]` tombstone — the on-disk log bytes never change.
    fn decrypt_in_place(&self, ep: &mut Episode) {
        let Some(ks) = &self.keystore else {
            return;
        };
        if !heraclitus_crypto::is_encrypted(&ep.content) {
            return; // legacy plaintext record — read as-is
        }
        let opened = ks
            .get(&ep.agent_id)
            .and_then(|key| heraclitus_crypto::open(&key, &ep.content, ep.agent_id.as_bytes()));
        ep.content = opened.unwrap_or_else(|| heraclitus_crypto::SHREDDED.to_vec());
    }
}

#[derive(Debug, Default)]
pub struct VerifyReport {
    pub segments: u64,
    pub records: u64,
    pub merkle_ok: u64,
}

fn new_active(dir: &Path, id: SegmentId, hlc: &Hlc) -> Result<Active, HeraclitusError> {
    let path = segment_path(dir, id);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .append(true)
        .open(&path)?;
    let header = SegmentHeader {
        version: format::FORMAT_VERSION,
        segment_id: id,
        created_hlc: hlc.now(),
    };
    file.write_all(&header.encode())?;
    file.sync_data()?;
    Ok(Active {
        file,
        segment_id: id,
        bytes_written: HEADER_LEN as u64,
        record_hashes: Vec::new(),
        min_lsn: u64::MAX,
        max_lsn: 0,
        last_sync: Instant::now(),
    })
}

struct RecoveredTail {
    valid_len: u64,
    record_hashes: Vec<[u8; 32]>,
    /// (lsn, byte-offset no ficheiro) de cada registo — alimenta o índice de
    /// offset por-LSN para `read` O(1) (seek directo em vez de varrer o segmento).
    locs: Vec<(Lsn, u64)>,
    min_lsn: Option<Lsn>,
    max_lsn: Option<Lsn>,
    sealed: bool,
    blake3_root: Option<[u8; 32]>,
    /// On-disk format version of this segment (from its header). Drives
    /// version-correct CRC/Merkle-leaf recomputation and the open-time
    /// decision to keep appending vs. seal-and-roll to the current version.
    version: u16,
}

/// Scan a segment file, validating header and every record crc.
/// If a torn write is found, truncate the file at the last valid offset
/// (torn-write recovery, §3.2) and emit the `CorruptionRecovered` metric.
fn recover_segment(path: &Path, _id: SegmentId) -> Result<RecoveredTail, HeraclitusError> {
    recover_segment_opts(path, true)
}

/// `allow_truncate = false` makes this a strictly read-only scan (used by
/// `verify`, audit #5): a torn tail is reported but never repaired here.
fn segment_id_from_path(path: &Path) -> SegmentId {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

fn recover_segment_opts(
    path: &Path,
    allow_truncate: bool,
) -> Result<RecoveredTail, HeraclitusError> {
    let data = std::fs::read(path)?;

    // A segment file whose header never reached disk: the writer was killed
    // during segment creation (a roll), between the file's `create` and the
    // header's `write` + `fsync`. Such a segment can hold no valid records, so
    // recover it as empty instead of failing — the M0 gate requires surviving a
    // crash at *any* point, including mid-roll. On the writable recovery path we
    // restore a proper header so the (last) segment is appendable again; `verify`
    // (read-only) just reports it as empty.
    if data.len() < HEADER_LEN || data[..4] != format::MAGIC {
        if allow_truncate {
            let mut f = OpenOptions::new().write(true).open(path)?;
            f.set_len(0)?;
            let header = SegmentHeader {
                version: format::FORMAT_VERSION,
                segment_id: segment_id_from_path(path),
                created_hlc: 0,
            };
            f.write_all(&header.encode())?;
            f.sync_all()?;
            tracing::warn!(path = %path.display(), "header-less segment recovered: reinitialized");
            metrics::counter!("heraclitus_corruption_recovered_total").increment(1);
        }
        return Ok(RecoveredTail {
            valid_len: HEADER_LEN as u64,
            record_hashes: Vec::new(),
            locs: Vec::new(),
            min_lsn: None,
            max_lsn: None,
            sealed: false,
            blake3_root: None,
            // Reinitialized (or empty) — treat as a fresh current-version tail.
            version: format::FORMAT_VERSION,
        });
    }

    let version = SegmentHeader::decode(&data)?.version;
    let mut offset = HEADER_LEN;
    let mut hashes = Vec::new();
    let mut locs: Vec<(Lsn, u64)> = Vec::new();
    let mut min_lsn = None;
    let mut max_lsn = None;
    let mut sealed = false;
    let mut root = None;

    while offset < data.len() {
        match format::decode_record(version, &data[offset..]) {
            Decoded::Record(lsn, _hlc, _payload, consumed) => {
                hashes.push(format::record_leaf(version, &data[offset..offset + consumed]));
                locs.push((lsn, offset as u64));
                min_lsn = Some(min_lsn.map_or(lsn, |m: u64| m.min(lsn)));
                max_lsn = Some(max_lsn.map_or(lsn, |m: u64| m.max(lsn)));
                offset += consumed;
            }
            Decoded::Footer(f) => {
                sealed = true;
                root = Some(f.blake3_root);
                offset += FOOTER_LEN;
                break;
            }
            Decoded::Torn => {
                if allow_truncate {
                    tracing::warn!(path = %path.display(), offset, "torn write recovered: truncating");
                    metrics::counter!("heraclitus_corruption_recovered_total").increment(1);
                    let f = OpenOptions::new().write(true).open(path)?;
                    f.set_len(offset as u64)?;
                    f.sync_all()?;
                } else {
                    tracing::debug!(path = %path.display(), offset, "torn tail observed (read-only scan)");
                }
                break;
            }
        }
    }

    Ok(RecoveredTail {
        valid_len: offset as u64,
        record_hashes: hashes,
        locs,
        min_lsn,
        max_lsn,
        sealed,
        blake3_root: root,
        version,
    })
}

fn seal_file(path: &Path, rec: &RecoveredTail) -> Result<(), HeraclitusError> {
    let mut file = OpenOptions::new().write(true).open(path)?;
    file.seek(SeekFrom::Start(rec.valid_len))?;
    let footer = SegmentFooter {
        record_count: rec.record_hashes.len() as u64,
        min_lsn: rec.min_lsn.unwrap_or(0),
        max_lsn: rec.max_lsn.unwrap_or(0),
        blake3_root: merkle_root(&rec.record_hashes),
    };
    file.write_all(&footer.encode())?;
    file.sync_all()?;
    Ok(())
}

/// Scan a segment file invoking `f(lsn, payload)` per record. Sealed
/// segments are mmap'd (read path, §3.2); the active segment is read
/// normally.
type RecordVisitor<'a> = &'a mut dyn FnMut(Lsn, &[u8]) -> Result<(), HeraclitusError>;

fn scan_file(path: &Path, use_mmap: bool, f: RecordVisitor<'_>) -> Result<(), HeraclitusError> {
    let file = File::open(path)?;
    let len = file.metadata()?.len() as usize;
    if len < HEADER_LEN {
        return Ok(());
    }
    // Audit #1: mmap ONLY for sealed (immutable) segments — mapping a file
    // under concurrent append is a TOCTOU hazard (SIGBUS / dirty reads).
    let mmap = if use_mmap {
        unsafe { memmap2::Mmap::map(&file) }.ok()
    } else {
        None
    };
    let owned;
    let data: &[u8] = match &mmap {
        Some(m) => &m[..],
        None => {
            let mut buf = Vec::with_capacity(len);
            let mut file = file;
            file.read_to_end(&mut buf)?;
            owned = buf;
            &owned
        }
    };
    let version = SegmentHeader::decode(data)?.version;
    let mut offset = HEADER_LEN;
    while offset < data.len() {
        match format::decode_record(version, &data[offset..]) {
            Decoded::Record(lsn, _h, payload, consumed) => {
                f(lsn, payload)?;
                offset += consumed;
            }
            Decoded::Footer(_) | Decoded::Torn => break,
        }
    }
    Ok(())
}

/// Pairwise blake3 Merkle root over record hashes. Empty list hashes to the
/// blake3 of the empty string (documented in LOG_FORMAT.md).
pub fn merkle_root(hashes: &[[u8; 32]]) -> [u8; 32] {
    if hashes.is_empty() {
        return *blake3::hash(b"").as_bytes();
    }
    let mut level: Vec<[u8; 32]> = hashes.to_vec();
    while level.len() > 1 {
        level = level
            .chunks(2)
            .map(|pair| {
                let mut h = blake3::Hasher::new();
                h.update(&pair[0]);
                h.update(pair.get(1).unwrap_or(&pair[0]));
                *h.finalize().as_bytes()
            })
            .collect();
    }
    level[0]
}

#[cfg(test)]
mod tests {
    use super::*;
    use heraclitus_core::EventKind;
    use proptest::prelude::*;

    fn ep(content: &str) -> Episode {
        Episode::new(
            "agent-1",
            EventKind::Observation,
            content.as_bytes().to_vec(),
        )
    }

    #[test]
    fn append_scan_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let log = Log::open(dir.path(), 1024 * 1024, FsyncPolicy::Always).unwrap();
        for i in 0..100 {
            let lsn = log.append(ep(&format!("event {i}"))).unwrap();
            assert_eq!(lsn, i);
        }
        let all = log.scan(0, u64::MAX).unwrap();
        assert_eq!(all.len(), 100);
        assert_eq!(all[42].1.content, b"event 42");
        assert_eq!(log.head(), 100);
    }

    #[test]
    fn reopen_continues_lsn() {
        let dir = tempfile::tempdir().unwrap();
        {
            let log = Log::open(dir.path(), 1024 * 1024, FsyncPolicy::Always).unwrap();
            for i in 0..10 {
                log.append(ep(&format!("e{i}"))).unwrap();
            }
        }
        let log = Log::open(dir.path(), 1024 * 1024, FsyncPolicy::Always).unwrap();
        assert_eq!(log.head(), 10);
        let lsn = log.append(ep("after reopen")).unwrap();
        assert_eq!(lsn, 10);
        assert_eq!(log.scan(0, u64::MAX).unwrap().len(), 11);
    }

    #[test]
    fn segment_roll_and_seal() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny segments force frequent rolls.
        let log = Log::open(dir.path(), 2048, FsyncPolicy::Always).unwrap();
        for i in 0..200 {
            log.append(ep(&format!("event number {i}"))).unwrap();
        }
        assert!(!log.sealed_segments().is_empty());
        let report = log.verify().unwrap();
        assert_eq!(report.records, 200);
        assert_eq!(report.merkle_ok, log.sealed_segments().len() as u64);
        // Everything still readable across segments.
        assert_eq!(log.scan(0, u64::MAX).unwrap().len(), 200);
    }

    #[test]
    fn torn_write_truncated_on_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let log = Log::open(dir.path(), 1024 * 1024, FsyncPolicy::Always).unwrap();
            for i in 0..20 {
                log.append(ep(&format!("e{i}"))).unwrap();
            }
        }
        // Corrupt the tail: append garbage simulating a torn write.
        let seg = segment_path(dir.path(), 0);
        let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
        f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02]).unwrap();
        f.sync_all().unwrap();

        let log = Log::open(dir.path(), 1024 * 1024, FsyncPolicy::Always).unwrap();
        assert_eq!(log.scan(0, u64::MAX).unwrap().len(), 20);
        // And the log keeps accepting appends after recovery.
        log.append(ep("post-recovery")).unwrap();
        assert_eq!(log.scan(0, u64::MAX).unwrap().len(), 21);
    }

    #[test]
    fn recovers_from_headerless_tail_segment() {
        // Regression: a crash during a segment roll leaves a newer segment file
        // whose header never reached disk (0 bytes / partial). Recovery must
        // treat it as empty, not fail with "bad magic or short header".
        let dir = tempfile::tempdir().unwrap();
        {
            let log = Log::open(dir.path(), 1024 * 1024, FsyncPolicy::Always).unwrap();
            for i in 0..5 {
                log.append(ep(&format!("e{i}"))).unwrap();
            }
        }
        // Simulate the half-created next segment (last in id order).
        std::fs::write(segment_path(dir.path(), 1), b"").unwrap(); // 0 bytes
        // A partial-header variant is handled the same way.
        // (Covered implicitly: data.len() < HEADER_LEN.)

        let log = Log::open(dir.path(), 1024 * 1024, FsyncPolicy::Always).unwrap();
        assert_eq!(log.scan(0, u64::MAX).unwrap().len(), 5, "no records lost");
        assert_eq!(log.head(), 5, "lsn continues past the empty tail");
        // The repaired tail is appendable, and the result verifies.
        let lsn = log.append(ep("after recovery")).unwrap();
        assert_eq!(lsn, 5);
        assert_eq!(log.scan(0, u64::MAX).unwrap().len(), 6);
        log.verify().unwrap();
    }

    #[test]
    fn cas_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let log = Log::open(dir.path(), 1024 * 1024, FsyncPolicy::Always).unwrap();
        log.append(ep("a")).unwrap();
        assert!(matches!(
            log.append_cas(0, ep("stale")),
            Err(HeraclitusError::CasConflict {
                expected: 0,
                head: 1
            })
        ));
        assert_eq!(log.append_cas(1, ep("fresh")).unwrap(), 1);
    }

    #[test]
    fn tail_subscribe_receives() {
        let dir = tempfile::tempdir().unwrap();
        let log = Log::open(dir.path(), 1024 * 1024, FsyncPolicy::Always).unwrap();
        let mut rx = log.tail_subscribe();
        log.append(ep("hello")).unwrap();
        let (lsn, e) = rx.try_recv().unwrap();
        assert_eq!(lsn, 0);
        assert_eq!(e.content, b"hello");
    }

    fn contains(hay: &[u8], needle: &[u8]) -> bool {
        hay.windows(needle.len()).any(|w| w == needle)
    }

    #[test]
    fn scan_window_pruning_and_cap() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny segments → many sealed segments → real pruning across them.
        let log = Log::open(dir.path(), 2048, FsyncPolicy::Always).unwrap();
        for i in 0..500 {
            log.append(ep(&format!("event {i}"))).unwrap();
        }
        // A narrow window returns exactly that LSN range (and only reads the
        // segments that overlap it — the rest are pruned).
        let w = log.scan(100, 110).unwrap();
        assert_eq!(w.len(), 10);
        assert_eq!(w.first().unwrap().0, 100);
        assert_eq!(w.last().unwrap().0, 109);
        // The row cap bounds the materialized result.
        assert_eq!(log.scan_capped(0, u64::MAX, 50).unwrap().len(), 50);
        // Uncapped full scan is still complete (no regression).
        assert_eq!(log.scan(0, u64::MAX).unwrap().len(), 500);
    }

    #[test]
    fn encryption_at_rest_and_crypto_shred() {
        let dir = tempfile::tempdir().unwrap();
        let keys = tempfile::tempdir().unwrap();
        let ks = heraclitus_crypto::KeyStore::open(keys.path()).unwrap();
        let log =
            Log::open_with_keystore(dir.path(), 1024 * 1024, FsyncPolicy::Always, Some(ks.clone()))
                .unwrap();
        log.append(ep("dados pessoais secretos")).unwrap();

        // Reads come back as plaintext (transparent to everything above the log).
        let all = log.scan(0, u64::MAX).unwrap();
        assert_eq!(all[0].1.content, b"dados pessoais secretos");

        // But the bytes on disk are ciphertext — no plaintext leak.
        let seg = std::fs::read(segment_path(dir.path(), 0)).unwrap();
        assert!(
            !contains(&seg, b"dados pessoais secretos"),
            "plaintext leaked to disk"
        );
        assert!(contains(&seg, &heraclitus_crypto::ENC_MAGIC[..]), "seal magic missing");

        // Crypto-shred the agent's key, then reopen (drop the in-memory cache):
        // the content is now permanently the tombstone, the log bytes untouched.
        assert!(ks.shred("agent-1").unwrap());
        let log2 = Log::open_with_keystore(
            dir.path(),
            1024 * 1024,
            FsyncPolicy::Always,
            Some(heraclitus_crypto::KeyStore::open(keys.path()).unwrap()),
        )
        .unwrap();
        let after = log2.scan(0, u64::MAX).unwrap();
        assert_eq!(after[0].1.content, heraclitus_crypto::SHREDDED);
    }

    #[test]
    fn v2_record_crc_covers_full_header() {
        let payload = b"the quick brown fox payload bytes";
        let rec = format::encode_record(format::FORMAT_VERSION, 7, 0xDEAD_BEEF_CAFE_F00D, payload);
        // Baseline: a clean v2 record decodes back to the same fields.
        assert!(matches!(
            format::decode_record(format::FORMAT_VERSION, &rec),
            format::Decoded::Record(7, 0xDEAD_BEEF_CAFE_F00D, p, _) if p == payload
        ));
        // Flipping ANY byte of the record — the len/lsn/hlc header fields too,
        // not just the payload — must be caught: the decoder may never hand back
        // the original (lsn, hlc, payload) as if untampered.
        for i in 0..rec.len() {
            for bit in 0..8u32 {
                let mut t = rec.clone();
                t[i] ^= 1 << bit;
                if let format::Decoded::Record(lsn, hlc, p, _) =
                    format::decode_record(format::FORMAT_VERSION, &t)
                {
                    assert!(
                        lsn != 7 || hlc != 0xDEAD_BEEF_CAFE_F00D || p != payload,
                        "v2: flip at byte {i} bit {bit} slipped through undetected"
                    );
                }
            }
        }
    }

    #[test]
    fn v1_accepts_hlc_flip_that_v2_rejects() {
        // Documents the hole the v2 format closes. Under v1 the CRC covers only
        // the payload, so flipping a byte in the hlc field decodes cleanly with
        // a *different* timestamp — silent retroactive tampering.
        let payload = b"audit me";
        let mut t = format::encode_record(1, 1, 1000, payload);
        t[16] ^= 0x01; // first byte of the hlc field
        assert!(
            matches!(
                format::decode_record(1, &t),
                format::Decoded::Record(_, hlc, _, _) if hlc != 1000
            ),
            "v1 should silently accept the forged hlc (the bug v2 fixes)"
        );
        // The same flip on a v2 record is rejected outright.
        let mut t2 = format::encode_record(2, 1, 1000, payload);
        t2[16] ^= 0x01;
        assert!(matches!(
            format::decode_record(2, &t2),
            format::Decoded::Torn
        ));
    }

    #[test]
    fn tampered_hlc_in_sealed_segment_is_caught_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            // Tiny segments → segment 0 seals with several records.
            let log = Log::open(dir.path(), 2048, FsyncPolicy::Always).unwrap();
            for i in 0..200 {
                log.append(ep(&format!("event number {i}"))).unwrap();
            }
            assert!(!log.sealed_segments().is_empty());
            assert_eq!(log.scan(0, u64::MAX).unwrap().len(), 200);
        }
        // Forge the timestamp (hlc) of the first record of sealed segment 0 —
        // exactly the retroactive-fraud move the v2 header CRC defends against.
        let seg0 = segment_path(dir.path(), 0);
        let mut data = std::fs::read(&seg0).unwrap();
        data[HEADER_LEN + 16] ^= 0x01; // first byte of record 0's hlc field
        std::fs::write(&seg0, &data).unwrap();

        // Reopen: under v1 (CRC over payload only) the flip slipped through and
        // all 200 survived; under v2 the CRC mismatch is caught on recovery, so
        // the forged record (and the rest of that segment) is dropped — the
        // tamper cannot pass silently.
        let log = Log::open(dir.path(), 2048, FsyncPolicy::Always).unwrap();
        assert!(
            log.scan(0, u64::MAX).unwrap().len() < 200,
            "forged hlc was silently accepted — record header is not integrity-protected"
        );
    }

    proptest! {
        /// A bit flip anywhere in the 24-byte record header (len/crc/lsn/hlc) is
        /// always detected under v2: the decoder never returns a clean record
        /// carrying the original (lsn, hlc, payload).
        #[test]
        fn prop_v2_any_header_byte_flip_detected(
            lsn in any::<u64>(),
            hlc in any::<u64>(),
            payload in proptest::collection::vec(any::<u8>(), 0..512),
            flip_in_header in 0usize..format::RECORD_HEADER_LEN,
            bit in 0u32..8,
        ) {
            let mut t = format::encode_record(format::FORMAT_VERSION, lsn, hlc, &payload);
            t[flip_in_header] ^= 1 << bit;
            if let format::Decoded::Record(l, h, p, _) =
                format::decode_record(format::FORMAT_VERSION, &t)
            {
                prop_assert!(l != lsn || h != hlc || p != &payload[..]);
            }
        }
    }
}
