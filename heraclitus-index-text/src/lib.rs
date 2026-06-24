//! heraclitus-index-text — derived BM25 inverted index (§3.6).

use heraclitus_core::{Episode, EventId, Lsn};
use heraclitus_memtable::tokenize;
use heraclitus_views::View;
use std::collections::HashMap;

const K1: f32 = 1.2;
const B: f32 = 0.75;

#[derive(Default)]
pub struct TextIndex {
    postings: HashMap<String, Vec<(u32, u32)>>, // term -> [(doc, tf)]
    doc_len: Vec<u32>,
    ids: Vec<EventId>,
    lsns: Vec<Lsn>,
    by_event: HashMap<EventId, u32>,
    total_len: u64,
    watermark: Lsn,
}

#[derive(Debug, Clone)]
pub struct TextHit {
    pub id: EventId,
    pub lsn: Lsn,
    pub score: f32,
}

impl TextIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    pub fn search(&self, query: &str, k: usize) -> Vec<TextHit> {
        let n = self.ids.len() as f32;
        if n == 0.0 {
            return Vec::new();
        }
        let avgdl = (self.total_len as f32 / n).max(1.0);
        let mut scores: HashMap<u32, f32> = HashMap::new();
        for term in tokenize(query) {
            let Some(plist) = self.postings.get(&term) else {
                continue;
            };
            let df = plist.len() as f32;
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
            for &(doc, tf) in plist {
                let dl = self.doc_len[doc as usize] as f32;
                let tf = tf as f32;
                let s = idf * (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + B * dl / avgdl));
                *scores.entry(doc).or_default() += s;
            }
        }
        let mut hits: Vec<TextHit> = scores
            .into_iter()
            .map(|(doc, score)| TextHit {
                id: self.ids[doc as usize],
                lsn: self.lsns[doc as usize],
                score,
            })
            .collect();
        hits.sort_by(|a, b| b.score.total_cmp(&a.score));
        hits.truncate(k);
        hits
    }
}

impl View for TextIndex {
    fn name(&self) -> &str {
        "text"
    }

    fn apply(&mut self, lsn: Lsn, event: &Episode) {
        self.watermark = lsn;
        if self.by_event.contains_key(&event.id) {
            return; // idempotent replay
        }
        let text = String::from_utf8_lossy(&event.content);
        let tokens = tokenize(&text);
        let doc = self.ids.len() as u32;
        self.by_event.insert(event.id, doc);
        self.ids.push(event.id);
        self.lsns.push(lsn);
        self.doc_len.push(tokens.len() as u32);
        self.total_len += tokens.len() as u64;

        let mut tf: HashMap<String, u32> = HashMap::new();
        for t in tokens {
            *tf.entry(t).or_default() += 1;
        }
        for (term, count) in tf {
            self.postings.entry(term).or_default().push((doc, count));
        }
    }

    fn watermark(&self) -> Lsn {
        self.watermark
    }

    fn reset(&mut self) {
        *self = TextIndex::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use heraclitus_core::EventKind;

    #[test]
    fn bm25_ranks_relevance() {
        let mut idx = TextIndex::new();
        let docs = [
            "the river flows into the sea",
            "no one steps in the same river twice",
            "fire is the element of change",
        ];
        let mut ids = Vec::new();
        for (i, d) in docs.iter().enumerate() {
            let e = Episode::new("a", EventKind::Observation, d.as_bytes().to_vec());
            ids.push(e.id);
            idx.apply(i as u64, &e);
        }
        let hits = idx.search("river", 3);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().all(|h| h.id == ids[0] || h.id == ids[1]));
        let fire = idx.search("fire change", 3);
        assert_eq!(fire[0].id, ids[2]);
    }
}
