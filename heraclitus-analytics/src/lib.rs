//! heraclitus-analytics — SQL analítico (DataFusion) sobre o log imutável.
//!
//! O log é *append-only* e a verdade; esta camada é **read-only** e derivada:
//! materializa os episódios num `RecordBatch` colunar (Apache Arrow — o mesmo
//! schema do espelho Parquet do `heraclitus-tier`) e regista-o como a tabela
//! `events` numa `SessionContext` do DataFusion. SQL OLAP (`SELECT ... GROUP
//! BY ...`) corre sobre isso sem descodificar bincode à mão e **sem tocar no
//! core**. Um snapshot `AS OF LSN n` limita a materialização a `lsn < n`.
//!
//! ```no_run
//! # async fn demo(log: &heraclitus_log::Log) -> Result<(), heraclitus_analytics::AnalyticsError> {
//! let a = heraclitus_analytics::LogAnalytics::from_log(log, None)?;
//! let rows = a.sql("SELECT agent_id, COUNT(*) AS n FROM events GROUP BY agent_id ORDER BY n DESC").await?;
//! # Ok(()) }
//! ```

pub use datafusion;
pub mod flight; // SPEC-016: data plane Flight (Arrow IPC) sobre o log
pub mod vectorized; // SPEC-012/013: motor de execução vetorizada Arrow

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use heraclitus_core::{EventKind, HeraclitusError, Lsn};
use heraclitus_log::Log;
use std::sync::Arc;

#[derive(Debug)]
pub enum AnalyticsError {
    Log(HeraclitusError),
    Arrow(String),
    Sql(String),
}

impl std::fmt::Display for AnalyticsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnalyticsError::Log(e) => write!(f, "log: {e}"),
            AnalyticsError::Arrow(e) => write!(f, "arrow: {e}"),
            AnalyticsError::Sql(e) => write!(f, "sql: {e}"),
        }
    }
}
impl std::error::Error for AnalyticsError {}
impl From<HeraclitusError> for AnalyticsError {
    fn from(e: HeraclitusError) -> Self {
        AnalyticsError::Log(e)
    }
}

pub(crate) fn kind_label(k: &EventKind) -> String {
    match k {
        EventKind::Custom(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

/// Sessão de analytics com a tabela `events` materializada do log.
pub struct LogAnalytics {
    ctx: SessionContext,
}

impl LogAnalytics {
    /// Materializa `events` a partir do log. `as_of = Some(n)` limita a
    /// `lsn < n` (snapshot temporal); `None` = tudo até ao head. O scan é
    /// janelado (`scan_capped`) para não materializar milhões de episódios
    /// num único Vec de golpe.
    pub fn from_log(log: &Log, as_of: Option<Lsn>) -> Result<Self, AnalyticsError> {
        let head = log.head();
        let to = as_of.unwrap_or(head).min(head);

        let mut lsns: Vec<u64> = Vec::new();
        let mut ids: Vec<String> = Vec::new();
        let mut agents: Vec<String> = Vec::new();
        let mut sessions: Vec<String> = Vec::new();
        let mut ts: Vec<u64> = Vec::new();
        let mut kinds: Vec<String> = Vec::new();
        let mut contents: Vec<String> = Vec::new();
        let mut attrs_json: Vec<String> = Vec::new();
        let mut valid_from: Vec<u64> = Vec::new();
        let mut valid_to: Vec<u64> = Vec::new();

        let mut cur = 0u64;
        while cur < to {
            let batch = log.scan_capped(cur, to, 50_000)?;
            let Some(&(last, _)) = batch.last() else {
                break;
            };
            for (lsn, e) in &batch {
                lsns.push(*lsn);
                ids.push(e.id.to_string());
                agents.push(e.agent_id.clone());
                sessions.push(e.session_id.clone());
                ts.push(e.ts_hlc);
                kinds.push(kind_label(&e.kind));
                contents.push(String::from_utf8_lossy(&e.content).into_owned());
                attrs_json.push(serde_json::to_string(&e.attrs).unwrap_or_else(|_| "{}".into()));
                valid_from.push(e.valid_from.unwrap_or(0));
                valid_to.push(e.valid_to.unwrap_or(0));
            }
            cur = last + 1;
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("lsn", DataType::UInt64, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("agent_id", DataType::Utf8, false),
            Field::new("session_id", DataType::Utf8, false),
            Field::new("ts_hlc", DataType::UInt64, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
            Field::new("attrs_json", DataType::Utf8, false),
            // 0 = ausente (aberto); o SQL pode filtrar `valid_from > 0`.
            Field::new("valid_from", DataType::UInt64, false),
            Field::new("valid_to", DataType::UInt64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(lsns)) as ArrayRef,
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(agents)),
                Arc::new(StringArray::from(sessions)),
                Arc::new(UInt64Array::from(ts)),
                Arc::new(StringArray::from(kinds)),
                Arc::new(StringArray::from(contents)),
                Arc::new(StringArray::from(attrs_json)),
                Arc::new(UInt64Array::from(valid_from)),
                Arc::new(UInt64Array::from(valid_to)),
            ],
        )
        .map_err(|e| AnalyticsError::Arrow(e.to_string()))?;

        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| AnalyticsError::Arrow(e.to_string()))?;
        ctx.register_table("events", Arc::new(table))
            .map_err(|e| AnalyticsError::Arrow(e.to_string()))?;
        Ok(Self { ctx })
    }

    /// Executa SQL sobre `events` e devolve as linhas como `Vec` de objetos
    /// JSON (uma chave por coluna). A fonte é imutável — isto é read-only.
    pub async fn sql(&self, query: &str) -> Result<Vec<serde_json::Value>, AnalyticsError> {
        let df = self
            .ctx
            .sql(query)
            .await
            .map_err(|e| AnalyticsError::Sql(e.to_string()))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| AnalyticsError::Sql(e.to_string()))?;
        let buf = Vec::new();
        let mut writer = datafusion::arrow::json::ArrayWriter::new(buf);
        for b in &batches {
            writer
                .write(b)
                .map_err(|e| AnalyticsError::Arrow(e.to_string()))?;
        }
        writer
            .finish()
            .map_err(|e| AnalyticsError::Arrow(e.to_string()))?;
        let bytes = writer.into_inner();
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|e| AnalyticsError::Arrow(e.to_string()))?;
        Ok(value.as_array().cloned().unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use heraclitus_core::{Episode, FsyncPolicy};

    #[tokio::test]
    async fn sql_group_by_over_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let log = Log::open(dir.path(), 1 << 20, FsyncPolicy::Always).unwrap();
        for i in 0..12 {
            let mut e = Episode::new(
                if i % 3 == 0 { "alice" } else { "bob" },
                EventKind::Observation,
                format!("evento {i}").into_bytes(),
            );
            e.attrs.insert("topic".into(), "rios".into());
            log.append(e).unwrap();
        }

        let a = LogAnalytics::from_log(&log, None).unwrap();
        let rows = a
            .sql("SELECT agent_id, COUNT(*) AS n FROM events GROUP BY agent_id ORDER BY agent_id")
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["agent_id"], "alice");
        assert_eq!(rows[0]["n"], 4);
        assert_eq!(rows[1]["agent_id"], "bob");
        assert_eq!(rows[1]["n"], 8);

        // Snapshot AS OF: só lsn < 6.
        let a6 = LogAnalytics::from_log(&log, Some(6)).unwrap();
        let total = a6.sql("SELECT COUNT(*) AS n FROM events").await.unwrap();
        assert_eq!(total[0]["n"], 6);

        // Colunas de valid time expostas ao SQL.
        let cols = a
            .sql("SELECT lsn, kind, valid_from FROM events WHERE lsn = 0")
            .await
            .unwrap();
        assert_eq!(cols[0]["kind"], "Observation");
        assert_eq!(cols[0]["valid_from"], 0);
    }
}
