<div align="center">
  <h1>HeraclitusDB</h1>
  <p><b>O Substrato de Memória Event-Sourced para Agentes de IA</b></p>
  <p><i>"Panta rhei — nenhum homem pisa no mesmo rio duas vezes."</i></p>
</div>

---

> Nietzsche considerava Heráclito seu único verdadeiro precursor.
> O NietzscheDB apostou que a inteligência emerge de geometria rica e autonomia interna.
> **O HeraclitusDB aposta que a memória de AGI exige tempo imutável, proveniência total e geometria aprendida — e que a inteligência vive no agente, não no banco de dados.**

---

## 0. Tese de Design (leia isto antes de escrever qualquer código)

1. **O log é a verdade.** Um único log de *episódios*, append-only e imutável, é a única fonte da verdade. Todo o resto — adjacência do grafo, índices vetoriais, fatos semânticos, escores de ativação — é uma **view materializada**: derivada, assíncrona e reconstruível por replay determinístico.
2. **O tempo é físico, não simulado.** A causalidade vem da ordem no log (+ relógios lógicos híbridos quando distribuído). Não embutimos o espaço-tempo; nós *somos* a linha do tempo.
3. **A geometria é aprendida, não decretada.** Os embeddings vivem numa **variedade produto** `P = H^a(κ₁) × S^b(κ₂) × E^c` cuja assinatura e curvaturas são estimadas a partir da distorção dos dados — e reajustadas durante a compactação.
4. **Consolidação de memória é compactação.** A destilação episódico→semântica roda como compactação estilo LSM em background ("sono"). Cada fato semântico carrega **ponteiros de proveniência** para os episódios que o geraram.
5. **Esquecimento é propriedade do índice, não destruição de dados.** Dados frios são desindexados e movidos para object storage com recibos criptográficos de rebaixamento. Nada no log é jamais mutado.
6. **O core é entediante e brutal.** Sem clientes HTTP embutidos, sem daemons autônomos mutando dados, sem scheduler. Snapshots MVCC, replicação Raft e testes determinísticos de simulação no CI desde o primeiro dia.

**Não-objetivos:** ser um framework de agentes; inventar uma linguagem de consulta nova; embutir chamadas a LLM na engine de armazenamento.

---

## 1. Estrutura do Repositório

```
heraclitusdb/
├── Cargo.toml                     # raiz do workspace (resolver = "2")
├── rust-toolchain.toml            # canal stable (NÃO nightly)
├── Justfile                       # comandos de dev: just test, just sim, just fuzz, just bench
├── README.md
├── LICENSE                        # Apache-2.0
├── docs/
│   ├── ARCHITECTURE.md            # diagramas + fluxo de dados
│   ├── LOG_FORMAT.md              # spec do formato binário (versionado)
│   ├── GEOMETRY.md                # matemática da variedade produto + estimação de curvatura
│   ├── ACTIVATION.md              # ativação ACT-R: spec exato + aproximação
│   ├── CONSISTENCY.md             # modelo formal de consistência (DEVE existir antes da v0.1)
│   └── RFCs/                      # um RFC por decisão de design importante
├── crates/
│   ├── heraclitus-core/           # tipos compartilhados, IDs, erros, config
│   ├── heraclitus-log/            # A fonte da verdade: log append-only segmentado
│   ├── heraclitus-manifold/       # engine de geometria da variedade produto
│   ├── heraclitus-memtable/       # índice da cauda em RAM (read-your-own-writes)
│   ├── heraclitus-views/          # engine de views materializadas + replay + watermarks
│   ├── heraclitus-index-vector/   # ANN (HNSW) com métrica plugável da variedade produto
│   ├── heraclitus-index-graph/    # índices de adjacência/propriedade derivados (RocksDB)
│   ├── heraclitus-index-text/     # índice invertido BM25 (derivado)
│   ├── heraclitus-activation/     # store de ativação ACT-R (incremental, aprox. O(1))
│   ├── heraclitus-retrieval/      # recuperação em 2 estágios: fusão de recall + API de reranker
│   ├── heraclitus-distill/        # compactação episódico→semântico + proveniência
│   ├── heraclitus-tier/           # tiering frio para object storage + recibos Merkle
│   ├── heraclitus-txn/            # snapshots MVCC sobre offsets do log
│   ├── heraclitus-query/          # parser do subconjunto Cypher/GQL + AS OF temporal
│   ├── heraclitus-raft/           # replicação (openraft) — atrás de feature flag
│   ├── heraclitus-server/         # gRPC (tonic) + REST mínimo (axum)
│   ├── heraclitus-proto/          # definições protobuf
│   ├── heraclitus-cli/            # CLI de admin & inspeção
│   └── heraclitus-client/         # SDK Rust
├── sim/
│   └── heraclitus-sim/            # testes determinísticos de simulação (turmoil)
├── fuzz/                          # alvos cargo-fuzz (decode do log, parser de query, ops do manifold)
├── benches/                       # criterion micro + harness ann-benchmarks
├── tests/                         # testes de integração cross-crate (injeção de falhas!)
├── sdk/
│   └── python/                    # SDK Python (heraclitusdb): connect / append / query / recall
├── mcp/                           # servidor Model Context Protocol (acesso de agentes LLM)
├── console/                       # console web: grafo + linha do tempo + proveniência + visão de fraude
├── bi/                            # export de BI / analytics
├── bench/
│   └── locomo/                    # harness do benchmark de memória agêntica LOCOMO
├── demo/                          # demos + ETLs do mundo real (Portal da Transparência)
└── windows/                       # instalação/deploy como serviço do Windows
```

---

## 2. Dependências Centrais (workspace `[workspace.dependencies]`)

| Finalidade | Crate | Notas |
|---|---|---|
| Runtime async | `tokio` | features completas no server; crates do core ficam runtime-agnósticas quando possível |
| gRPC | `tonic`, `prost` | superfície da API |
| REST (só admin) | `axum` | camada fina sobre o mesmo serviço |
| Serialização (log) | `rkyv` *ou* `bincode` v2 | leituras zero-copy preferidas; **escolha uma no RFC-001 e nunca misture** |
| Serialização (API) | `serde`, `serde_json` | |
| Checksums | `crc32fast`, `blake3` | crc por registro, blake3 para recibos Merkle |
| Mmap | `memmap2` | caminho de leitura dos segmentos selados |
| Storage das views | `rocksdb` | estado derivado APENAS — descartável a qualquer momento |
| Mapas concorrentes | `dashmap` | memtable + hot set de ativação |
| Bitmaps | `roaring` | push-down de filtro no ANN |
| Parser | `pest` | gramática do subconjunto Cypher/GQL |
| Consenso | `openraft` | feature `replication` |
| Object storage | `object_store` (Apache) | S3/GCS/local para o tier frio |
| Observabilidade | `tracing`, `metrics`, `metrics-exporter-prometheus` | |
| Property testing | `proptest` | obrigatório para manifold + log |
| Fuzzing | `cargo-fuzz` | job de CI, orçamento de 10 min por alvo |
| Simulation testing | `turmoil` | testes de partição de rede / clock-skew para raft + views |
| Benchmarks | `criterion` | |
| Reranker (opcional) | `ort` (ONNX Runtime) | feature `rerank-onnx`; padrão = scorer linear, sem ONNX |

**Toolchain: Rust stable.** Sem features nightly. Esta é uma decisão deliberada de confiabilidade.

---

## 3. Especificações dos Módulos

### 3.1 `heraclitus-core`

- `EventId` = ULID (ordenado no tempo, 128-bit). `FactId`, `SegmentId`, `Lsn` (número de sequência do log, u64).
- `Episode { id, ts_hlc, agent_id, session_id, kind, content: Bytes, embedding: ProductPoint, attrs: Map, parents: Vec<EventId> }`
- `Fact { id, statement, embedding, confidence, provenance: Vec<EventId>, derived_at_lsn }`
- Taxonomia de erros: `StorageError`, `CorruptionError` (nunca engolido em silêncio), `GeometryError`.
- Toda a config via uma struct `HeraclitusConfig`, carregável de TOML + overrides por env.

### 3.2 `heraclitus-log` — o único escritor da verdade

Formato binário de segmento (spec em `docs/LOG_FORMAT.md`, byte de versão obrigatório):

```
[Cabeçalho do Segmento: magic "HRKL" | format_version u16 | segment_id | created_hlc]
[Record]* onde Record = [len u32][crc32 u32][lsn u64][hlc u64][payload]
[Rodapé do Segmento ao selar: record_count | min_lsn | max_lsn | blake3_root]
```

- Caminho de append: serializa → crc → escreve → política de `fsync` (`always` | `group_commit(interval_ms)` — padrão group commit 5ms).
- Segmentos rolam aos 256 MB (config). Segmentos selados são imutáveis, mmap'd, e ganham uma raiz Merkle blake3 (usada por recibos de tiering e anti-entropia).
- **Recuperação de torn-write:** ao abrir, varre o último segmento não selado; trunca no primeiro mismatch de crc; registra uma métrica `CorruptionRecovered`. Escreva um teste de injeção de falhas que mata o processo no meio do append 1.000 vezes e afirma a recuperação (use um harness de processo-filho).
- API de leitura: `read(lsn)`, `scan(range)`, `tail_subscribe() -> broadcast::Receiver<(Lsn, Episode)>` — esta subscrição alimenta o memtable e as views.

### 3.3 `heraclitus-manifold` — geometria produto aprendida

Tipo de ponto:

```rust
pub struct ProductPoint {
    pub hyp: Vec<f32>,   // componente bola de Poincaré, ‖x‖ < 1, curvatura κ₁ < 0
    pub sph: Vec<f32>,   // componente esfera unitária, ‖x‖ = 1, curvatura κ₂ > 0
    pub euc: Vec<f32>,   // componente Euclidiana
}
```

- `dist(a, b) = sqrt( w₁·d_H(a,b)² + w₂·d_S(a,b)² + w₃·d_E(a,b)² )` — agregação de distância-ao-quadrado (padrão para variedades produto).
- Implemente por componente: distância geodésica, `exp_map` / `log_map`, adição de Möbius (hyp), ponto médio esférico. Promova para f64 internamente perto da fronteira de Poincaré; faça clamp das normas com epsilons documentados.
- **Estimação de curvatura/assinatura** (`estimate.rs`): dada uma amostra de distâncias par-a-par do grafo vs. distâncias do embedding, calcule a distorção por assinatura candidata; exponha `fit_signature(sample) -> Signature{a,b,c,κ₁,κ₂}`. Usada offline por `heraclitus-distill` durante a compactação; o re-embedding é uma *nova* versão de view derivada, nunca uma mutação in-place.
- **Invariantes garantidos por `debug_assert!` + proptest:** norma da bola < 1, norma da esfera = 1 ± 1e-6, erro de roundtrip exp∘log < 1e-4 sobre 10 projeções encadeadas, simetria de distância + desigualdade triangular (amostradas).

### 3.4 `heraclitus-memtable` — resolve o read-your-own-writes

A fraqueza conhecida das views assíncronas é a amnésia de curto prazo. Resolva do jeito LSM:

- Subscreve ao `tail_subscribe()`. Mantém os últimos N eventos (padrão: tudo acima do **watermark da view**, limitado a 100k) em RAM:
  - varredura vetorial força-bruta (array plano amigável a SIMD de `ProductPoint`s) — exato, ok para ≤100k,
  - pequeno overlay de adjacência (DashMap),
  - texto tokenizado para merge BM25.
- Toda query executa como `merge(memtable_results, view_results)` com dedup por LSN. **Um agente deve sempre ver a própria escrita na query seguinte.** Este é um requisito rígido de correção com teste de integração: escreve → consulta em até 1ms → afirma visível.

### 3.5 `heraclitus-views` — engine de replay

- Um trait `View`: `fn apply(&mut self, lsn: Lsn, event: &Episode)`, `fn watermark(&self) -> Lsn`, `fn checkpoint(&self)`, `fn rebuild_from(&mut self, lsn: Lsn)`.
- Views registradas: índice de grafo, índice vetorial, índice de texto, store de ativação, store de fatos.
- Watermarks persistidos no RocksDB; na inicialização cada view re-executa `(watermark, head]`. **`heraclitus-cli rebuild --view X` deve sempre funcionar a partir do LSN 0** — esta é a história de recuperação; teste no CI apagando o RocksDB e afirmando estado de view bit-idêntico após replay (determinismo!).
- Toda aplicação de view deve ser determinística: sem leituras de relógio de parede, sem RNG sem uma seed derivada do LSN.

### 3.6 `heraclitus-index-vector`

- HNSW no próprio crate (**não** dependa de um crate externo de HNSW — a métrica é uma distância customizada da variedade produto e precisamos de push-down de filtro).
- Padrões `M=16, ef_construction=200`; pré-filtragem com RoaringBitmap; filtros de metadados (Eq/In/Range/And).
- Persistência: serializa o grafo para uma CF do RocksDB no checkpoint (é derivado — perdê-lo significa replay, não perda de dados).
- `search(query: ProductPoint, k, filter, snapshot_lsn)` — resultados carregam o LSN em que são válidos.

### 3.7 `heraclitus-activation` — ACT-R, feito O(1)

Ativação de nível base: `Bᵢ = ln Σⱼ tⱼ^(−d)` (d padrão 0.5). O cálculo exato sobre todos os timestamps é inviável em escala, então por item armazene:

```rust
pub struct ActivationRecord {
    recent: ArrayVec<u64, 8>,  // últimos 8 timestamps de acesso — cabeça exata
    n: u64,                    // contagem total de acessos
    first_access: u64,         // âncora de tempo de vida
}
```

- Aproximação (híbrida estilo Petrov): soma exata sobre os 8 acessos recentes + cauda em forma fechada `((n − k) · L^(1−d)) / (1 − d) / L` onde `L` = tempo de vida. Documente a fórmula e seu limite de erro em `docs/ACTIVATION.md`; property-test contra o cálculo exato (erro relativo < 5% para n ≤ 10k traces sintéticos).
- Atualizações são O(1) no acesso; scoring é O(1) no tempo de query. O decaimento **não precisa de job em background** — ele cai da fórmula no momento da leitura.
- Ativação por espalhamento: soma ponderada de um salto a partir do conjunto de contexto da query, fan-out limitado a 64.

### 3.8 `heraclitus-retrieval` — dois estágios

1. **Recall:** roda em paralelo — ANN top-200, BM25 top-200, ativação top-200 — funde com **RRF** (Reciprocal Rank Fusion, k=60).
2. **Rerank:** trait `Reranker { fn score(&self, query, candidate) -> f32 }`. Padrão: mistura linear calibrada de (distância no manifold, BM25, ativação, recência). A feature opcional `rerank-onnx` carrega um cross-encoder. O trait também aceita feedback: `fn observe(&mut self, query_id, chosen, outcome)` — persistido como eventos de log comuns (`kind = RetrievalFeedback`) para que o reranker possa ser retreinado offline a partir do próprio log.

### 3.9 `heraclitus-distill` — consolidação como compactação

- Disparado por política (contagem de episódios / staleness / manual), **nunca** concorrentemente consigo mesmo; rate-limited; com orçamento de CPU (config `compaction_max_cores`).
- Pipeline: clusteriza embeddings episódicos (densidade estilo HDBSCAN ou aglomerativo simples no manifold) → para cada cluster estável emite um `Fact` **como um novo evento de log** (`kind = FactDerived`) com `provenance = [ids dos episódios]` → opcionalmente chama `manifold::fit_signature` sobre o corpus e, se a melhoria de distorção > threshold, agenda um job de re-embedding que produz uma nova *versão* do índice vetorial (swap blue/green num watermark).
- **Fatos também são eventos de log.** O log continua a única fonte da verdade mesmo para conhecimento derivado; as views indexam fatos como qualquer outra coisa.

### 3.10 `heraclitus-tier` — esquecimento com recibos

- Política de rebaixamento: ativação `Bᵢ` abaixo do threshold por T dias E não fixado por proveniência de nenhum fato quente.
- Rebaixar = remover dos índices quentes + enviar faixas de segmento selado para o `object_store` + acrescentar um evento de log `DemotionReceipt` contendo a prova Merkle blake3. Recall-on-demand: uma flag de query `INCLUDE COLD` busca e re-indexa temporariamente.
- Nada é jamais deletado. Apagamento estilo LGPD/GDPR (a única exceção legítima) = crypto-shredding: chaves de criptografia por `agent_id`; o apagamento destrói a chave. Documentado em `docs/CONSISTENCY.md`.

### 3.11 `heraclitus-txn`

- Um snapshot = um LSN. `begin_snapshot() -> Lsn`; todas as leituras o carregam; as views respondem "as of ≤ LSN" via seu watermark + merge do memtable.
- Escritas: append single-writer-por-processo (o log serializa); CAS otimista `expected_lsn` para fluxos de compare-and-append. Sem transações de escrita interativas multi-statement na v0.x — documente isso honestamente.

### 3.12 `heraclitus-query`

- **Não invente uma linguagem.** Subconjunto Cypher/GQL: `MATCH`, `WHERE`, `RETURN`, `ORDER BY`, `LIMIT`, `CREATE` (→ append no log), mais extensões:
  - `AS OF LSN n` / `AS OF TIMESTAMP t` (leituras temporais),
  - `NEAREST(embedding, k)` (recall vetorial),
  - `RECALL("texto", k)` (recuperação completa em dois estágios como função de tabela),
  - `PROVENANCE(fact)` (expande ponteiros de proveniência).
- Gramática Pest; **faça fuzz do parser desde o primeiro dia**. Planner v0 é baseado em regras; colete contagens por campo para habilitar decisões baseadas em custo depois. `EXPLAIN` a partir da v0.1.

### 3.13 `heraclitus-raft` (feature `replication`, depois do M4)

- openraft sobre o log: o log *é* a entrada da máquina de estados; followers re-executam para suas próprias views. Snapshot = segmentos selados + checkpoints de view. Testes de simulação turmoil: partição, kill de líder, clock skew — eventos appended-and-acked devem sobreviver ao failover (esta é a garantia principal; teste, depois reivindique).

### 3.14 `heraclitus-server` / `heraclitus-cli`

- gRPC: `Append`, `Query`, `Recall`, `Subscribe(tail)`, `Snapshot`, `Admin{rebuild, demote, stats}`.
- CLI: `heraclitus log inspect`, `heraclitus view rebuild`, `heraclitus verify` (scan completo de crc + Merkle), `heraclitus bench`.

---

## 4. Milestones (construa nesta ordem — cada milestone entrega com seus testes)

**M0–M7 constroem o motor** (o leito do rio). **M8–M18 são o roadmap v2.0** — o grafo causal, contrafactual, decisório e adaptativo (detalhe em [versao-2.0.md](../versao-2.0.md)).

| Milestone | Entrega | Critério de aceitação |
|---|---|---|
| **M0** | esqueleto do workspace, `core`, `log` (append/scan/recover) | teste de injeção de falhas de 1.000 iterações verde; alvo de fuzz no decode de registro |
| **M1** | `manifold` (distâncias, mapas, invariantes) | suíte proptest verde; baselines do criterion registradas |
| **M2** | `memtable` + `views` + índices vetor/texto/grafo | teste de determinismo apaga-e-replay; teste read-your-own-writes < 1ms |
| **M3** | `activation` + `retrieval` (RRF + reranker linear) | teste do limite de erro da aproximação; demo de recall ponta-a-ponta |
| **M4** | `txn` (snapshots) + `query` (subconjunto GQL + AS OF) | fuzz do parser 10min limpo; `EXPLAIN` funciona; teste de query temporal |
| **M5** | `distill` + `tier` (fatos, proveniência, storage frio) | teste de round-trip de proveniência; verificação de recibo de rebaixamento |
| **M6** | replicação `raft` + `server`/`cli`/`client` | suíte de partição turmoil verde; failover perde zero escritas acked |
| **M7** | benchmarks vs Qdrant/pgvector em datasets hierárquicos (WordNet) | curvas QPS×recall publicadas em `benches/REPORT.md` |
| **M8** | engine de grafo v1 (`NEIGHBORS`, `TRAVERSE`) — 100% derivada do log | travessias determinísticas; `state_hash` blake3 estável |
| **M9** | grafo temporal (`MATCH (a)-[r]->(b) AS OF LSN X`) | iguala o replay parcial; assert/retract de arestas vivas por LSN |
| **M10** | engine híbrida (`FUSE` grafo + vetor + texto) | a fusão supera os canais isolados; pesos versionados |
| **M11** | entity resolution (`RESOLVE`, `CLUSTER`) | clustering determinístico; merge/split reproduzíveis por replay |
| **M12** | hypothesis graph (`HYPOTHESES`) — crença por log-odds | versões de aresta concorrentes coexistem (FRAUD_PARTNER vs NOT_RELATED) |
| **M13** | consultas causais (`WHY`) | retorna a cadeia causal mínima, combinando com a proveniência |
| **M14** | graph analytics (`COMMUNITY`, `METRICS`) | componentes conexas & z-score replay-stable |
| **M15** | camada de decisão (`DECIDE`) — regras geram eventos `Action` no log | idempotente por `action_id` |
| **M16** | motor contrafactual (`SIMULATE ADD/REMOVE EDGE ... THEN <q>`) | muda o resultado sem alterar o log (grafo virtual, divergência isolada) |
| **M17** | grafo adaptativo (`ADAPT`) — aprende o threshold de decisão do feedback | replay-stable; F1 melhora |
| **M18** | contrato de consistência (`REQUIRE LSN >= X`) | falha explicitamente se o backend não atingiu X (sem leituras de "grafo atrasado") |

CI (GitHub Actions): fmt + clippy (deny warnings) + test + proptest + fuzz de 10 min + simulação turmoil + checagem de regressão do criterion.

---

## 5. O que o HeraclitusDB deliberadamente NÃO tem

Sem cliente HTTP embutido, sem scheduler cron, sem daemons autônomos mutando dados, sem chamadas a LLM dentro da engine, sem linguagem de consulta inventada, sem Rust nightly. A agência pertence ao agente. O trabalho do banco de dados é ser **o leito do rio: imutável, auditável e impossível de corromper em silêncio.**

---

## 6. Licença & Citação

Apache-2.0. `CITATION.cff` a partir da v0.1.

---

*Concebido em diálogo entre José R. F. Junior e Claude (Anthropic), junho de 2026 — como o contraponto de engenharia ao NietzscheDB.*
