# SPEC-new/STATUS.md — Estado real dos documentos SPEC-new

**Gerado:** 2026-07-08/09 · **Método:** auditoria automatizada multi-agente
(extração de afirmações + verificação adversarial), cada afirmação verificada
com grep/leitura contra o código real em `crates/*/src`. `graphify-out/`
excluído (é cache, dá falsos positivos).

> **TL;DR:** os documentos em `SPEC-new/` são **PROPOSTAS (RFCs), não
> implementação**. Vários vendem como "código real", "auditado", "CONGELADO" ou
> "APROVADO" componentes que **não existem em nenhum crate**. Este ficheiro lista
> cada afirmação falsa/enganosa com a evidência. O estado real da plataforma e o
> roteiro estão em [../PLANO-SPECS.md](../PLANO-SPECS.md).

## ATUALIZAÇÃO 2026-07-09 — SPEC-009-035 implementados (módulos reais)

A pedido, os SPEC-009 a 035 foram **implementados como módulos Rust reais, que
compilam e passam testes** (workspace: 206 → **254 testes, 0 falhas**), adaptados
aos tipos reais do código (`Lsn`/`SegmentId` são `u64`, não newtypes; nada do
código v3.2.0 verbatim, que não compilava).

> **Distinção honesta:** "✅ módulo" = tipo/trait/lógica implementados **e
> testados** em unidade. **Wired = ❌** significa que o módulo existe e funciona,
> mas **ainda não está ligado ao caminho vivo** (planner/servidor/gRPC) — isso é
> trabalho de integração, não de implementação. Nenhum destes é "engine de
> produção completo"; são o **contrato + implementação de referência** que os
> specs descrevem.

| SPEC | Módulo real | Testado | Wired ao motor vivo |
|---|---|---|---|
| 009 | `core::canonical::CanonicalKeyCodec` · `index_graph::dense_map` | ✅ | 🟡 `CanonicalKeyCodec` **wired** no `index-attr` (range scans; corrigiu bug −0.0/+0.0). `DenseEntityMap` ainda não usado |
| 010 | `zone_map::ZoneMap` (lsn/ts/agent/session/attrs) + `skip_scan::SkipScanner` (+ sidecar `.zmap`) + pushdown GQL `scan_builtin_eq` | ✅ | ✅ **ponta-a-ponta**: query `WHERE agent_id/session_id=…` → planner → skip por zone map → sidecar persistente (cold-boot). Salta segmentos, nunca perde match. |
| 011 | `core::runtime` (StorageEngine, DatabaseManifest, DerivedExecutionArtifact, budgets) · `txn::SnapshotManager` | ✅ | 🟡 parcial |
| 012/013 | `core::ir` (LogicalPlan/PhysicalIr/DAG) · `core::cost` | ✅ | ❌ |
| 014 | `index_graph::provenance::ProvenanceEngine` · `core::dispatcher` · query `WHY(…) UNTIL "cause"` (minimal chain) | ✅ | ✅ **WHY UNTIL wired na GQL** (gramática→AST→plan→backend); minimal causal chain, shortest path testado |
| 016 | `core::flight::FlightService` (contrato) | ✅ | ❌ (falta gRPC+Arrow IPC) |
| 019 | `core::consistency::IsolationLevel` | ✅ | ❌ |
| 022 | `core::streaming::StreamSubscriber` | ✅ | ❌ |
| 023 | **HQL — REJEITADO por design** (mantém GQL) | — | — |
| 024 | `core::contracts` (Planner/Optimizer/TaskScheduler/SegmentCatalog) | ✅ | ❌ |
| 025 | `core::plugin` (HeraclitusPlugin + PluginHost) | ✅ | ❌ |
| 026 | `core::capability::CapabilityCatalog` (detect real) | ✅ | ❌ |
| 027 | `EventKind::SystemMetric` · `core::telemetry` | ✅ | ❌ (falta thread de telemetria) |
| 028/031 | `core::artifact_registry` (registry + evicção em cascata) | ✅ | ❌ |
| 029 | `core::format_version::StorageFormatVersion` (negociação) | ✅ | ❌ (log usa `u16` simples) |
| 030 | `index_graph::GraphIndex::state_hash` + trait `View` | ✅ | ✅ |
| 032 | `core::cost::EmaCalibrator` | ✅ | ❌ |
| 033 | `core::numa` (política; pinning real = follow-up OS) | ✅ | ❌ |
| 034 | `core::ebr::Versioned<T>` (reclamação por Arc) | ✅ | ❌ |
| 035 | `core::sandbox::run_sandboxed` (crash boundary; WASM = follow-up) | ✅ | ❌ |
| 015/021 | `raft` log-shipping v0 + hardening (partição/heal/state_hash) | ✅ | 🟡 |
| 020 | crash recovery (torn-write) — **já existia** no log | ✅ | ✅ |

**Próximo nível (não feito):** *wiring* — pôr o `ZoneMap` no planner de scan, o
`CanonicalKeyCodec` nos índices, expor `FlightService` por gRPC, ligar a thread de
telemetria, enforçar `IsolationLevel` no servidor. E os que são honestamente
"referência, não produção": Flight real (gRPC+Arrow), NUMA pinning (libnuma),
sandbox WASM (wasmtime). Cada um é um milestone próprio.

---

## Cobertura da auditoria

| Ficheiro | Extração | Verificação adversarial |
|---|---|---|
| SPEC-INDEX.md | ⚠️ falhou (limite de sessão fable-5) — banner escrito por leitura manual | — |
| SPEC-009-u64.md | ✅ | ⚠️ falhou (limite sessão); extração mantida |
| SPEC-010.md | ✅ | ⚠️ falhou (limite sessão); extração mantida |
| SPEC-011.md | ✅ | ⚠️ falhou (limite sessão); extração mantida |
| SPEC-019-028.md | ✅ | ✅ **refuted_count = 0** (nenhum finding refutado) |
| SPEC-029-035.md | ✅ | ⚠️ falhou (limite sessão); extração mantida |

Legenda de veredicto: **false** = afirma existência/auditoria e o símbolo/ficheiro
não existe ou é outra coisa · **misleading** = parcialmente verdade mas induz em
erro (existe algo relacionado, não o descrito) · afirmações de *design/intenção
futura* não são listadas (são legítimas como RFC).

---

## SPEC-INDEX.md — o "Manifesto" grandioso

Estado: **RFC/brainstorm**, não índice de algo implementado. Declara-se
`CONGELADO / DECLARATIVO E DETERMINÍSTICO` e `CONGELADO E SELADO`, descreve uma
"Data Computation Platform" com dual-compiler, HQL, Arrow Flight, WASM, cost-based
JIT — **nada disto existe no código**. Contradiz a tese fundadora do próprio
projeto (`SPEC.md`): SPEC.md tem como **não-objetivo** "inventar uma linguagem de
consulta nova" e usa **GQL** (`gql.pest`); o manifesto diz "a inteligência vive no
agente, não no banco" e depois enche o banco de compiladores e feedback adaptativo.
*(A extração automática deste ficheiro falhou por limite de sessão; banner escrito
por leitura manual — as contradições acima já tinham sido verificadas na análise
inicial.)*

---

## SPEC-009-u64.md — "CONGELADA / ALINHADA COM O CORE"

Estado real: os **factos numéricos conferem** (EventId é ULID 128-bit;
GraphIndex projeta EventId→u32 denso em ordem de LSN), mas **os dois artefactos
que a spec existe para especificar não existem**.

| Linha | Afirmação | Veredicto | Evidência |
|---|---|---|---|
| 3 | "Status: CONGELADA / ALINHADA COM O CORE" | misleading | `CanonicalKeyCodec` e `DenseEntityMap` = zero ocorrências em `crates/`. "Alinhada com o core" sugere spec verificada contra código. |
| 27 | "A auditoria do código-fonte... revelou..." | misleading | A conclusão (ULID) confere, mas atribui-a a `vm/codec.rs` — que é o codec de frames `VmInstruction` da H-VM — e coloca nesse ficheiro um `CanonicalKeyCodec` inexistente. |
| 50 | `// heraclitus-core/src/vm/codec.rs` `pub struct CanonicalKeyCodec;` | **false** | `vm/codec.rs` existe mas é o codec binário de `VmInstruction` (M20.1). `CanonicalKeyCodec` não existe em ficheiro nenhum. Bloco apresentado como conteúdo real de um ficheiro nunca escrito. |
| 47 | "O método `encode_f64` realiza o colapso canônico de NaN..." | **false** | Não existe `encode_f64`/`encode_i64`/`SIGN_BIT_MASK`. O único encoder f64→u64 real é `f64_ordered` em `heraclitus-index-attr/src/lib.rs:52` — que **não** trata NaN nem -0.0. |
| 123 | `// heraclitus-index-graph/src/dense_map.rs` | **false** | O ficheiro não existe (a pasta tem lib/adaptive/decision/entity/temporal). `DenseEntityMap`/`FrozenDenseEntityMap` = zero hits. Linha 128 declara `EventId = [u8;16]`, contradizendo o real `EventId(pub ulid::Ulid)`. |
| 30 | "GraphIndex projeta... u32 denso em ordem de LSN" | ✅ true | Confere: `index-graph/src/lib.rs:26-28` + `apply()`. |

---

## SPEC-010.md — design puro (sem selos), mas caracteriza mal o código atual

Estado real: **documento de design** (sem claims de auditoria/CONGELADO/notas).
O problema é dizer que o log atual não tem o que **já tem**, e propor como novo o
que já existe sob outro nome.

| Linha | Afirmação | Veredicto | Evidência |
|---|---|---|---|
| 17 | "...`scan_capped`... **sem metadados estruturais**" | misleading | Falso: o log **já** é segmentado em `.hrkl` com `SegmentHeader`+`SegmentFooter` (record_count, min_lsn, max_lsn, raiz Merkle blake3 — `format.rs:95-100`) + `SegmentMeta`/`SegmentIndex`/`LogCatalog` em memória. |
| 39 | "...tabela indexada em memória... (`SegmentCatalog`)" | misleading | Já existe: `LogCatalog {sealed, active}` + `SegmentIndex` (`log/lib.rs:62-76`). Os símbolos `SegmentCatalog`/`SegmentState`/`SegmentMetadata` do spec não existem; o análogo real é `SegmentMeta` (sem timestamps/compression_type). |
| 232 | "raiz Merkle... salva no seu rodapé" | ✅ true | Já implementado: `SegmentFooter.blake3_root` escrito ao selar (`log/lib.rs:1464`). Mas "Fase 3 (Freeze)" não existe como fase nomeada — o análogo é o *sealing*. |
| 233 | "durante reconstrução... recomputa a assinatura... aborta por divergência" | misleading | O replay real (`views/lib.rs:140-190`) **não** recomputa Merkle nem aborta. Há CRC-32 por registo no decode + verificação Merkle só via `verify_segment` (CLI `check`), não durante replay. |
| 245 | "O operador `WHY` deixa de ser... busca bidirecional simplificada" | misleading | `WHY` existe mas é BFS **unidirecional** de ancestrais (`trace_causes`, `backend.rs:1476`), não bidirecional. A "Provenance Engine" de 1ª classe não existe. |
| 212 | "`heraclitus-analytics` intercepta query complexa... quatro componentes" | misleading | `analytics` é um wrapper SQL DataFusion de ~170 linhas sobre a tabela `events`; não intercepta grafo. Statistics/Cardinality/CostModel/PhysicalPlanner/`GraphOperator` = zero hits. |

Genuinamente confere (estado atual bem descrito): materialização Arrow sem poda
(`analytics/lib.rs:65`), replay single-thread (`views/lib.rs:158`), `GraphIndex`
existe.

---

## SPEC-011.md — "Matriz de Maturidade" com cinco notas 10.0 auto-atribuídas

Estado real: **design puro**. Nenhum componente especificado existe; o único nome
coincidente (`StorageEngine`) é uma **variante de erro**, não uma trait.

| Linha | Afirmação | Veredicto | Evidência |
|---|---|---|---|
| 181 | "Abstração de Armazenamento — **10.0** — a trait `StorageEngine`..." | **false** | Não existe trait `StorageEngine`. Só `HeraclitusError::StorageEngine(String)` em `core/src/error.rs:11`. `append_raw`/`fetch_segment`/`write_manifest` + `DatabaseManifest` = zero hits. |
| 182 | "Consistência de Visão — **10.0** — `TransactionSnapshot`..." | misleading | `TransactionSnapshot` não existe. Real: `pub struct Snapshot(Lsn)` (`txn/lib.rs:16`) — newtype de 1 LSN, sem `watermark_lsn`/`visible_segments`. |
| 183 | "Agnosticismo de Artefatos — **10.0** — `DerivedExecutionArtifact`..." | **false** | `DerivedExecutionArtifact`/`ArtifactManager`/`ArtifactType`/`QueryFingerprint` = zero hits. Índices reais não partilham trait de ciclo de vida. |
| 184 | "Proteção de Hardware — **10.0** — `Memory Manager`+`ResourceScheduler`..." | **false** | `MemoryManager`/`ResourceScheduler`/`SystemResources`/`GraphOperator`/zonas Hot-Warm-Cold = zero hits. O único "cold" é `ColdTier` (tiering p/ object storage), não gestão de RAM. |
| 185 | "Determinismo Lógico — **10.0**" | misleading | Nota a uma garantia sem mecanismo: não há PhysicalPlanner/GraphOperator/múltiplas estratégias entre as quais exigir/testar determinismo. |
| 187 | "...atinge maturidade máxima... chancelado e pronto para a codificação" | misleading | "Chancelado" autodeclarado; nada existe em código. Atenuante: "pronto para a codificação" admite que o código não existe. |

---

## SPEC-019-028.md — verificação adversarial completa: **0 refutados**

Design puro com 3 claims concretos de código, **todos confirmados problemáticos**
(o verificador adversarial tentou refutar e não conseguiu).

| Linha | Afirmação | Veredicto | Evidência |
|---|---|---|---|
| 72 | "consenso de replicação... implementado via Raft no crate `heraclitus-raft`" | misleading | `raft/lib.rs` (153 l.) nega no header: "v0 (RFC-003): single-leader log shipping... we do NOT claim automatic failover". Só `Follower::sync_once` + `LogTransport`. `openraft` nem é dependência. |
| 251 | `EventKind { ... SystemMetric }` em `core/src/event.rs` | **false** | Enum real: `{Observation, Action, Message, RetrievalFeedback, FactDerived, DemotionReceipt, Custom}` — sem `SystemMetric`. Zero telemetria endógena. Shape inventado atribuído a ficheiro real. |
| 263 | "...usa `heraclitus-analytics` e a sintaxe SQL/HQL para investigar a si mesmo" | misleading | `analytics` é SQL DataFusion real, mas "HQL" = zero hits (é GQL). A query exemplo (`WHERE kind='SystemMetric'`, colunas `freeze_duration_ms`) nunca devolveria nada — colunas/eventos inexistentes. |
| 350 | "...perfeitamente amarradas e consolidadas. Deixa de ser um desenho teórico" | misleading | Zero hits para todos os componentes das SPEC-019–028 (também sob nomes alternativos). Continua 100% desenho teórico. |

---

## SPEC-029-035.md — output de chat de LLM colado como spec

Estado real: abre com **bajulação ao interlocutor** ("O seu parecer é definitivo..."),
admite ser pré-código (linha 3), e fecha com decreto auto-emitido
**"CONGELADO, CHANCELADO E APROVADO PARA IMPLEMENTAÇÃO IMEDIATA"**. Nenhum
componente nomeado existe.

| Linha | Afirmação | Veredicto | Evidência |
|---|---|---|---|
| 1 | "O seu parecer é definitivo e eleva o projeto ao nível mais alto..." | misleading (bajulação) | Abertura de resposta de LLM; não há "parecer" no repo. |
| 15 | "`DatabaseManifest`... `StorageFormatVersion` (major/minor/feature_flags)" | **false** | Zero hits. Formato real: magic "HRKL" + `format_version: u16` (FORMAT_VERSION=5), sem tripla nem bitmask. |
| 63 | "`ArtifactRegistry` rastreia a árvore de derivação física" | **false** | `ArtifactRegistry`/`ArtifactDependencyNode` = zero hits. Sem DAG de artefactos. |
| 76 | "`MemoryManager` expurga artefato da RAM..." | **false** | `MemoryManager` = zero hits. Sem evicção em cascata. |
| 119 | "`StatisticsCatalog` consome feedbacks... média móvel exponencial" | **false** | `StatisticsCatalog`/`ExecutionFeedback`/`CostModel` = zero hits. Sem malha adaptativa de custo. |
| 133 | "`ResourceScheduler` obriga core pinning das threads do GraphBLAS" | **false** | `ResourceScheduler`/GraphBLAS = zero hits. NUMA só como comentário em `mmap.rs`. Sem core pinning. |
| 179 | "Todos os plugins... executados dentro de um runtime WASM embarcado" | **false** | wasmtime/extism/wasm/`ExtensionCapabilities`/plugin/sandbox = zero hits. Sem sistema de plugins. |
| 204-206 | "Gate 1/2/3... Blindagem de Identidade / EBR / Sandbox WASM" | **false** | `StableId`/EBR/crossbeam-epoch/sandbox WASM = zero hits. "Formalizado" só neste texto. |
| 208 | "CONGELADO, CHANCELADO E APROVADO PARA IMPLEMENTAÇÃO IMEDIATA" | **false** | Decreto auto-emitido; o código real divergiu por completo destas specs. |

---

## Ação recomendada (Fase 0 do PLANO-SPECS.md)

1. **Rebaixar** toda a pasta `SPEC-new/` de "SPEC congelada" para **RFC/proposta**
   (banners aplicados no topo de cada ficheiro em 2026-07-08/09).
2. **Não citar** estes documentos como estado de implementação.
3. **Extrair** as ~5 ideias boas e compatíveis (segment footers com zone maps/bloom,
   delta-of-delta, format versioning completo, EBR, merge determinístico) como RFCs
   pequenos — ver Fases 2–3 do [PLANO-SPECS.md](../PLANO-SPECS.md).
4. **Rejeitar** HQL (SPEC-023): manter GQL, como o código já decidiu.
