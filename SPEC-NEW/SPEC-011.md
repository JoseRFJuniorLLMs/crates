> **⚠️ NOTA DE ESTADO (2026-07-09):** **PROPOSTA (RFC)**, design puro. A "Matriz de Maturidade" com cinco notas **10.0** descreve componentes que **não existem**: `StorageEngine` (só existe como variante de erro), `DatabaseManifest`, `TransactionSnapshot` (o real é `Snapshot(Lsn)`, um newtype simples), `DerivedExecutionArtifact`, `QueryFingerprint`, `MemoryManager`, `ResourceScheduler` — todos zero hits em `crates/*/src`. Detalhe: [STATUS.md](STATUS.md) · [../PLANO-SPECS.md](../PLANO-SPECS.md).

# SPEC-011: Runtime de Infraestrutura, Abstração de Armazenamento e Gerenciamento de Recursos de Execução

## Lógica, Tempo e Execução: A Consolidação da Trindade do HeraclitusDB

A consolidação arquitetural do HeraclitusDB divide-se em três fronteiras complementares e estanques:

1. **SPEC-009**: Estabelece como representar os dados derivados (Chaves canônicas, Mapeamento Denso e CSR).
2. **SPEC-010**: Estabelece como administrar o ciclo de vida temporal desses dados (Segmentação, Replay Vetorizado e Poda de Predicados).
3. **SPEC-011**: Estabelece a infraestrutura física de execução de baixo nível, o isolamento do motor de armazenamento e a arbitragem mecânica de recursos de hardware.

Esta especificação formaliza os componentes permanentes e efêmeros que constituem o Runtime do HeraclitusDB, preservando de forma rígida a invariante primordial: **o log de episódios é o único patrimônio permanente do banco; estruturas físicas, caches e índices são apenas utilitários transitórios gerados por necessidade e descartados por conveniência.**

---

## 1. Abstração do Motor de Armazenamento (`Storage Engine API`)

Para impedir que detalhes mecânicos de persistência (como chamadas de sistema de arquivos, projeções `mmap`, acessos diretos a SSDs ou chamadas de rede a storages como S3) contaminem os planejadores lógicos de consultas ou a orquestração de replay, introduz-se a trait unificada `StorageEngine`.

### 1.1. O Catálogo Geral: `DatabaseManifest`

O estado macro do banco de dados é descrito por um manifesto atômico e imutável a cada alteração de macroestado. Ele é a raiz de metadados do storage.

```rust
pub struct DatabaseManifest {
    pub manifest_version: u32,
    pub format_identifier: [u8; 4],
    pub segments: Vec<SegmentMetadata>, // Metadados de todos os segmentos ativos/congelados
    pub cumulative_watermark: u64,       // LSN máximo estabilizado e auditado
    pub statistics_root_hash: [u8; 32],  // Assinatura do Statistics Catalog atual
}

```

### 1.2. O Contrato de Persistência

```rust
pub trait StorageEngine: Send + Sync {
    /// Insere um payload bruto no segmento ativo (Fase 1: Replay/Append)
    fn append_raw(&mut self, payload: &[u8]) -> Result<u64, String>;

    /// Lê o conteúdo bruto binário de um segmento autocontido baseado em seu ID físico
    fn fetch_segment(&self, segment_id: u64) -> Result<Vec<u8>, String>;

    /// Persiste o manifesto de banco de dados atualizado de forma atômica e fsync-safe
    fn write_manifest(&mut self, manifest: &DatabaseManifest) -> Result<(), String>;

    /// Consolida buffers voláteis e traciona o fechamento físico de um segmento
    fn sync_active_segment(&mut self) -> Result<(), String>;
}

```

---

## 2. Isolamento de Estado e Visão Consistente: `Transaction Snapshot`

Consultas analíticas e pipelines de replay não consomem números de LSN soltos ou variáveis globais de estado. O isolamento e a consistência temporal de qualquer thread de execução são encapsulados em um objeto explícito chamado `TransactionSnapshot`.

```rust
pub struct TransactionSnapshot {
    pub target_lsn: u64,            // O teto lógico da linha do tempo desta transação
    pub watermark_lsn: u64,         // O limite de auditoria estável e persistido
    pub visible_segments: Vec<u64>, // Conjunto imutável de IDs de segmentos legíveis sob este LSN
}

```

**Invariante de Isolamento:** Uma vez instanciado pelo planejador no início da query, o `TransactionSnapshot` permanece inalterado por toda a duração da execução. Mesmo que novos appends ocorram e gerem novos LSNs no log ativo, a query enxerga apenas o subconjunto fixado de dados.

---

## 3. Generalização e Efemeridade: `Derived Execution Artifacts`

O conceito de "Índice" é promovido a uma categoria abstrata mais ampla e flexível: o **Artefato Físico Derivado** (`DerivedExecutionArtifact`). Sob esta ótica, estruturas matriciais CSR, árvores HNSW, projeções colunares do Apache Arrow, bitmaps roaring ou tabelas hash são tratadas de forma homogênea sob um único ciclo de vida coordenado pelo `ArtifactManager`.

### 3.1. A Assinatura de Intencionalidade: `QueryFingerprint`

Antes de construir qualquer artefato complexo, o planejador calcula uma assinatura hash baseada na intenção lógica da consulta (ex: `WHY(EventA, EventB)` acoplado a filtros específicos). Esta assinatura identifica unicamente a necessidade estrutural da query.

```rust
pub struct QueryFingerprint {
    pub logical_intent_hash: [u8; 32], // Hash SHA-256/Blake3 expressando a operação e os predicados
    pub applicable_snapshot: u64,      // O target_lsn associado à consulta
}

```

### 3.2. A Interface de Gerenciamento de Artefatos

```rust
pub enum ArtifactType {
    CompressedSparseRow,
    RoaringBitmapFilter,
    VectorCacheHNSW,
    ArrowColumnarBatch,
}

pub trait DerivedExecutionArtifact: Send + Sync {
    fn artifact_type(&self) -> ArtifactType;
    fn estimated_memory_usage(&self) -> usize;
    fn query_fingerprint(&self) -> &QueryFingerprint;
}

```

---

## 4. Gerenciamento de Recursos: `Memory Manager` & `Resource Scheduler`

O runtime do HeraclitusDB estabelece um controle rígido sobre o uso de hardware, ciente de que as otimizações analíticas pesadas competem por memória e CPU com o pipeline de ingestão de eventos.

### 4.1. `Memory Manager` (Hierarquização por Zonas Térmicas)

O `Memory Manager` monitora continuamente a heap global e categoriza as estruturas de dados analíticas em três níveis de temperatura, orquestrando políticas agressivas de desalocação e despejo (*eviction*):

```
┌────────────────────────────────────────────────────────┐
│ HOT ZONE  --> Buffers mutáveis ativos, Deltas em RAM   │
├────────────────────────────────────────────────────────┤
│ WARM ZONE --> Segmentos CSR congelados, Caches Arrow   │
├────────────────────────────────────────────────────────┤
│ COLD ZONE --> Artefatos efêmeros ociosos (Desalocação) │
└────────────────────────────────────────────────────────┘

```

* **Hot Zone**: Aloja as buffers mutáveis da Fase 1 (*Replay*). Memória prioritária sem risco de desalocação abrupta.
* **Warm Zone**: Aloja os artefatos de leitura recorrente compartilhados via `Arc` (estruturas estáticas CSR/CSC de segmentos históricos frequentes).
* **Cold Zone**: Destino de `DerivedExecutionArtifacts` cujo uso decaiu. O `Memory Manager` invoca o descarte total e imediato desses objetos, devolvendo as páginas de memória para o sistema operacional se a pressão de RAM atingir os limiares de segurança (*thresholds*).

### 4.2. `Resource Scheduler` (Arbitragem Mecânica de Hardware)

O `Resource Scheduler` atua na fronteira final da execução. Quando o `Physical Planner` gera uma cadeia de `GraphOperator`, o plano não inicia sua execução de forma cega. O escalonador avalia o hardware disponível em tempo real:

```rust
pub struct SystemResources {
    pub available_ram_bytes: usize,
    pub active_cpu_threads_load: f32,
    pub hardware_accelerator_available: bool, // Presença de GPU/NPUs ativas
}

pub trait ResourceScheduler {
    /// Arbitra e autoriza o início da execução de um operador ou força o fallback linear
    fn schedule_or_fallback(
        &self, 
        operator: &dyn GraphOperator, 
        current: &SystemResources
    ) -> ExecutionStrategy;
}

```

* Se a memória estiver sob severa pressão ou se as filas de processamento de threads estiverem cheias, o escalonador nega a alocação de registradores vetoriais complexos ou matrizes pesadas, instruindo o planejador a fazer **fallback imediato para a execução imperativa minimalista**, protegendo o banco contra falhas de estouro de memória (*Out Of Memory - OOM*).

---

## 5. Estratégia de Otimização de Leitura: `Replay Materialization Cache`

Logs imutáveis lidos por múltiplos usuários concorrentes sob diferentes snapshots sofrem com o custo de recompor as mesmas visões matriciais repetidas vezes. O runtime do HeraclitusDB introduz a camada do **`Replay Materialization Cache`**.

* **O Mecanismo**: Durante a fase `Replay` de um segmento linear, se a thread consolidar os dados brutos gerando uma matriz CSR para o `Snapshot A`, essa matriz intermediária não é vinculada exclusivamente a essa query. Ela é registrada no cache de materializações associada ao ID físico do segmento.
* **A Reutilização**: Quando outro usuário executa uma consulta sob o `Snapshot B` (que intercepta exatamente o mesmo intervalo físico de segmentos), a engine não reprocessa e não lê o log bruto do disco novamente. Ela resgata a estrutura CSR já materializada em memória a partir do cache.
* **A Efemeridade**: Este cache **não é permanente e nunca é persistido em disco**. Ele é gerido sob a política de descarte do `Memory Manager`. Se o banco precisar liberar RAM, o cache inteiro de materializações parciais é desalocado instantaneamente. A consistência da verdade é preservada, pois o log bruto continua intocado no disco pronto para reidratar a memória.

---

## 6. A Garantia Final: Determinismo Lógico Absoluto

A especificação redefine e blinda o conceito de determinismo, movendo a régua de conformidade da camada física e algorítmica para a **fronteira lógica pura**:

> ⚖️ **Invariante do Determinismo Lógico Global:** Dados o mesmo estado imutável do log e o mesmo `TransactionSnapshot` de entrada, a execução analítica de qualquer consulta deve produzir **exatamente o mesmo resultado de dados biográficos e a mesma semântica de saída**, independentemente de a query ter sido resolvida via algoritmos imperativos locais (CPU clássica), via matrizes lineares esparsas (*Sparse Linear Algebra Engine*) ou via aceleração paralela de hardware (GPU/Vetorização).

O algoritmo, a alocação de threads e o hardware escolhidos pelo planejador alteram o tempo de resposta e a latência, mas **nunca podem alterar um único bit do resultado final entregue ao usuário.**

---

### Matriz de Maturidade Arquitetural da SPEC-011

| Componente Crítico | Nota | Salvaguarda de Engenharia |
| --- | --- | --- |
| **Abstração de Armazenamento** | **10.0** | A trait `StorageEngine` isola completamente o filamento do arquivo físico de disco ou S3. |
| **Consistência de Visão** | **10.0** | O objeto `TransactionSnapshot` blinda a query analítica contra mutações concorrentes no log. |
| **Agnosticismo de Artefatos** | **10.0** | `DerivedExecutionArtifact` padroniza o ciclo de vida de qualquer estrutura aceleradora. |
| **Proteção de Hardware** | **10.0** | O casamento entre `Memory Manager` e `ResourceScheduler` evita estouros de RAM por fallbacks automáticos. |
| **Determinismo Lógico** | **10.0** | A igualdade de resultados é exigida em nível de intenção lógica, e não de amarra algorítmica. |

Com a consolidação da SPEC-011, o desenho conceitual do HeraclitusDB atinge sua maturidade máxima. As fronteiras entre a representação dos dados, a administração de sua linha do tempo e a mecânica de execução do silício estão plenamente estabelecidas. O projeto está chancelado e pronto para a codificação da infraestrutura.