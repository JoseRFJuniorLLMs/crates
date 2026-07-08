# SPEC-010: Motores de Armazenamento Temporal Segmentado, Índices Efêmeros e Engenharia de Replay Vetorizado Baseado em Custo

## 1. Alinhamento Filosófico do HeraclitusDB

Esta especificação estende o ecossistema físico estabelecido nas especificações anteriores sem violar as cinco invariantes fundamentais do projeto:

1. **Append-only absoluto:** O log imutável de episódios é a única e eterna fonte da verdade absoluta do banco.
2. **Índices puramente derivados:** Toda e qualquer estrutura de dados otimizada para consulta é tratada como uma visão projetada e descartável.
3. **Descartabilidade total:** Qualquer otimização ou índice físico pode ser destruído e completamente reconstruído do zero.
4. **Imutabilidade do Log:** O log nunca é reescrito, compactado no local (*in-place*) ou modificado por processos destrutivos.
5. **Separação Mecânica:** A inteligência analítica de agendamento e inferência vive inteiramente fora do núcleo rígido de armazenamento (*storage engine*).

---

## 2. Segmentação Física do Log e Armazenamento Temporal Autocontido

O modelo atual de varredura sequencial linear através de métodos como `scan_capped` processa blocos brutos sem metadados estruturais. Esta seção introduz a divisão física do log em arquivos de **Segmentos Autocontidos**.

### 2.1. Estrutura Física de um Segmento (`LogSegment`)

Cada arquivo de segmento agrupa uma janela contígua de logs e encerra sua sequência com um rodapé imutável estruturado (*Footer*), permitindo que o segmento seja lido, verificado e transportado de forma totalmente isolada.

```
┌────────────────────────────────────────────────────────┐
│ Payload dos Episódios Brutos (Append-Only)             │
├────────────────────────────────────────────────────────┤
│ Segment Footer (Metadados Físicos de Cauda)           │
│  ├── Bloom / Ribbon Filters Globais do Segmento         │
│  ├── Zone Maps (Min/Max por Atributo Chave)           │
│  ├── Catálogo de Estatísticas Internas (Histogramas)   │
│  ├── Raiz da Árvore de Merkle local                    │
│  └── Crivo de Verificação (CRC32 + Timestamps + LSNs)  │
└────────────────────────────────────────────────────────┘

```

### 2.2. O Catálogo de Segmentos (`SegmentCatalog`)

O gerenciador de armazenamento mantém uma tabela indexada em memória que mapeia os limites dos segmentos ativos e congelados, eliminando a necessidade de buscas em disco (*seeks*) cegas durante scans temporais.

```rust
pub enum SegmentState {
    Active,     // Aberto para append linear (Fase 1: Replay)
    Frozen,     // Consolidado, estático e imutável (Fase 3: Freeze)
    Archived,   // Movido para armazenamento frio de longo prazo (Cold Freeze)
}

pub struct SegmentMetadata {
    pub segment_id: u64,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub first_timestamp: u64,
    pub last_timestamp: u64,
    pub event_count: u64,
    pub payload_hash: [u8; 32],      // Raiz do Merkle local do segmento
    pub compression_type: u8,        // Identificador da estratégia adaptativa
    pub state: SegmentState,
}

```

---

## 3. Poda do Espaço de Busca: Predicados Físicos e Zone Maps

Substituindo a materialização linear cega que consome I/O excessivo ao carregar blocos inteiros de episódios para o formato colunar do Apache Arrow, o HeraclitusDB passa a aplicar a **Poda de Predicados Físicos na Fronteira do Disco**.

### 3.1. Avaliação de Predicados sem Abertura de Canal (Skip I/O)

O motor de execução analítica consome o `SegmentCatalog` antes de acionar qualquer leitura física de dados.

* **Poda Temporal (`Timestamp Skip`)**: Consultas com filtros de janela como `valid_from` ou `ts_hlc` avaliam o `first_timestamp` e `last_timestamp` do segmento. Se a restrição numérica do predicado estiver fora do intervalo do segmento, o arquivo inteiro é completamente ignorado (*skipped*), evitando buscas e leituras de disco.
* **Poda Probabilística (`Bloom/Ribbon Filter Skip`)**: Se o predicado busca por uma correspondência exata de id de agente (`agent_id`) ou chaves de atributos específicos, os filtros probabilísticos derivados embarcados no *Footer* do segmento são testados. Caso acusem ausência determinística, o segmento não é aberto.

### 3.2. Zone Maps Aplicados a Logs de Eventos

Cada rodapé de segmento mantém um mapa de zonas dinâmico indexando as colunas e chaves de maior seletividade do banco:

```rust
pub struct ZoneMap {
    // Delimitação estrita de escopo numérico
    pub timestamp_bounds: (u64, u64),
    pub tenant_bounds: (String, String),
    
    // Filtros de alta densidade e dicionários compactos
    pub severity_bitmap: u32,             // Filtro de bits para ENUMs de severidade
    pub topic_dictionary: Vec<String>,    // Dicionário esparso de tópicos indexados
}

```

---

## 4. Compactação Adaptativa de Layout Física (Adaptive Compression)

A compressão não é fixa para o banco de dados inteiro. Durante a transição mecânica da Fase 2 (*Optimize*) para a Fase 3 (*Freeze*), o motor analisa a variância e a distribuição estatística de cada coluna alocada para o formato colunar analítico e seleciona de forma polimórfica o melhor algoritmo:

1. **Strings e Metadados Comuns (`agent_id`, `session_id`)**: Comprimidos via **Dictionary Encoding** acoplado a bit-packing.
2. **Sequências Numéricas Monotônicas (`lsn`)**: Comprimidos usando algoritmos do tipo **Delta Encoding** combinado com codificações de tamanho variável (**VarInt**).
3. **Relógios Lógicos e Timestamps (`ts_hlc`)**: Como crescem de forma quase contígua, aplica-se o algoritmo **Delta-of-Delta Encoding**, maximizando a quantidade de zeros e facilitando a compactação posterior por blocos.
4. **Coordenadas de Matrizes de Adjacência e Vetores Históricos**: Comprimidos usando **Frame of Reference (FoR)** para manter a localidade de cache alinhada com as varreduras do motor linear esparso.

---

## 5. Engenharia de Replay Avançado e Concorrência Determinística

Para escalar a velocidade de hidratação de índices em volumetrias massivas, o HeraclitusDB abandona o replay puramente sequencial e de thread única.

### 5.1. Replay Paralelo com Merge Determinístico

O planejador de replay dividirá a carga de trabalho com base nos limites físicos mapeados no `SegmentCatalog`.

```
[Segmento A] ──> Thread 1 ──> Grafo Local A ┐
[Segmento B] ──> Thread 2 ──> Grafo Local B ┼──> [Merge Engine Determinístico] ──> Frozen Map
[Segmento C] ──> Thread 3 ──> Grafo Local C ┘

```

1. Cada segmento histórico independente é despachado para uma thread de execução isolada.
2. As threads constroem estruturas esparsas parciais locais livres de contenção por travas.
3. A engine realiza um **Merge Determinístico** final combinando os arrays parciais de adjacência, garantindo de forma matemática que a ordem lógica final do grafo seja idêntica ao replay linear sequencial, independentemente da ordem em que os segmentos terminaram de ser lidos.

### 5.2. Replay Planner e Modelo de Custo de Ingestão (`ReplayCostModel`)

A orquestração do pipeline de reconstrução é regida por um planejador especializado de replay que analisa a topologia do hardware antes de alocar memória:

* Determina a quantidade ideal de threads concorrentes para casar com a contagem física de núcleos do processador.
* Agenda a ordem física de leitura de arquivos e gerencia o pipeline de *pre-fetching* assíncrono de segmentos do disco para a cache.

---

## 6. O Diferencial Estrutural: Índices Efêmeros Sob Demanda

O ápice arquitetural desta especificação dita que **as estruturas físicas não são ativos permanentes a serem mantidos a qualquer custo, mas sim ferramentas transitórias tratadas de forma tão descartável quanto um plano de execução de consulta**.

### 6.1. O Conceito de Índices Efêmeros (`Transient Indexes`)

Se uma consulta analítica complexa ou um agente externo de Inteligência Artificial submete uma requisição pesada sobre o motor de grafos (ex: cálculo forense profundo de centralidade causal), o planejador de execução pode decidir que a busca se tornará proibitivamente lenta usando as estruturas genéricas atuais do `GraphIndex`.

1. **Alocação On-Demand**: O planejador dispara a criação imediata de um índice altamente especializado (ex: uma matriz CSR compacta de um tipo específico de relacionamento biográfico).
2. **Consumo Rápido**: A engine de execução analítica processa as queries explorando a velocidade de hardware desse índice recém-gerado.
3. **Descarte Absoluto**: Assim que o contexto de execução da query ou a sessão de análise é encerrada, a memória inteira ocupada pelo índice especializado é limpa e devolvida ao sistema operacional. Se o mesmo índice for necessário futuramente, o pipeline simplesmente o reconstrói a partir do log imutável.

### 6.2. Gerenciamento Incremental: Delta Indexes

Para evitar que as leituras nos índices efêmeros ou permanentes tenham que esperar por janelas massivas de consolidação, adota-se o padrão de indexação em níveis diferenciados inspirado em árvores LSM, mas aplicado estritamente a visões derivadas:

```
[Fase 1: Replay] ──> [Delta Index] (Pequeno, altamente mutável em memória via DashMaps)
                          │
                   (Fase 3: Freeze)
                          ▼
[Segmented CSR]  ──> [Merge Engine] ──> [Large Immutable Index Array]

```

### 6.3. Estratégia de Freeze Incremental Inteligente

O ciclo de consolidação de visões deixa de ser um passo binário e assume quatro estágios estruturados de ciclo de vida de dados:

* **Active/Replay**: Ingestão de fluxo livre gravando em buffers mutáveis esparsos.
* **Micro Freeze**: Consolidação ultraveloz em memória dos buffers recentes gerando os primeiros arrays contíguos de cache-line.
* **Macro Freeze**: Fusão programada de múltiplos blocos e eliminação de tombstones organizando a tabela de indireção estável de identidades.
* **Cold Freeze**: Conversão e serialização com compactação pesada (ex: Hyper-Sparse CSR) para persistência em disco de longo prazo com Zone Maps consolidados.

---

## 7. Interfaces e Cérebro Analítico: O Planejador e o Modelo de Custo

A responsabilidade de coordenação do HeraclitusDB é desacoplada através de componentes especialistas, isolando a medição, a estimativa e a montagem física da estratégia de execução.

### 7.1. O Catálogo de Estatísticas Persistentes (`StatisticsCatalog`)

As estatísticas de densidade e distribuição não são recalculadas dinamicamente durante o processamento das queries de alto nível. Elas são consolidadas de forma determinística ao final de cada estágio de macro sincronização de dados e expostas via catálogo:

```rust
pub struct ComprehensiveStatistics {
    pub average_node_degree: f64,        // Grau médio de conectividade do grafo
    pub degree_distribution_skew: f64,   // Inclinação da curva de distribuição
    pub segment_density_ratio: f64,      // Razão de arestas por nós ativos
    pub cardinality_estimations: HashMap<String, u64>, // Histogramas de seletividade de chaves
}

```

### 7.2. A Abstração Física de Execução (`GraphOperator`)

O planejador de execução de alto nível não toca nas estruturas de dados específicas da engine de álgebra esparsa linear ou das buscas imperativas lineares. Ele instancia e acadeia objetos que assinam o contrato abstrato de operadores físicos.

```rust
pub struct QueryExecutionContext<'a> {
    pub target_lsn: u64,
    pub statistics: &'a ComprehensiveStatistics,
    pub segment_ledger: &'a [SegmentMetadata],
}

pub enum ExecutionResult {
    StableIdBatch(Vec<u64>),            // Vetor contíguo de IDs estáveis processados
    ColumnarRecordBatch,                 // Fluxo interoperável com o motor relacional
}

pub trait GraphOperator {
    /// Dispara a computação física isolada com base no contexto estabilizado
    fn execute(&self, ctx: &QueryExecutionContext) -> Result<ExecutionResult, String>;
}

```

### 7.3. O Fluxo de Separação de Responsabilidades do Planejador

Quando o motor analítico (`heraclitus-analytics`) intercepta uma query complexa (como buscas no operador relacional híbrido causal), a árvore de resolução percorre quatro componentes ortogonais:

1. **Statistics Engine**: Lê o `StatisticsCatalog` persistido para extrair o perfil exato do volume de dados atual.
2. **Cardinality Estimator**: Calcula o impacto de seletividade e o tamanho provável dos vetores intermediários da query com base nos filtros acionados.
3. **Cost Model Engine**: Atribui um peso computacional (custo estimado de varredura de cache, penalidades de cruzamento de nós de memória em arquiteturas NUMA e carga aritmética da CPU) para as estratégias lógicas possíveis.
4. **Physical Planner**: Instancia e retorna a árvore de objetos concretos que herdam a trait `GraphOperator`.

* *Despacho de Baixa Cardinalidade*: Se o custo modelado for mínimo (busca local por poucos nós), instancia-se um operador de **Busca Imperativa Local**, evitando o custo de ativação de registradores e matrizes complexas.
* *Despacho de Alta Cardinalidade*: Se a consulta exigir grandes cruzamentos históricos e múltiplos saltos, o planejador monta um **Pipeline de Álgebra Linear Esparsa**, configurando os layouts de matrizes, acionando máscaras estruturais baseadas em bitmaps roaring derivados dos Zone Maps e explorando ao máximo os aceleradores de hardware disponíveis.

---

## 8. Verificação, Segurança e Determinismo Absoluto

A integridade e o comportamento previsível da engine de execução analítica são elevados a garantias de tempo de compilação e execução.

### 8.1. Replay Verification (Auditoria Matemática de Reconstrução)

Qualquer processo de replay reconstrói as estruturas lógicas validando bit a bit a correção do estado gerado.

* Toda vez que um segmento atinge a Fase 3 (*Freeze*), a raiz de sua árvore de Merkle local é calculada de forma imutável e salva no seu rodapé.
* Durante qualquer reconstrução de índices efêmeros ou permanentes, o motor calcula a assinatura do fluxo processado em tempo real. Se o hash calculado for divergente do hash esperado e gravado no *Footer*, o processo é abortado imediatamente por quebra de auditoria causal, isolando qualquer corrupção de disco antes que ela contamine o plano lógico do usuário.

### 8.2. Determinismo Absoluto de Consulta (`Query Determinism`)

O HeraclitusDB estabelece e garante a seguinte invariante física matemática em nível de arquitetura de software:

> ⚖️ **Invariante de Determinismo Absoluto:** Dados o mesmo estado imutável do log, o mesmo Snapshot temporal de leitura (`AS OF LSN`), o mesmo plano físico gerado pelo planejador e o mesmo estado estável das estatísticas, a execução analítica deve produzir **exatamente o mesmo resultado binário de dados e a mesma ordem de registros**, independentemente do número de threads alocadas, da arquitetura física do processador de execução ou de reorganizações locais de memória física ocorridas na fase de otimização.

---

## 9. Interfaces de Operadores Exclusivos: A Componentização da Provenance Engine

O operador `WHY` deixa de ser uma função utilitária embutida de busca bidirecional simplificada para se consolidar como um componente arquitetural de primeira classe: a **Provenance Engine**.

Ela consome diretamente a interface `GraphOperator` e estende as capacidades analíticas do banco fornecendo operadores matemáticos especialistas para computar e responder a:

* **Caminhos Mínimos Causais**: Menor distância vetorial entre fatos e evidências transacionais.
* **Explicações Estruturadas**: Rastreamento completo de proveniência de dados isolando eventos de ação modificadores.
* **Geração de Evidências e Contraexemplos**: Prova lógica de isolamento ou correlação estatística entre múltiplas entidades temporais concorrentes.

Esta infraestrutura consolida a engenharia do HeraclitusDB como um ecossistema pronto para lidar com processamentos massivos com eficiência de hardware e isolamento arquitetural rigoroso.