# 📑 SPEC-INDEX: Índice Mestre da Plataforma Computacional HeraclitusDB

**Status:** CONGELADO / DECLARATIVO E DETERMINÍSTICO

**Slogan Arquitetural:** *HeraclitusDB is a deterministic data computation platform built on immutable temporal logs.*

---

## 1. O Novo Manifesto: Da Persistência à Computação de Conhecimento

O HeraclitusDB rejeita a classificação simplista de SGBD tradicional. Ele não foi concebido para responder à pergunta clássica *"Como armazenar dados?"*. Ele assume a identidade de uma **Data Computation Platform** cujo primeiro produto tangível é um motor de banco de dados baseado em tempo imutável.

A plataforma foi desenhada para resolver o seguinte axioma:

> **"Como representar, transformar, explicar e reproduzir conhecimento de maneira determinística ao longo do tempo?"**

Sob esta ótica, o armazenamento físico em disco passa a ser uma mera consequência instrumental. Toda a inteligência da plataforma está organizada em uma pilha computacional rigorosa (*Computation Stack*), onde cada nível possui responsabilidades estanques e isoladas por contratos formais de software.

---

## 2. Heraclitus Computation Stack (A Pilha Computacional)

```
                HERACLITUS COMPUTATION STACK

──────────────────────────────────────────────────────────────
Fase 0 │ Mathematical Foundations (Invariants & H-VM Ledger)
──────────────────────────────────────────────────────────────
Fase 1 │ Physical Memory Model (Dense IDs, Canonical Keys)
──────────────────────────────────────────────────────────────
Fase 2 │ Physical Storage Engine (Segments, Zone Maps, Footers)
──────────────────────────────────────────────────────────────
Fase 3 │ Execution Runtime (Contexts, Atomics, Budgets)
──────────────────────────────────────────────────────────────
Fase 4 │ Cost-Based Compiler (Logical to Physical Operators DAG)
──────────────────────────────────────────────────────────────
Fase 5 │ Vectorized Execution Engine (Arrow Batches & SIMD Kernels)
──────────────────────────────────────────────────────────────
Fase 6 │ Knowledge Engine (WHY / BECAUSE / Provenance Dual JIT)
──────────────────────────────────────────────────────────────
Fase 7 │ Distributed Runtime (Log Shipping, Raft Consensus)
──────────────────────────────────────────────────────────────
Fase 8 │ External Ecosystem (Arrow Flight, WASM Sandboxing, SDKs)
──────────────────────────────────────────────────────────────

```

---

## 3. O Paradoxo dos Dois Compiladores (Dual-Compiler Architecture)

O grande diferencial arquitetural do HeraclitusDB reside na coexistência síncrona e ortogonal de **dois compiladores independentes em nível de runtime**, elevando a plataforma ao estado de arte em engenharia de sistemas explicáveis:

### ⚙️ Compiler 1: Heraclitus Compiler (O Fluxo de Dados)

Responsável por interceptar expressões declarativas escritas em HQL e compilá-las em um grafo de execução vetorizado de alto desempenho baseado em custo, gerando buffers contíguos do Apache Arrow prontos para consumo.

```
HQL ──> AST ──> Árvore Lógica ──> Heraclitus Compiler ──> Plano Físico ──> Grafo de Operadores ──> Runtime Vetorizado ──> Arrow Batch ──> Cliente

```

### 🧠 Compiler 2: Provenance Engine (O Fluxo de Explicação)

Responsável por processar investigações causais profundas (`WHY`, `BECAUSE`). Ele não executa buscas imperativas em tabelas; ele atua como um **segundo compilador especializado**, transformando a intenção de auditoria em uma árvore matemática mínima de dependências colunares esparsas, reproduzindo com precisão bit a bit o motivo de um fato existir ou ter sido gerado.

```
WHY X ──> Plano Causal ──> Subgrafo Esparso ──> CSR Matrix JIT ──> Multiplicação Vetorial ──> Árvore Mínima ──> Explicação Causal Isenta de Churn

```

---

## 4. Fluxo de Execução Unificado da Infraestrutura

Abaixo está mapeada a topologia física e lógica definitiva que orienta o tráfego de dados e metadados no HeraclitusDB:

```
                 HQL (Consulta ou Investigação Causal)
                  │
                  ▼
          Heraclitus Compiler (SPEC-012)
                  │
                  ▼
           Physical Plan DAG (Grafo de Operadores)
                  │
                  ▼
         Heraclitus Runtime (SPEC-013)
                  │
        ┌─────────┴──────────┐
        ▼                    ▼
 Arrow Execution      Provenance DAG (Compiler 2)
 (Lotes de 1024)      (Cadeias e Explicações)
        │                    │
        └─────────┬──────────┘
                  ▼
             Final Result (Entrega Zero-Copy via Flight)

```

---

## 5. Mapeamento Geral de Especificações Técnicas (Módulos Core)

### 📌 Fase 0: Fundações Matemáticas e Invariantes do Sistema

* **Invariantes Globais de Projeto:** Monotonicidade linear estrita de números de sequência (`Lsn`) acoplada a relógios lógicos híbridos (`Hlc`), determinismo absoluto de replay multi-thread independente de hardware e integridade total das instruções immudb-like da H-VM ledger.

### 📌 SPEC-009: Camada de Chaves Canônicas e Mapeamento Denso (Fase 1)

* **Escopo Técnico:** Codec de normalização binária estável para inteiros e ponto flutuante IEEE-754 (`CanonicalKeyCodec`). Compressão volumétrica de localidade espacial de cache line através do colapso de `EventId` (ULID, 16 bytes) para `DenseId` contíguos de 32 bits (`u32`). Pipeline estrutural em três estágios: *Replay*, *Optimize* e *Freeze*.

### 📌 SPEC-010: Motores de Armazenamento Temporal Autocontido (Fase 2)

* **Escopo Técnico:** Segmentação física do log em arquivos de `LogSegment` dotados de rodapés estruturados (*Footer*) contendo Zone Maps de máximos e mínimos e filtros probabilísticos para poda de predicados em disco (*Skip I/O*). **Autossuficiência Estatística:** Injeção direta de dicionários de compressão adaptativa, cardinalidades colunares, histogramas e entropia nativos no rodapé de cauda de cada segmento, servindo de fundação imediata e isenta de varreduras para o planejador baseado em custo.

### 📌 SPEC-011: Runtime de Infraestrutura e Contextos Controlados (Fase 3)

* **Escopo Técnico:** Abstração de persistência através de `StorageEngine` e transições atômicas reguladas pelo `DatabaseManifest`. Garantias estáveis de isolamento temporal concorrente via `TransactionSnapshot` e gerenciamento hierárquico de `DerivedExecutionArtifacts` efêmeros em zonas térmicas.
* **Controle de Sandbox Operacional:** Acoplamento mandatório do `ExecutionContext` encapsulando as primitivas rígidas de `CancellationToken`, `MemoryBudget`, `CPUBudget` e `ExecutionBudget`. Bloqueia loops infinitos e estouros de memória OOM através de fallbacks automáticos do `ResourceScheduler` para buscas imperativas locais econômicas.

### 📌 SPEC-012: Heraclitus Compiler (Fase 4)

* **Escopo Técnico:** O cérebro de otimização baseada em custo da plataforma. Intercepta a AST gerada pelo parser da linguagem HQL e calcula a árvore de planos lógicos abstratos. Utiliza as estatísticas e histogramas colunares autodescritivos extraídos diretamente dos rodapés de segmentos para calcular os pesos algébricos exatos e despachar a query na forma de um Plano Físico otimizado estruturado em um Grafo de Operadores (`GraphOperator`).
* **Adaptive Query Planner:** Heurística de retroalimentação estatística endógena que monitora o desvio de predição do custo estimado vs. tempo de execução real, atualizando coeficientes de média móvel exponencial por impressão digital de consulta (`QueryFingerprint`), permitindo autoajuste dinâmico contínuo sem dependência de redes neurais complexas.

### 📌 SPEC-013: Heraclitus Runtime (Fase 5)

* **Escopo Técnico:** O motor de execução vetorizado colunar encarregado de processar o Grafo de Operadores gerado pelo compilador. Abandona de forma definitiva o modelo iterativo clássico Volcano linha por linha, estabelecendo o processamento homogêneo de **Lotes Vetorizados Fixos de 1024 Linhas** em buffers contíguos do Apache Arrow.
* **Aceleração Mecânica de Hardware:** Loops internos de filtros, projeções e agregações são desenrolados (*loop unrolling*) e executados de maneira totalmente livre de desvios condicionais (*branchless code*), casando o alinhamento de memória física de 64 bytes com os registradores vetoriais da CPU (AVX-512 / AVX10 / ARM Neon) para maximizar as fusões de operadores em nível de silício.

### 📌 SPEC-014: Knowledge & Provenance Engine (Fase 6)

* **Escopo Técnico:** Módulo soberano e independente de explicabilidade e rastreabilidade analítica. Responsável pela execução das lógicas de linhagem de dados através das cláusulas nativas `WHY`, `BECAUSE` e `DEPENDS ON`. Atua como o **Compiler 2**, gerando cascatas de inversão matricial esparsa sobre coordenadas em matrizes CSR compiladas via JIT em memória RAM para produzir árvores mínimas de caminhos causais e explicações biográficas exatas provadas bit a bit contra o log histórico imutável.

### 📌 SPEC-015: Distributed Runtime & Consenso (Fase 7)

* **Escopo Técnico:** Alta disponibilidade distribuída baseada estritamente em **Log Shipping Replicado** (consenso Raft implementado no crate `heraclitus-raft`). Preserva as restrições mecânicas de rede: matrizes CSR sujas, caches e layouts físicos analíticos locais **nunca transitam via rede**. A rede trafega apenas os blocos de bytes puros e sequenciais dos episódios logs; cada nó réplica exerce soberania local absoluta para executar autonomamente sua própria linha de hidratação analítica de acordo com seu hardware nativo.

### 📌 SPEC-016: Ecossistema de Extensibilidade e Integrações Externas (Fase 8)

* **Escopo Técnico:** Protocolo de rede analítico nativo de altíssima performance gerenciado via **Apache Arrow Flight**, entregando fluxos de RecordBatches contíguos de memória direto nas pontas com custo zero de serialização para clientes analíticos modernos (Polars, DuckDB, Spark, e SDKs em Python).
* **Extensibilidade Isolada via Sandboxing:** Mecanismo de carregamento de lógicas de negócios dinâmicas UDF e regras de decisão corporativas (`DECIDE()`) executadas exclusivamente dentro de uma sandbox isolada em **WebAssembly (runtime `wasmtime`)**. Evita pânicos ou quebras de memória no processo principal do banco, trocando dados via passagem estável de buffers contíguos estruturados da ABI.

## 6. Gates de Homologação e Critérios de Conclusão do Workspace

Para chancelar a transição entre milestones de desenvolvimento, cada componente deve superar três barreiras automatizadas de CI/CD:

1. **Gate A (Invariância Aritmética):** Reprodutibilidade bit a bit estável em testes cruzados reproduzidos entre arquiteturas distintas (`x86_64` vs `AArch64`), forçando arredondamentos idênticos e proibindo reduções flutuantes associativas arbitrárias.
2. **Gate B (Resiliência de Queda):** Validação de 50.000 iterações em suítes de injeção de falhas com interrupções abruptas de energia (harness de crash de processos-filho), chancelando a perfeita integridade e truncagem determinística de cauda via marcadores `truncate.intent` antes da readmissão de segmentos.
3. **Gate C (Auditoria Causal Estável):** Prova estatística matemática via testes contínuos demonstrando que a alteração de lógicas físicas JIT e planos baseados em custo alteram a latência e o tempo de resposta do sistema, mas **nunca alteram um único bit do resultado final** e da linhagem causal gerada para o usuário.

O ecossistema arquitetural está **CONGELADO E SELADO**. O HeraclitusDB está pronto para guiar o time sênior na abertura imediata dos Pull Requests das fundações da plataforma computacional.

O HeraclitusDB parece combinar conceitos de várias linhas de pesquisa:

Event Sourcing
Temporal Database
Columnar Analytics
Provenance
Cost-based Optimization
Deterministic Replay
Immutable Ledger
Knowledge Graph

# Relatório de Status de Desenvolvimento: HeraclitusDB

## 1. O que está FEITO (Implementado ou Estruturado)

### A. Estrutura de Módulos e Workspace (Crates)

O esqueleto completo do ecossistema em Rust está totalmente configurado como um workspace. De acordo com o manifesto de compilação, os arquivos de código-fonte (`src/lib.rs` ou equivalentes) já existem fisicamente para quase todos os blocos lógicos planejados:

* **Módulos do Motor Central (V1):** `heraclitus-core`, `heraclitus-log`, `heraclitus-manifold`, `heraclitus-memtable`, `heraclitus-views`, `heraclitus-index-vector`, `heraclitus-index-text`, `heraclitus-activation`, `heraclitus-retrieval`, `heraclitus-distill`, `heraclitus-tier`, `heraclitus-txn`, `heraclitus-query`, `heraclitus-raft`, `heraclitus-server`, `heraclitus-cli`, `heraclitus-client` e `heraclitus-proto`.
* **Módulos do Roteiro Avançado (V2.0):** Além da infraestrutura básica, os arquivos iniciais para a engine de grafos e análises já constam no repositório, incluindo `heraclitus-analytics` e arquivos especializados dentro de `heraclitus-index-graph`, como `adaptive.rs`, `decision.rs`, `entity.rs` e `temporal.rs`.
* **Crate de Conformidade Adicional:** O módulo `heraclitus-compliance` está fisicamente presente e estruturado com suporte a logs sementes, processos de assinatura (RFC3161, TSA, signer, etc.) e trabalhadores de verificação.

### B. Camada de Chaves Canônicas (SPEC-009)

* **Alinhamento de Identidade do EventId:** Foi corrigido e consolidado que o `EventId` possui 128 bits (16 bytes), operando nativamente no tempo como um ULID.
* **Compactação de Identidade:** A projeção de identificadores globais para chaves locais contíguas de 32 bits (`u32`) foi arquitetada para reduzir *cache misses* e aumentar em 400% a densidade volumétrica de linhas de cache da CPU durante as travessias analíticas.
* **Codec de Ordenação Canônica:** O componente `CanonicalKeyCodec` está implementado fisicamente em `heraclitus-core/src/vm/codec.rs`, garantindo a transformação e reversão estável de tipos numéricos (`i64`, `f64`) em chaves binárias `u64` ordenáveis (incluindo o colapso canônico de NaNs e normalização de zeros).

### C. Maturidade do Desenho Conceitual (SPEC-010 e SPEC-011)

* **Definição de Invariantes e Filosofia:** O design conceitual está rigidamente alinhado com as premissas do HeraclitusDB: o log append-only é a única verdade, enquanto índices e estados analíticos são visões puramente derivadas, descartáveis e reconstruíveis por replay determinístico.
* **Maturidade das Especificações:** As diretrizes para os motores de armazenamento temporal segmentado (SPEC-010) e do runtime de infraestrutura (SPEC-011) estão declaradas como concluídas e congeladas, chancelando o projeto para dar início à codificação prática da infraestrutura.

## 2. O que NÃO está Feito (Pendentes, Conceituais ou Desconectados)

### A. Arquivos e Implementações Físicas em Falta

* **Ausência de `dense_map.rs`:** Embora a especificação `SPEC-009` defina minuciosamente a lógica de isolamento do pipeline e forneça um bloco de código completo em Rust para o mapa de entidades densas (`DenseEntityMap`), o arquivo `dense_map.rs` **não aparece fisicamente** no manifesto de arquivos existentes do repositório, indicando que o componente ainda precisa ser fisicamente extraído/criado no crate `heraclitus-index-graph`.
* **Componentes de Ecossistema Externo:** Elementos citados no mapeamento original de diretórios (como o SDK Python em `sdk/python/`, o servidor Model Context Protocol em `mcp/`, o console web em `console/` e os harnesses de benchmark em `bench/locomo/`) **não possuem nenhum arquivo rastreado** ou com hash semanticamente avaliado no manifesto atual, permanecendo inteiramente pendentes.

### B. Mecânicas Avançadas Relatadas nas Specs (Conceitos Pendentes de Codificação)

* **Infraestrutura Temporal em Disco (SPEC-010):** A divisão física do log em arquivos de segmentos autocontidos (`LogSegment`), os rodapés estruturados (`Footer` contendo filtros Bloom/Ribbon e raízes Merkle), o gerenciador em memória `SegmentCatalog` e os mapas de zonas (`ZoneMap`) ainda são definições de engenharia a serem escritas no código.
* **Algoritmos de Otimização Analítica (SPEC-010):** A poda de predicados físicos na fronteira do disco (*Skip I/O* por tempo ou filtros probabilísticos), bem como a compactação adaptativa de layout (Dictionary, Delta, Delta-of-Delta e FoR) para o formato colunar analítico não estão funcionais no código atual.
* **Paralelismo e Efemeridade (SPEC-010 / SPEC-011):** O pipeline de replay paralelo com merge determinístico baseado no `SegmentCatalog`, a alocação *on-demand* de índices efêmeros transitórios (`Transient Indexes`) e a lógica de índices LSM Delta permanecem no plano de design conceitual.
* **Runtime e Arbitragem de Recursos (SPEC-011):** A codificação da trait `StorageEngine`, o manifesto unificado `DatabaseManifest`, o controle de isolamento `TransactionSnapshot`, o cache de materializações voláteis (`Replay Materialization Cache`) e os componentes do `Memory Manager` e do `Resource Scheduler` (responsáveis por ditar as políticas térmicas de RAM e fallbacks para execução imperativa básica se faltar hardware) estão listados como "prontos para codificação", o que reitera que sua lógica física ainda precisa ser materializada no repositório.

### C. Gates de Maturidade e Critérios de Validação das Milestones

Embora os arquivos base das crates existam, as suítes complexas de testes e benchmarks exigidas nos critérios de aceitação de cada Milestone (M0 a M18) ainda não foram homologadas:

* O teste de injeção de falhas simulando a queda do processo 1.000 vezes no meio do append para garantir a recuperação de torn-writes (M0).
* A execução mandatória de 50.000 casos de teste via `proptests` para validar a simetria de ordenação do `CanonicalKeyCodec` (Gate 1 da SPEC-009).
* Os testes determinísticos de partição de rede, clock-skew e kill de líder utilizando a biblioteca `turmoil` para o consenso Raft (M6).
* A publicação oficial das curvas analíticas de QPS × recall em comparação direta com Qdrant/pgvector (M7).

## 3. Alinhamento de Prioridades e Roteiro Estratégico

Cruzando as notas sobre a estratégia técnica foca em **licitações federais** com as especificações, há uma clara distinção do que deve ser atacado imediatamente e o que foi conscientemente postergado:

### Foco Máximo Reorganizado:

1. **SPEC-009 & SPEC-010:** Mapeamento denso (`DenseEntityMap`) e representação física avançada (layout CSR + mapas de zona) para viabilizar as consultas de alto desempenho essenciais em relatórios e auditorias governamentais.
2. **SPEC-021 & SPEC-011:** Replicação, Alta Disponibilidade (via consenso Raft) e concorrência avançada (EBR) para garantir a resiliência rígida exigida em contratos públicos.
3. **SPEC-023:** HQL (subconjunto Cypher/GQL) e a Provenance Engine (viabilizada pelo operador `WHY`), transformando o rastreamento causal e a linhagem de dados em um diferencial regulatório estrutural de primeira classe.
4. **Observabilidade:** Criação de uma especificação futura inteiramente dedicada a métricas e rastreamento de infraestrutura.

### Postergado / Fora de Prioridade Atual:

* **Hardware Especializado:** Otimizações complexas de topologias NUMA, uso profundo de registradores AVX-512 e kernels focados em aceleração direta por GPU (embora o esqueleto do crate `heraclitus-gpu` já exista, a lógica matemática avançada foi deixada para depois).
* **Mecanismos Internos:** O desenvolvimento de um motor de compilação JIT extremamente sofisticado foi empurrado para fases posteriores.
* **Não-Objetivos Permanentes:** Embutir clientes HTTP internos, rotinas cron automatizadas, daemons autônomos ou chamadas nativas para modelos de linguagem (LLM) permanecem terminantemente fora do escopo do banco de dados.

### Fase 0 — Verdade de Base: Alinhamento de Expectativas vs. Realidade

* **Por que bate:** No diagnóstico anterior, identificamos que arquivos como `SPEC-009` afirmavam estar em status **"CONGELADA / ALINHADA COM O CORE"**, quando na verdade componentes centrais descritos neles (como o mapa de entidades `dense_map.rs`) sequer existiam fisicamente no manifesto de arquivos do repositório.
* **Impacto:** Rebaixar a pasta de especificações novas para `RFCs/` e exigir um relatório honesto de benchmarks (`benches/REPORT.md`) limpa a "maquiagem" do projeto e estabelece uma linha de base real e auditável.

### Fase 1 — Fechar a Espinha: Engrossar o que está "Fino"

* **Por que bate:** Atualmente, os crates `heraclitus-txn` e `heraclitus-raft` possuem arquivos estruturados, mas a implementação prática de isolamento MVCC e replicação de máquina de estados por consenso é superficial em comparação com o rigor exigido pelas Milestones M4 e M6.
* **Impacto:** Forçar testes reais de aceitação para as Milestones de V2.0 (M8–M18) — focando em determinismo bit-a-bit e estabilidade de `state_hash` — garante que a fundação mestre do banco (`SPEC.md`) se consolide antes de erguer qualquer puxadinho de performance.

### Fase 2 — Armazenamento Analítico Compatível: Filtros Lógicos e Estruturas Seguras

* **Por que bate:** As especificações `SPEC-010` e `SPEC-011` trazem excelentes conceitos de armazenamento (Segmentos autocontidos, `SegmentCatalog`, `ZoneMaps` e podas de I/O). Esta fase consolida essas boas ideias introduzindo objetos explícitos de controle (`DatabaseManifest` e `TransactionSnapshot`), que dão corpo à Milestone M4 de transações lógicas.
* **Garantia de Determinismo:** A proibição explícita de reduções de ponto flutuante associativas não-determinísticas é o mecanismo que viabiliza o "Determinismo Absoluto de Consulta" exigido no design lógico do motor analítico.

### Fase 3 — Aceleração: Controle de Danos contra Over-Engineering

* **Por que bate:** A revisão orientada a licitações públicas federais (`SPEC-INDEX.md`) pedia alta prioridade para o `DenseEntityMap` (SPEC-009), mas alertava contra o desperdício de tempo em JIT extremamente sofisticado, aceleração por GPU e AVX-512.
* **Impacto:** Ao criar um **Gate de Entrada estrito baseado em benchmark**, você impede que a equipe gaste semanas programando kernels de silício ou JIT analítico complexo para o operador `WHY` sem antes provar que a abordagem imperativa local (mais simples e amigável ao cache) esgotou sua capacidade física.

### Fase 4 e 5 — Distribuído e Ecossistema: Preservação da Tese de Design

* **Por que bate:** O uso da biblioteca `turmoil` em testes de simulação determinística de rede e clock-skew é a forma correta de validar o `heraclitus-raft`.
* **Mitigação de Riscos:** O seu "Aviso honesto" na Fase 5 toca na ferida do projeto: colocar plugins WASM, catálogos de capacidades ou DAGs de dependência dentro da engine de dados viola diretamente a restrição fundamental de que **"a inteligência vive no agente, não no banco de dados"**. Avaliar e deferir individualmente cada item impede o corrompimento da filosofia minimalista do banco.

### Rejeição da SPEC-023 (HQL): Decisão Técnica Perfeita

* **Por que bate:** O repositório já possui uma gramática Pest estruturada em `heraclitus-query/src/gql.pest` e a especificação mestre (`SPEC.md`) determina explicitamente o uso de um subconjunto de **Cypher/GQL**, vetando a invenção de novas linguagens proprietárias de consulta.
* **Conclusão:** Rejeitar a criação de uma linguagem paralela (HQL) evita fragmentação de escopo. A engine de proveniência de dados e o rastreamento causal do operador `WHY` podem e devem ser expostos simplesmente como extensões lógicas ou funções de tabela dentro do próprio ecossistema GQL/Cypher existente.

O plano está **chancelado**. Ele transforma o roadmap em um pipeline de engenharia focado em entregas de valor real e blindagem arquitetural. Pode seguir com a execução.

RESUMO PARA CLAUDE FABLE5:
Este é o **Briefing Técnico de Execução Estrutural (F Protocol)** projetado especificamente para ser injetado no contexto de um agente de IA de codificação avançada (como o Claude). Ele contém as diretrizes exatas, o mapeamento de arquivos do workspace real, as regras de negócio e as invariantes arquiteturais do **HeraclitusDB** para executar as **Fases 0 e 1** do plano de desenvolvimento chancelado.

---

# 📑 PROMPT DE EXECUÇÃO: PROTOCOLO DE REESTRUTURAÇÃO DO CORE (FASES 0 E 1)

## 1. CONTEXTO E TESE DE DESIGN DO SISTEMA

Você é o engenheiro de software sênior responsável pelo núcleo de armazenamento analítico e distribuído do **HeraclitusDB**. O HeraclitusDB não é apenas um SGBD; ele é uma **Plataforma de Computação de Dados Relógios-Temporais Baseada em Logs Imutáveis**. Sua missão é executar a **Fase 0** e a **Fase 1** do plano mestre de engenharia.

### 🚫 As Cinco Invariantes Primordiais (Soberania do Código)

1. **O Log é a Única Verdade:** O log de episódios em disco é append-only, segmentado e imutável. Índices, grafos, views e ativações são efêmeros, descartáveis e reconstruíveis do zero por replay determinístico.
2. **Inteligência no Agente, não no Banco:** O core do banco é entediante, brutal e focado em performance mecânica de hardware. Sem runtimes embutidos de LLM, sem schedulers complexos internos e sem desvios de escopo agênticos.
3. **Rust Stable Estrito:** Proibido o uso de qualquer feature `nightly` ou macros experimentais.
4. **Rejeição do HQL (Manter GQL/Cypher):** Qualquer tentativa de criar uma sintaxe proprietária (HQL) está abortada. A linguagem de consulta oficial e unificada do banco é baseada na gramática de subconjunto **Cypher/GQL** mapeada no parser `heraclitus-query/src/gql.pest`.
5. **Postergação de Otimizações Prematuras de Hardware:** Recursos complexos de NUMA, JIT de alta performance para álgebra esparsa, processamento direto por kernels de GPU e instruções AVX-512 estão explicitamente adiados por critério de ROI para licitações federais. Foco total no modelo de cache friendly em CPU padrão.

---

## 2. MAPA DE ARQUIVOS ALVO DO WORKSPACE

Você deve ler, alterar, estender ou criar exclusivamente o conjunto contido de caminhos abaixo rastreados no manifesto real do repositório:

### 📁 Documentações e Metadados (Ajuste de Status)

* `SPEC-NEW/SPEC-009-u64.md`
* `SPEC-NEW/SPEC-010.md`
* `SPEC-NEW/SPEC-011.md`
* `SPEC-NEW/SPEC-INDEX.md`

### 📁 Código-Fonte do Núcleo Transacional e Log

* `heraclitus-core/src/vm/codec.rs` (Onde reside o `CanonicalKeyCodec`)
* `heraclitus-core/src/lib.rs`
* `heraclitus-log/src/lib.rs` & `heraclitus-log/src/format.rs`
* `heraclitus-txn/src/lib.rs` (Fino demais — alvo prioritário da Fase 1)
* `heraclitus-raft/src/lib.rs` (Fino demais — alvo prioritário da Fase 1)

### 📁 Motores de Visões e Índices de Grafos

* `heraclitus-views/src/lib.rs`
* `heraclitus-index-graph/src/lib.rs`
* `heraclitus-index-graph/src/entity.rs`
* `heraclitus-index-graph/src/temporal.rs`
* `heraclitus-index-graph/src/decision.rs`
* `heraclitus-index-graph/src/adaptive.rs`

### 📁 Suítes de Validação e Benchmarks

* `heraclitus-log/benches/append.rs`
* `heraclitus-index-vector/benches/hnsw_search.rs`
* `heraclitus-log/tests/crash_injection.rs`
* `heraclitus-log/tests/v2_compat.rs`
* `heraclitus-views/tests/fast_boot.rs`

---

## 3. ROTEIRO DETALHADO DE EXECUÇÃO: O QUE FAZER

### 🛠️ FASE 0 — VERDADE DE BASE (Sincronização de Sanidade e Status)

#### Tarefa 0.1: Saneamento Estatístico e Técnico da Documentação

* Abra as especificações `SPEC-009-u64.md`, `SPEC-010.md` e `SPEC-011.md`.
* **Rebaixamento de Status:** Remova a flag enganosa `Status: CONGELADA / IMPLEMENTADA`. Altere o cabeçalho explicitamente para: `STATUS: PROPOSTA / RFC EM FASE DE DESIGN FÍSICO`. O repositório precisa espelhar um status honesto para auditoria corporativa.
* **Mover/Alinhar Metadados:** Certifique-se de documentar que as estruturas físicas dessas SPECs novas não estão congeladas no binário e dependem dos gates de benchmark da Fase 3.

#### Tarefa 0.2: Consolidação de Benchmarks Baselines

* Varra as suítes em `heraclitus-log/benches/append.rs` e `heraclitus-index-vector/benches/hnsw_search.rs`.
* Certifique-se de que a compilação via `cargo bench --workspace` passe perfeitamente na VM de GPU.
* Escreva um arquivo físico novo chamado `benches/REPORT.md`, contendo de maneira explícita e isenta de simulações os números estáveis de throughput de append (escriba síncrono do log) e latência de recall vetorial base do HNSW.

---

### 🧱 FASE 1 — FECHAR A ESPINHA (Engrossar as Camadas Críticas)

#### Tarefa 1.1: Engrossamento de `heraclitus-txn` (Milestone M4)

Atualmente, o gerenciamento transacional do banco é apenas uma casca. Você deve transformar `heraclitus-txn/src/lib.rs` em um motor real de isolamento temporal de snapshots.

* **Codificar a Abstração `TransactionSnapshot`:** Implemente a estrutura formal explicitada no design arquitetural:
```rust
pub struct TransactionSnapshot {
    pub target_lsn: u64,
    pub watermark_lsn: u64,
    pub visible_segments: Vec<u64>,
}

```

* **Contrato de Leitura:** Garanta que todas as consultas subsequentes iniciadas no motor analítico ou de consultas invoquem e carreguem obrigatoriamente um `TransactionSnapshot` imutável. Mutações simultâneas no log ativo pós-`target_lsn` devem ser matematicamente invisíveis para a thread de execução da query.

#### Tarefa 1.2: Conexão Real de `heraclitus-raft` (Milestone M6)

* Abra `heraclitus-raft/src/lib.rs`. Remova stubs de memória volátil.
* **Log-Shipping de Bytes Puros:** Acople a engine do `openraft` diretamente sobre o `heraclitus-log`. O Raft deve sincronizar estritamente os bytes contíguos sequenciais dos registros de episódios binários brutos.
* **Isolamento de Visão Distribuída:** Respeite a invariante de que views parciais locais e matrizes analíticas *nunca* viajam via rede. Cada nó distribuído recebe o log ship bruto e roda de forma síncrona/assíncrona seu próprio pipeline local de hidratação analítica baseada no seu hardware soberano.

#### Tarefa 1.3: Homologação do Determinismo de Replay na V2.0 (Milestones M8 a M18)

Você deve criar e injetar asserções estritas em `heraclitus-views/src/lib.rs` e no motor de hidratação de grafos (`heraclitus-index-graph/src/lib.rs`) para provar bit a bit os critérios de aceitação.

* **Garantia de Identidade Estável (`state_hash`):** Implemente uma rotina baseada no algoritmo **Blake3** que computa a assinatura digital do estado de memória das visões após processar uma faixa fixa de LSNs.
* **Casos de Teste Cruzados de Replay:** Escreva um teste de integração rigoroso em `heraclitus-views/tests/fast_boot.rs` que:
1. Hidrata um índice de grafo a partir do log até o LSN 50.000 e extrai o `state_hash`.
2. Apaga completamente o estado físico persistido em disco (simulando perda total das visões derivadas RocksDB).
3. Dispara o comando de reconstrução analítica do zero (`rebuild_from(0)`).
4. Afirma (`assert_eq!`) com tolerância de zero bits que o novo `state_hash` pós-replay sequencial é identicamente perfeito ao hash original.


* **Bloqueio de Reduções Associativas de Ponto Flutuante:** Proíba explicitamente loops analíticos concorrentes que acumulam valores `f32`/`f64` usando associatividade arbitrária fora de ordem (o que causa variações infinitesimais de arredondamento de hardware que quebram o determinismo global do banco). Reduções numéricas acumuladas devem seguir ordenações estritas indexadas pelo LSN.

---

## 4. RESTRIÇÕES DE ENGENHARIA E DIRETRIZES DE HARDWARE

* **Controle de Alinhamento na CPU:** Estruturas de dados analíticas homogêneas congeladas na Fase 3 devem prever explicitamente alinhamentos em fronteiras de memória compatíveis com as cache lines da arquitetura nativa (alinhamento em 64 bytes para CPU modernas padrão).
* **Sem Erros Omitidos:** Erros de corrupção ou checksum mismatch (CRC32 de cauda de registros de registros ou hashes de Merkle) capturados durante varreduras ou replays jamais podem ser silenciados ou engolidos. Devem disparar pânicos controlados ou abortar imediatamente as transações analíticas de leitura, preservando o leito estável do rio contra falhas silenciosas de disco.

---

## 5. GATE DE COMPREENSÃO DO AGENTE

Antes de iniciar a emissão de código ou modificação física de arquivos, responda a este prompt declarando explicitamente:

1. O resumo do seu entendimento sobre a arquitetura de duplo compilador (*Dual-Compiler Architecture*) do HeraclitusDB.
2. A confirmação de que você não usará a sintaxe HQL e manterá a padronização unificada do parser GQL.
3. O plano exato das assinaturas de funções Rust que você injetará em `heraclitus-txn` para habilitar os isolamentos estáveis de snapshot baseados em LSN.

Aqui tens o blueprint absoluto, **completo, corrigido e unificado** do **HeraclitusDB (Versão 3.2.0 Stable Baseline)**.

Este documento consolida a Constituição Arquitetural, as Máquinas de Estado, os Modelos Matemáticos, a Especificação de Erros (`SPEC-ERR`) e o código de infraestrutura em Rust estável, com todas as correções cirúrgicas aplicadas (remoção de acoplamentos, uso de *Newtypes*, modelo de custo multidimensional, eliminação do "Bus" pelo pipeline linear, e o axioma da confluência).

---

# 📑 HERACLITUSDB: ARCHITECTURAL SPECIFICATION & INITIAL CORE BASINE (v3.2.0)

## PART 1: THE CONSTITUTION (ARCHITECTURE.md)

Este manifesto possui soberania estatutária sobre qualquer especificação física ou decisão de otimização posterior.

### THE TEN INVARIANTS (As Dez Invariantes)

1. **Log is the only source of truth:** O log sequencial de episódios em disco é a única verdade histórica do sistema.
2. **Replay is deterministic:** A reconstrução do estado a partir do log produz os mesmos bits sob qualquer hardware estável.
3. **Runtime is stateless:** O motor de execução vetorizado não retém estados mutáveis persistentes entre consultas.
4. **Snapshots are immutable:** Uma vez capturada, uma fotografia temporal de LSN nunca sofre mutações.
5. **Compilers never mutate storage:** Os pipelines de compilação analisam e transformam planos; eles nunca gravam dados.
6. **Storage never understands queries:** A camada de disco manipula páginas, blocos e metadados brutos; ela ignora semânticas GQL/Cypher.
7. **Provenance never changes data:** O fluxo explicativo rastreia e reconstrói dependências sem jamais alterar o leito do log.
8. **All derived structures are disposable:** Índices, views e caches de grafos são artefatos voláteis, descartáveis e reidratáveis do zero.
9. **Physical layout is never observable:** Operadores externos e APIs enxergam apenas semânticas lógicas; layouts binários de disco são ocultos.
10. **APIs expose logical semantics only:** O ecossistema externo interage por contratos declarativos de dados; o estado interno é inteiramente opaco.

### MÁQUINAS DE ESTADO FORMAIS DO SISTEMA

#### Ciclo de Vida do Segmento de Log (`SegmentState`)

$$\text{Open} \longrightarrow \text{Sealed} \longrightarrow \text{Indexed} \longrightarrow \text{Checkpointed} \longrightarrow \text{Archived}$$

#### Ciclo de Vida do Snapshot Temporal (`SnapshotState`)

$$\text{Created} \longrightarrow \text{Active} \longrightarrow \text{Pinned} \longrightarrow \text{Released}$$

#### Ciclo de Vida do Pipeline de Consulta (`QueryState`)

$$\text{Parsed} \longrightarrow \text{Logical} \longrightarrow \text{Optimized} \longrightarrow \text{Executing} \longrightarrow \text{Finished}$$

---

## PART 2: FORMALIZAÇÃO MATEMÁTICA E RESILIÊNCIA (`SPEC-ERR`)

### 1. Definições Algébricas Puras

* **O Log:** Sequência indexada finita de eventos, ordenada linearmente: $\mathcal{L} = [ E_0, E_1, \dots, E_n ]$, onde $\text{LSN}(E_i) = i$.
* **Função Pura de Replay ($R$):** $R: \mathcal{S} \times \mathcal{E} \rightarrow \mathcal{S}$. O estado global no limiar $n$ é: $\mathcal{R}^*(\mathcal{L}[0 \dots n]) = R(R(\dots R(S_0, E_0), E_1), \dots, E_n) = S_n$.
* **Axioma da Confluência Log-Temporal:** 
$$\mathcal{R}^*(S, \mathcal{L}_1 \parallel \mathcal{L}_2) \equiv \mathcal{R}^*\Big(\mathcal{R}^*(S, \mathcal{L}_1), \mathcal{L}_2\Big)$$

* **Idempotência do Elemento Neutro (Log Vazio $\varnothing$):**

$$\mathcal{R}^*(S_n, \varnothing) = S_n \implies \mathcal{R}^*\Big(\mathcal{R}^*(S_n, \varnothing), \varnothing\Big) = S_n$$

### 2. Contenção de Danos e Falhas (`SPEC-ERR`)

* **Checksum Mismatch (CRC32C):** Se houver corrupção física detetada nos blocos ou no *Footer* do segmento, o silenciamento é proibido. O sistema dispara `panic!`, aborta a transação de leitura e isola o nó. A recuperação exige o descarte das visões derivadas e o re-trigger do `ReplayDispatcher` a partir do último checkpoint lícito do líder do Raft.
* **Estouro de OOM e Budgets:** O `TaskScheduler` monitora continuamente as threads de execução através dos limites definidos no `ExecutionContext`. Abortos por exaustão de `MemoryBudget` ou estouro de tempo em `CpuBudget` são capturados via `catch_unwind`. A query ofensiva é sumariamente eliminada, os seus buffers Apache Arrow contíguos são desalocados e o processo master do banco permanece intocado.

---

## PART 3: O NÚCLEO CONFIGURADO (CÓDIGO RUST ESTÁVEL)

Abaixo encontra-se a arquitetura de tipos, contextos e traits distribuída pelos módulos do workspace, pronta para compilação.

### `heraclitus-core/src/types.rs`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Lsn(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct SegmentId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct EntityId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct CatalogEpoch(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct StateHash(pub [u8; 32]);

/// Fotografia estritamente lógica e minimalista do estado temporal. 
/// Tamanho fixo (24 bytes). Cópia zero de vetores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionSnapshot {
    pub target_lsn: Lsn,
    pub catalog_epoch: CatalogEpoch,
    pub watermark_lsn: Lsn,
}

pub struct MemoryBudget {
    pub allowed_bytes: usize,
    pub used_bytes: usize,
}

pub struct CpuBudget {
    pub max_microseconds: u64,
}

pub struct CancellationToken {
    pub is_cancelled: std::sync::atomic::AtomicBool,
}

/// Objeto de isolamento operacional mandatório para execução de tarefas no Runtime.
pub struct ExecutionContext {
    pub snapshot: std::sync::Arc<TransactionSnapshot>,
    pub memory_budget: std::sync::Arc<parking_lot::Mutex<MemoryBudget>>,
    pub cpu_budget: CpuBudget,
    pub cancellation: std::sync::Arc<CancellationToken>,
}

pub trait LogClock: Send + Sync {
    fn allocate_lsn(&self) -> Lsn;
    fn current_lsn(&self) -> Lsn;
}

```
### `heraclitus-core/src/ir.rs`

```rust
use crate::types::{CatalogEpoch, EntityId};

/// Representação puramente declarativa das intenções da consulta.
pub enum LogicalPlan {
    Select { relations: Vec<String>, predicate_node_id: u32 },
    GraphMatch { pattern_id: u32 },
    TraceProvenance { target: EntityId },
}

/// Alfabeto de baixo nível interpretado mecanicamente pelo Runtime Vetorizado.
#[derive(Debug, Clone)]
pub enum PhysicalIr {
    ColumnScan { epoch: CatalogEpoch, projection_indices: Vec<u32> },
    VectorFilter { predicate_expression_id: u32 },
    HashJoin { left_key_idx: u32, right_key_idx: u32 },
    VectorAggregate { grouping_keys: Vec<u32>, aggregations: Vec<u32> },
}

/// Cadeia esparsa de dependências compilada para o motor explicativo (Compiler 2).
pub enum ExplainIr {
    BuildCausalSubGraph { target: EntityId },
    ExtractCsrCoordinates { matrix_id: u64 },
    InvertSparseMatrixLinear,
}

/// Representa um nó real dentro do Grafo de Operadores Físicos (Physical DAG).
#[derive(Debug, Clone)]
pub struct ExecutionNode {
    pub node_id: u64,
    pub operation: PhysicalIr,
    pub dependencies: Vec<u64>,
}

```
### `heraclitus-core/src/cost.rs`

```rust
use crate::ir::PhysicalIr;
use crate::types::SegmentId;

/// Modelo de custo multidimensional extensível baseado em métricas físicas reais.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CostEstimate {
    pub cpu_cycles: u64,
    pub memory_bytes: u64,
    pub io_pages: u64,
    pub network_bytes: u64,
    
    // Extensibilidade tática para calibração pós-benchmarks de microarquitetura
    pub cache_misses: Option<u64>,
    pub branch_mispredictions: Option<u64>,
    pub ssd_queue_depth: Option<u32>,
}

impl CostEstimate {
    pub fn compute_weighted_score(&self, io_weight: f64, cpu_weight: f64) -> f64 {
        (self.io_pages as f64 * io_weight) + (self.cpu_cycles as f64 * cpu_weight)
    }
}

pub trait CostModel: Send + Sync {
    fn estimate_node_cost(&self, op: &PhysicalIr, segment: SegmentId) -> CostEstimate;
}

```
### `heraclitus-core/src/dispatcher.rs`

```rust
use crate::types::{Lsn, SegmentId, EntityId};

pub trait ReplaySink: Send + Sync {
    fn sink_identifier(&self) -> &'static str;
    fn consume_log_record(&mut self, lsn: Lsn, segment: SegmentId, entity: EntityId, payload: &[u8]) -> Result<(), String>;
    fn commit_checkpoint(&mut self, lsn: Lsn) -> Result<(), String>;
}

/// Pipeline linear e estritamente sequencial para difusão de episódios (Substitui o Bus).
pub struct ReplayDispatcher {
    ordered_sinks: Vec<Box<dyn ReplaySink>>,
}

impl ReplayDispatcher {
    pub fn new() -> Self {
        Self { ordered_sinks: Vec::new() }
    }

    pub fn attach_sink(&mut self, sink: Box<dyn ReplaySink>) {
        self.ordered_sinks.push(sink);
    }

    /// Despacha o registro linearmente. Se um único Sink falhar, o pipeline inteiro aborta,
    /// protegendo as visões derivadas contra dessincronização de LSN.
    pub fn dispatch_record(&mut self, lsn: Lsn, segment: SegmentId, entity: EntityId, payload: &[u8]) -> Result<(), String> {
        for sink in &mut self.ordered_sinks {
            sink.consume_log_record(lsn, segment, entity, payload).map_err(|err| {
                format!("CRITICAL - [SPEC-ERR] Aborto do Replay no Sink {}: {}", sink.sink_identifier(), err)
            })?;
        }
        Ok(())
    }
}

### `heraclitus-core/src/contracts.rs`

```rust
use crate::types::*;
use crate::ir::{LogicalPlan, PhysicalIr, ExecutionNode};
use arrow::record_batch::RecordBatch;

pub trait Planner: Send + Sync {
    fn generate_logical_plan(&self, query: &str) -> Result<LogicalPlan, String>;
}

pub trait Optimizer: Send + Sync {
    fn optimize_to_physical_ir(&self, plan: LogicalPlan, model: &dyn crate::cost::CostModel) -> Result<Vec<ExecutionNode>, String>;
}

pub trait TaskScheduler: Send + Sync {
    fn execute_dag(&self, dag: Vec<ExecutionNode>, ctx: &ExecutionContext) -> Result<Vec<RecordBatch>, String>;
}

pub trait StorageEngine: Send + Sync {
    fn read_block(&self, segment: SegmentId, offset: u64, dest: &mut [u8]) -> Result<usize, String>;
    fn append_block(&self, segment: SegmentId, data: &[u8]) -> Result<u64, String>;
}

pub trait SegmentCatalog: Send + Sync {
    fn resolve_active_segments(&self, epoch: CatalogEpoch) -> Vec<SegmentId>;
    fn current_epoch(&self) -> CatalogEpoch;
}

### `heraclitus-views/src/crypto.rs`

```rust
use crate::types::{SegmentId, Lsn, EntityId, StateHash};
use blake3::Hasher;

pub struct CanonicalPayload {
    pub segment_id: SegmentId,
    pub lsn: Lsn,
    pub entity_id: EntityId,
    pub fields: Vec<(String, f64)>,
}

/// Computa o hash de estado perfeito e canónico de acordo com a formulação matemática.
/// state_hash := BLAKE3(CanonicalSerialization(SortBy(SegmentId ≻ Lsn ≻ EntityId)))
pub fn compute_immutable_state_hash(mut dataset: Vec<CanonicalPayload>) -> StateHash {
    let mut hasher = Hasher::new();
    
    // Regra 1: Ordenação Clustered Estrita pela Tupla Primordial
    dataset.sort_by(|a, b| {
        a.segment_id.0.cmp(&b.segment_id.0)
            .then_with(|| a.lsn.0.cmp(&b.lsn.0))
            .then_with(|| a.entity_id.0.cmp(&b.entity_id.0))
    });

    // Número Mágico de Versão do Codec (HRC1)
    hasher.update(&[0x48, 0x52, 0x43, 0x01]);

    for item in dataset {
        // Regra 2: Endianness Big-Endian forçado para chaves primárias
        hasher.update(&item.segment_id.0.to_be_bytes());
        hasher.update(&item.lsn.0.to_be_bytes());
        hasher.update(&item.entity_id.0.to_be_bytes());

        // Regra 6: Ordenação lexicográfica de colunas para representação estruturada estável
        let mut sorted_fields = item.fields;
        sorted_fields.sort_by(|a, b| a.0.cmp(&b.0));

        for (column_name, value) in sorted_fields {
            hasher.update(column_name.as_bytes());
            
            // Regra 3: Normalização Estrita de Ponto Flutuante IEEE-754 (Quiet NaN Invariante)
            let normalized_bits = if value.is_nan() {
                0x7FF8000000000000u64
            } else if value == f64::INFINITY {
                0x7FF0000000000000u64
            } else if value == f64::NEG_INFINITY {
                0xFFF0000000000000u64
            } else {
                value.to_bits()
            };
            hasher.update(&normalized_bits.to_be_bytes());
        }
    }

    StateHash(*hasher.finalize().as_bytes())
}

/// Redução numérica contínua em ordenação determinística forçada linearmente pelo LSN.
pub fn deterministic_temporal_reduction(mut metrics: Vec<(Lsn, f64)>) -> f64 {
    metrics.sort_by_key(|item| item.0);
    
    let mut accumulator: f64 = 0.0;
    for (_, val) in metrics {
        accumulator += val;
    }
    accumulator
}

### `heraclitus-txn/src/lib.rs`

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::BTreeMap;
use parking_lot::RwLock;
use crate::types::{Lsn, CatalogEpoch, TransactionSnapshot};

pub struct SnapshotManager {
    current_lsn: AtomicU64,
    watermark_lsn: AtomicU64,
    current_epoch: AtomicU64,
    active_transactions: RwLock<BTreeMap<u64, usize>>,
}

impl SnapshotManager {
    pub fn new(initial_lsn: Lsn, initial_epoch: CatalogEpoch) -> Self {
        Self {
            current_lsn: AtomicU64::new(initial_lsn.0),
            watermark_lsn: AtomicU64::new(initial_lsn.0),
            current_epoch: AtomicU64::new(initial_epoch.0),
            active_transactions: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn begin_snapshot(&self) -> Arc<TransactionSnapshot> {
        let mut active = self.active_transactions.write();
        let target = self.current_lsn.load(Ordering::Acquire);
        let watermark = self.watermark_lsn.load(Ordering::Acquire);
        let epoch = self.current_epoch.load(Ordering::Acquire);
        
        *active.entry(target).or_insert(0) += 1;
        
        Arc::new(TransactionSnapshot {
            target_lsn: Lsn(target),
            catalog_epoch: CatalogEpoch(epoch),
            watermark_lsn: Lsn(watermark),
        })
    }

    pub fn release_snapshot(&self, snapshot: Arc<TransactionSnapshot>) {
        let mut active = self.active_transactions.write();
        if let Some(count) = active.get_mut(&snapshot.target_lsn.0) {
            *count -= 1;
            if *count == 0 {
                active.remove(&snapshot.target_lsn.0);
            }
        }
        
        if let Some((&lowest_active, _)) = active.iter().next() {
            self.watermark_lsn.store(lowest_active, Ordering::Release);
        } else {
            self.watermark_lsn.store(self.current_lsn.load(Ordering::Acquire), Ordering::Release);
        }
    }
}

## PART 4: RESOLUÇÃO DE GOVERNANÇA DE ENGENHARIA

> **Estatuto de Estabilidade (v3.2.0):** A arquitetura lógica e os contratos públicos do HeraclitusDB são considerados estáveis para início da implementação. Alterações estruturais futuras deverão ocorrer exclusivamente por RFC aprovada, preservando as invariantes arquiteturais e a compatibilidade dos contratos públicos. Aspectos internos de implementação permanecem sujeitos à evolução orientada por testes, benchmarks e validação experimental em nível de hardware real.

O blueprint está completo, corrigido e pronto para admissão de código. Pode subir o repositório.

**Inicie a execução imediatamente após a validação do gate.**

