**SPECs do Repositório (Base Atual do HeraclitusDB)**


| **SPEC**                  | **Escopo / Tema Principal**                                                                               | **Status no Projeto**                         |
| ------------------------- | --------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| **SPEC-0001 a SPEC-0041** | Fundações do projeto, motor analítico, concorrência, visões materializadas, indexação e réplicas. | **Concluídas / Mapeadas**no repositório.    |
| **SPEC-0042**             | Benchmark e decisão de arquitetura analítica (DataFusion vs. HUME)^^.<br/>                              | **Concluída**(Veredicto registrado)^^. <br/> |
| **SPEC-0043**             | Otimização de alta performance e suporte a vetores no HUME.                                             | **Concluída / Mapeada**no repositório.      |

**SPECs de Defesa, Desempenho e Governança (Fase Atual / A Implementar)**


| **SPEC**      | **Escopo / Tema Principal**                                                                                                                                                           | **Status**                                                                                 |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| **SPEC-0044** | **Otimização de Microarquitetura:**SIMD explícito (AVX2/AVX-512/NEON), zero-allocation no hot loop, fusão adaptativa de filtros e cache JIT.                                      | **Proposta / Projetada**(Aguardando implementação de código).                           |
| **SPEC-0045** | **Heraclitus Sentinel:**Agente autônomo de defesa (SOC), funil L0–L4, isolamento de instruções de IA e proveniência de decisões em LSNs^^.<br/>                                 | **Proposta / Projetada**(Aguardando implementação de código).                           |
| **SPEC-0046** | **Heraclitus Gov-Compliance:**Soberania nacional, operação*StrictAirGap*, carimbos do tempo ICP-Brasil (RFC 3161), conformidade GSI (NC-14/NC-21) e LGPD/ANPD^^. <br/>              | **Proposta / Projetada**(Aguardando implementação de código).                           |
| **SPEC-0047** | **Heraclitus Threat-Sync:**Troca de inteligência de ameaças com o CTIR Gov, STIX 2.1, TAXII 2.1, MISP e sanitização pré-exportação^^.<br/>                                     | **Proposta / Projetada**(Aguardando implementação de código).                           |
| **SPEC-0048** | **Forge Orchestrator & Forensic:**SOAR no-code/low-code, playbooks visuais tipados, aprovação humana e geração de laudos periciais em PDF/A com Merkle proofs^^.<br/>             | **Proposta / Projetada**(Aguardando implementação de código).                           |
| **SPEC-0049** | **Production Qualification (Q1–Q6):**Testes reais de carga, picos de tráfego, falha elétrica, ataques Red Team, atualizações sem downtime, perda de nó e restauração do zero. | **Proposta / Projetada**(Aguardando execução da suíte de qualificação).               |
| **SPEC-0050** | **HRKL v6 & Lakehouse:**Formato físico canônico, compressão por bloco (Zstd), arquivos sidecar (`.hrki`/`.hrkm`) e exportação assíncrona para Parquet/Iceberg/Delta^^. <br/>    | **Proposta / Projetada**(Aguardando refatoração do`heraclitus-log`e`heraclitus-tier`)^^. |
