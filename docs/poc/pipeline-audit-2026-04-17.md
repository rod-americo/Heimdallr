# Auditoria do Pipeline Heimdallr

Data: 2026-04-17  
Host: `dtc-iapacs01` (`ms-heimdallr`)  
Recorte: `95` estudos residentes em `runtime/studies/`

## Resumo

1. O gargalo continua sendo a segmentação em CPU.
   - Média: `3min 38s`
   - Mediana: `3min 28s`
   - Configuração atual: `1` caso por vez com `14` thread-hints por tarefa

2. O consumo de disco está concentrado em `derived/`.
   - `runtime/studies`: `66.3 GiB`
   - `derived/`: `56.2 GiB` (`84.8%`)
   - média de `2.7` séries NIfTI por caso

3. O `egress` regular é rápido.
   - mediana real: `3s`
   - p80: `5s`
   - cálculo considera apenas itens da fila normal com `status=done` na primeira tentativa, sem reenvios manuais

4. A telemetria residente de RAM já permite fechar a política de concorrência da segmentação.
   - `resource_monitor_case_peaks` já acumula `61` casos distintos
   - na segmentação, o `p80` de `memory.current` do cgroup já chegou a `23.7 GB`
   - o mínimo de memória disponível do host durante segmentação observada caiu para `1.1 GB`
   - com `32 GiB` de RAM, a recomendação operacional continua sendo de `1` caso por vez na segmentação

## Critérios

- tempos calculados apenas sobre os `95` estudos ainda presentes em disco
- `segmentation` exclui casos com `reuse`
- `egress` exclui reenvios manuais e retries

## Tempos

| Etapa | n | Média | Mediana | Desvio padrão | p80 |
|---|---:|---:|---:|---:|---:|
| Prepare | 94 | `1min 24s` | `1min 12s` | `36s` | `1min 51s` |
| Segmentation | 88 | `3min 38s` | `3min 28s` | `1min 48s` | `4min 49s` |
| Metrics | 91 | `47s` | `47s` | `13s` | `55s` |
| Compute total | 88 | `5min 50s` | `5min 28s` | `2min 17s` | `7min 46s` |
| DICOM egress | 3490 | `21s` | `3s` | `3min 28s` | `5s` |

## Prepare

| Subetapa | n | Média | Mediana | Desvio padrão | p80 |
|---|---:|---:|---:|---:|---:|
| `extract_zip_seconds` | 94 | `2s` | `2s` | `1s` | `3s` |
| `scan_dicoms_seconds` | 94 | `2s` | `1s` | `1s` | `2s` |
| `select_and_convert_seconds` | 94 | `1min 20s` | `1min 10s` | `34s` | `1min 44s` |
| `convert_series_total_seconds` | 94 | `1min 20s` | `1min 8s` | `1min 11s` | `2min 7s` |
| `phase_detection_total_seconds` | 94 | `2min 19s` | `1min 28s` | `1min 55s` | `3min 43s` |
| `candidate_series_total_seconds` | 94 | `3min 38s` | `2min 46s` | `3min 2s` | `5min 54s` |

Leitura curta:
- o tempo serial do `prepare` está em conversão + seleção
- o custo agregado por série continua alto por causa do `phase_detection`

## Disco

### Pegada atual

| Área | Tamanho |
|---|---:|
| `runtime/studies` | `66.3 GiB` |
| `runtime/intake` | `649 MiB` |
| `runtime/queue` | `16 KiB` |
| `database` | `5.0 MiB` |

### Por caso

| Métrica | Valor |
|---|---:|
| Média | `714.7 MiB` |
| Mediana | `658.3 MiB` |
| p80 | `969.7 MiB` |
| Máximo | `1.63 GiB` |

### Por diretório

| Diretório | Total | Participação |
|---|---:|---:|
| `derived/` | `56.22 GiB` | `84.8%` |
| `artifacts/total/` | `5.80 GiB` | `8.7%` |
| `artifacts/metrics/` | `3.74 GiB` | `5.6%` |
| `artifacts/tissue_types/` | `0.55 GiB` | `0.8%` |
| `logs/` | `2.2 MiB` | desprezível |
| `metadata/` | `3.1 MiB` | desprezível |

### Redundância

- média de séries NIfTI por caso: `2.72`
- casos com mais de uma série NIfTI: `66/95`
- máximo por caso: `6`

## Observações operacionais

- o host está em `16` vCPUs e `32 GiB` RAM
- o `prepare` é serial por caso, mas paraleliza até `5` séries do mesmo caso
- a `segmentation` deve operar com `1` caso por vez neste host
  - cada tarefa do TotalSegmentator continua com `14` thread-hints
- a fila de `metrics` ainda é conservadora
  - `1` caso por vez
  - `max_parallel_jobs = 1`

## RAM observada com `resource_monitor`

O monitor residente já acumulou dados suficientes para leitura prática:

- `181` linhas em `resource_monitor_case_peaks`
- `61` casos distintos
- cobertura por etapa:
  - `prepare`: `61` casos
  - `segmentation`: `60` casos
  - `metrics`: `60` casos

### Memória relevante por etapa

| Etapa | p80 `memory.current` do serviço | pior memória livre do host |
|---|---:|---:|
| `prepare` | `12.7 GiB` | `2.8 GiB` |
| `segmentation` | `23.1 GiB` | `1.1 GiB` |
| `metrics` | `4.5 GiB` | `18.8 GiB` |

### Piores casos observados

- `prepare`
  - pico de `max_cgroup_memory_current`: `26.9 GiB`
  - pico de `max_subtree_pss`: `27.0 GiB`
- `segmentation`
  - pico de `max_cgroup_memory_current`: `29.3 GiB`
  - pico de `max_subtree_pss`: `28.9 GiB`
- `metrics`
  - pico de `max_cgroup_memory_current`: `5.4 GiB`
  - pico de `max_subtree_pss`: `5.4 GiB`

Leitura prática:

- a `segmentation` já encosta no limite físico de um host com `32 GiB`
- o `prepare` também apresenta bursts relevantes de memória
- o `metrics` permanece relativamente leve
- a RAM, portanto, não é mais uma incógnita; ela participa materialmente da definição de concorrência segura

## Hardware mínimo para `150 casos/dia`

Premissas desta estimativa:

- objetivo de `150` casos por dia, equivalente a `6.25` casos por hora em média
- remoção dos casos quase imediatamente após sucesso de `metrics + egress`
- sem retenção prolongada de `runtime/studies/`
- mesmo perfil atual de processamento, com segmentação em CPU

Leitura prática:

- a mediana atual de `compute total` é `5min 28s` por caso
- isso sugere capacidade teórica serial de aproximadamente `10–11` casos por hora
- na prática, a segmentação domina o consumo de CPU e introduz variabilidade
- portanto, para `150 casos/dia`, o mínimo seguro continua sendo um host equivalente ao atual, não menor

Configuração mínima recomendada para um único host:

| Recurso | Mínimo pragmático | Observação |
|---|---:|---|
| CPU | `16` vCPUs | abaixo disso, a segmentação CPU-only fica apertada demais para absorver variação e picos |
| RAM | `32 GiB` | suficiente apenas com `segmentation = 1 caso por vez` |
| Disco local | `200 GiB` SSD/NVMe | assume limpeza agressiva pós-processamento e pouco backlog residente |

Interpretação:

- com limpeza quase imediata, disco deixa de ser o principal limitador
- para `150 casos/dia`, os limitadores passam a ser CPU da segmentação e memória disponível durante segmentação
- em `32 GiB`, a configuração segura é manter `segmentation` serial
- se o objetivo incluir absorver picos concentrados em poucas horas com mais folga operacional, `24` vCPUs e `48 GiB` passam a ser a faixa mais saudável

## Conclusão operacional sobre concorrência

Com os dados atuais, já dá para fechar estas decisões:

1. `segmentation`: `1` caso por vez
   - a etapa já alcança `p80` de `23.7 GB` em `memory.current`
   - o pior mínimo de memória livre do host caiu para `1.0 GB`
   - manter concorrência de casos aqui, em `32 GiB`, é agressivo demais

2. `metrics`: manter `1` caso por vez
   - não é gargalo principal
   - o consumo de RAM é muito mais baixo e estável

3. `prepare`: manter sem concorrência entre casos neste host
   - os bursts de memória são relevantes
   - paralelizar casos no `prepare` dividiria mal a margem restante com a segmentação

## Prioridades

1. Reduzir oversubscription na segmentação.
   - a recomendação já pode ser fechada como `1 caso x 14 threads` no host atual

2. Reduzir retenção de NIfTI alternativo.
   - `derived/` sozinho já responde por `84.8%` do disco do pipeline

3. Persistir telemetria de etapas em formato consultável no SQLite.
   - concluído parcialmente com `resource_monitor_samples` e `resource_monitor_case_peaks`
