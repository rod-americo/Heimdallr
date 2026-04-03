# Prototipo local de jobs oportunisticos de osteoporose

Este prototipo gera artefatos locais para inspecao manual e validação rapida de quatro jobs:

* `bone_health_l1_hu`
* `bone_health_l1_volumetric`
* `vertebral_fracture_screen`
* `opportunistic_osteoporosis_composite`

Ele foi desenhado para rodar sobre estudos ja segmentados pelo TotalSegmentator, reaproveitando a estrutura de estudos do Heimdallr:

* `derived/<case_id>.nii.gz` para o CT convertido
* `artifacts/total/vertebrae_*.nii.gz` para as mascaras vertebrais
* `metadata/id.json` e `metadata/resultados.json` para metadados e rastreabilidade

O script tambem aceita estudos incompletos: se `L1` nao existir, ele faz fallback para outra vertebra disponivel, registrando a vertebra efetivamente usada.

## Script

Arquivo: [scripts/prototype_osteoporosis_jobs.py](/Users/rodrigo/Heimdallr/scripts/prototype_osteoporosis_jobs.py)

## Como executar

Rodar em um estudo unico:

```bash
python scripts/prototype_osteoporosis_jobs.py runtime/studies/_metrics_demo --output-root ~/Temp/lab-osteoporose
```

Rodar em todos os estudos sob um diretorio:

```bash
python scripts/prototype_osteoporosis_jobs.py --input-root runtime/studies --output-root ~/Temp/lab-osteoporose
```

Rodar o smoke test sintetico:

```bash
python scripts/prototype_osteoporosis_jobs.py --self-test
```

## Saida esperada

Cada caso gera uma arvore local em `~/Temp/lab-osteoporose/<case_id>/`:

```text
<case_id>/
  prototype_summary.json
  prototype_summary.md
  metrics/
    bone_health_l1_hu/
      result.json
      overlay.png
    bone_health_l1_volumetric/
      result.json
      overlay.png
    vertebral_fracture_screen/
      result.json
      profile.png
    opportunistic_osteoporosis_composite/
      result.json
```

No nivel superior do output, o script tambem grava `index.json` com o status de todos os estudos processados.

## Contrato de saida

Os JSONs usam nomes de metricas consistentes com o pipeline proposto:

* `bone_health_l1_hu`
* `bone_health_l1_volumetric`
* `vertebral_fracture_screen`
* `opportunistic_osteoporosis_composite`

Campos principais:

* `status`: `done`, `missing`, `indeterminate`, `negative`, `suspected` ou `error`
* `inputs`: caminhos e vertebra usada
* `measurement`: valores numericos e classificacao
* `qc`: flags de qualidade e necessidade de revisao manual
* `artifacts`: nomes dos arquivos produzidos dentro do diretorio do job

## Observacoes metodologicas

`bone_health_l1_hu` e `bone_health_l1_volumetric` sao triagens quantitativas e nao substituem DXA. O job volumetrico usa uma heuristica conservadora: erosao 3D, janela central e mascara principal conectada, para reduzir cortical e elementos posteriores.

`vertebral_fracture_screen` e propositalmente um screen heuristico. Ele classifica padroes de deformacao por perfil morfometrico e devolve `suspected` ou `indeterminate` quando a geometria nao oferece confianca suficiente.

O `opportunistic_osteoporosis_composite` combina os sinais acima em uma camada simples de priorizacao, sem tentar fazer interpretacao clinica definitiva.

## Onde o prototipo entra

Este prototipo fica no lado operacional do Heimdallr: executa apos a segmentacao, consome mascaras existentes, produz artefatos auditaveis e prepara evidencias para uma futura camada de inteligencia ou priorizacao mais sofisticada.
