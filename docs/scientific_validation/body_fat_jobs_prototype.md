# Prototipo local de jobs oportunisticos de gordura abdominal

Este prototipo implementa dois jobs operacionais para composicao corporal abdominal em estudos ja segmentados pelo Heimdallr:

* `body_fat_abdominal_volumes`
* `body_fat_l3_slice`

O objetivo e gerar artefatos auditaveis, com delimitacao padronizada por vertebras, antes de qualquer camada semantica ou assistiva.

## Script

Arquivo: [scripts/prototype_body_fat_jobs.py](/Users/rodrigo/Heimdallr/scripts/prototype_body_fat_jobs.py)

## Entradas esperadas

Layout de estudo Heimdallr:

* `derived/<case_id>.nii.gz`
* `artifacts/tissue_types/subcutaneous_fat.nii.gz`
* `artifacts/tissue_types/torso_fat.nii.gz`
* `artifacts/total/vertebrae_T12.nii.gz` ate `vertebrae_L5.nii.gz`
* `metadata/id.json`

## Delimitacao do bloco

O job volumetrico usa `T12-L5` como bloco abdominal padronizado.

Quando `T12`, `L1`, `L2`, `L3`, `L4` e `L5` estao todos presentes, as fronteiras entre slabs sao definidas no ponto medio entre os centros axiais das vertebras consecutivas.

Quando a cobertura esta incompleta, o job faz fallback para a extensao axial da mascara vertebral disponivel e marca o caso para revisao manual.

## Como executar

Estudo unico:

```bash
python3 scripts/prototype_body_fat_jobs.py runtime/studies/<case_id> --output-root ~/Temp/lab-gordura
```

Todos os estudos sob um diretorio:

```bash
python3 scripts/prototype_body_fat_jobs.py --input-root runtime/studies --output-root ~/Temp/lab-gordura
```

Smoke test sintetico:

```bash
python3 scripts/prototype_body_fat_jobs.py --self-test --output-root ~/Temp/lab-gordura
```

## Saida esperada

Para cada caso:

```text
<case_id>/
  prototype_summary.json
  prototype_summary.md
  metrics/
    body_fat_abdominal_volumes/
      result.json
      sagittal_profile.png
    body_fat_l3_slice/
      result.json
      overlay.png
```

Na raiz do output:

* `index.json` com o status de todos os casos processados

## Contrato de saida

`body_fat_abdominal_volumes`:

* `measurement.slabs.<level>.subcutaneous_fat_cm3`
* `measurement.slabs.<level>.torso_fat_cm3`
* `measurement.aggregate.subcutaneous_fat_cm3`
* `measurement.aggregate.torso_fat_cm3`
* `measurement.aggregate.torso_to_subcutaneous_ratio`
* `qc.coverage_complete`
* `qc.missing_levels`

`body_fat_l3_slice`:

* `measurement.slice_index`
* `measurement.subcutaneous_fat_area_cm2`
* `measurement.torso_fat_area_cm2`
* `measurement.torso_to_subcutaneous_ratio`

## Observacao arquitetural

Isto continua no lado Heimdallr: calcula biomarcadores estruturados e gera artefatos de validacao. A decisao de destacar, comparar longitudinalmente no laudo ou transformar isso em insight clinico fica para a camada superior.
