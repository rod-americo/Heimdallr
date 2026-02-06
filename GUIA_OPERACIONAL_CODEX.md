# Guia Operacional do Projeto Heimdallr (para futuras solicitacoes)

Este documento resume a arquitetura, os fluxos e os pontos de alteracao mais provaveis para acelerar futuras demandas sem precisar redescobrir o projeto.

## 1) Visao geral rapida

- Stack principal: Python + FastAPI + SQLite + frontend estatico (HTML/CSS/JS).
- Dominio: pipeline de radiologia (CT/MR) com segmentacao via TotalSegmentator.
- Entrada principal:
  - DICOM Listener (`dicom_listener.py`) via C-STORE (porta 11112 por padrao)
  - Upload manual (`uploader.py`) via HTTP `/upload`
- Orquestracao:
  - `server.py` recebe ZIP e dispara `prepare.py`
  - `prepare.py` converte DICOM->NIfTI e coloca em `input/`
  - `run.py` monitora `input/` e processa casos
  - `metrics.py` calcula metricas e gera `resultados.json`
- Saida por caso: `output/<CaseID>/` com `id.json`, `resultados.json`, mascaras e overlays.

## 2) Mapa de arquivos (o que editar em cada tipo de demanda)

- API/backend:
  - `server.py`: endpoints REST, downloads, biometria, SMI, dashboard root.
  - `config.py`: configuracoes centralizadas via env vars.
- Ingestao e preparo:
  - `dicom_listener.py`: recepcao DICOM, agrupamento por estudo, upload com retry.
  - `prepare.py`: selecao de serie, conversao NIfTI, metadados e insercao inicial no DB.
- Processamento:
  - `run.py`: daemon, execucao de tarefas TotalSegmentator, update de DB e arquivo final.
  - `metrics.py`: regras clinicas/metricas, overlays, deteccao de regioes.
- Frontend:
  - `static/index.html`, `static/styles.css`, `static/app.js`
- Banco:
  - `database/schema.sql` (schema base)
  - migracoes: `migrate_add_biometrics.py`, `migrate_add_smi.py`
  - utilitarios: `backfill_database.py`, `backfill_id_json.py`

## 3) Fluxo ponta a ponta (mental model)

1. Exame chega por DICOM Listener ou uploader manual.
2. `server.py:/upload` salva ZIP em `uploads/` e roda `prepare.py` em background.
3. `prepare.py`:
   - extrai ZIP
   - le DICOMs e escolhe a melhor serie (logica diferente para CT e MR)
   - converte com `dcm2niix`
   - grava `output/<case>/id.json`
   - copia NIfTI final para `input/<case>.nii.gz`
   - atualiza/inicializa `database/dicom.db`
4. `run.py` detecta NIfTI novo em `input/` e processa:
   - segmentacao (`total`/`total_mr`, `tissue_types`)
   - opcional: `cerebral_bleed` se cerebro detectado (CT)
   - metricas via `metrics.py`
   - atualiza DB (`CalculationResults`, `IdJson`, biometria se houver)
   - move NIfTI para `nii/` (arquivo final)
5. Dashboard consulta `server.py` para lista de pacientes, resultados, imagens e downloads.

## 4) Contratos de dados importantes

- `id.json`:
  - metadados do estudo, `CaseID`, `ClinicalName`, `Pipeline`, `SelectedSeries`
- `resultados.json`:
  - `modality`, `body_regions`, volumetria de orgaos, HU (CT), sarcopenia (L3), hemorragia
- DB `dicom_metadata` (chave = `StudyInstanceUID`):
  - campos principais: `IdJson`, `DicomMetadata`, `CalculationResults`, `Weight`, `Height`, `SMI`

## 5) Endpoints relevantes

- `GET /api/patients`
- `GET /api/patients/{case_id}/results`
- `GET /api/patients/{case_id}/metadata`
- `GET /api/patients/{case_id}/nifti`
- `GET /api/patients/{case_id}/download/{folder_name}` (`bleed`, `tissue_types`, `total`)
- `GET /api/patients/{case_id}/images/{filename}`
- `PATCH /api/patients/{case_id}/biometrics`
- `PATCH /api/patients/{case_id}/smi`
- `POST /upload`

## 6) Dependencias e requisitos operacionais

- Python 3.10+
- `dcm2niix` instalado no host
- `TotalSegmentator` + chave `TOTALSEGMENTATOR_LICENSE` em `.env`
- GPU CUDA recomendada para performance

## 7) Pontos de atencao (riscos tecnicos)

- `config.py` exige `TOTALSEGMENTATOR_LICENSE`; sem isso processos falham ao importar config.
- `prepare.py` faz `sys.exit(1)` em varios erros: impacto direto no fluxo de ingestao.
- `run.py` paraleliza casos; erros por corrida de `config.json` do TotalSegmentator ja tem retry.
- Frontend calcula e envia SMI apos salvar peso/altura; depende de SMA existir no resultado.
- Schema SQL base nao inclui `Weight/Height/SMI`; requer migracoes para ambientes antigos.

## 8) Playbook rapido por tipo de solicitacao

- "Criar/ajustar endpoint":
  - editar `server.py` + validar impacto em `static/app.js` (se UI usar).
- "Mudar criterio de selecao de serie":
  - editar `prepare.py` (blocos CT/MR e score).
- "Nova metrica clinica":
  - editar `metrics.py`, persistencia segue por `run.py`.
- "Ajuste de pipeline/execucao":
  - editar `run.py` (ordem de tarefas, retries, concorrencia).
- "Melhoria PACS/DICOM":
  - editar `dicom_listener.py` e configs em `config.py`.
- "Mudanca de UI":
  - editar `static/index.html`, `static/styles.css`, `static/app.js`.

## 9) Como subir localmente (baseline)

Em terminais separados:

1. `source venv/bin/activate && python server.py`
2. `source venv/bin/activate && python run.py`
3. (Opcional PACS) `source venv/bin/activate && python dicom_listener.py`

Dashboard: `http://localhost:8001`  
Docs API: `http://localhost:8001/docs`

## 10) Checklist antes de qualquer mudanca futura

- Confirmar impacto em:
  - API (`server.py`)
  - pipeline (`prepare.py`/`run.py`)
  - persistencia (`database/dicom.db`)
  - dashboard (`static/app.js`)
- Verificar compatibilidade CT vs MR.
- Validar que `id.json` e `resultados.json` permanecem consistentes.
- Evitar quebrar fluxo de ingestao automatica (DICOM Listener -> upload -> prepare -> run).

