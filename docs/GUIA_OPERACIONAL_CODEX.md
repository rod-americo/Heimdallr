# Heimdallr Operational Guide for Future Maintenance

This document summarizes the architecture, flows, and most likely edit points so future work can start from an accurate repository map instead of rediscovering the codebase.

## 1. Quick Overview

- Primary stack: Python, FastAPI, SQLite, and a static HTML/CSS/JS frontend.
- Domain: radiology pipeline with CT/MR preprocessing and TotalSegmentator-based analysis.
- Main intake paths:
  - DICOM listener at [`services/dicom_listener.py`](../services/dicom_listener.py) via C-STORE on port `11112` by default
  - Manual upload through [`clients/uploader.py`](../clients/uploader.py) or `POST /upload`
- Orchestration:
  - [`app.py`](../app.py) receives ZIP uploads and launches [`core/prepare.py`](../core/prepare.py)
  - [`core/prepare.py`](../core/prepare.py) converts DICOM to NIfTI and places jobs in `input/`
  - [`run.py`](../run.py) monitors `input/` and processes claimed jobs from `processing/`
  - [`core/metrics.py`](../core/metrics.py) computes derived metrics and writes `resultados.json`
- Per-case outputs live in `output/<CaseID>/` and typically include `id.json`, `resultados.json`, masks, logs, and overlays.

## 2. File Map

### API and backend
- [`app.py`](../app.py): upload entry point, REST endpoints, dashboard root, and static serving
- [`config.py`](../config.py): centralized configuration and environment variable handling
- [`api/routes/upload.py`](../api/routes/upload.py): `POST /upload`
- [`api/routes/patients.py`](../api/routes/patients.py): patient/result/download endpoints
- [`api/routes/proxy.py`](../api/routes/proxy.py): proxy routes for assistive X-ray services

### Intake and preparation
- [`services/dicom_listener.py`](../services/dicom_listener.py): DICOM reception, study grouping, idle close, ZIP upload with retry
- [`core/prepare.py`](../core/prepare.py): series selection, DICOM-to-NIfTI conversion, metadata persistence, queue creation

### Processing
- [`run.py`](../run.py): background worker, TotalSegmentator task execution, retry handling, archival
- [`core/metrics.py`](../core/metrics.py): clinical and quantitative metric logic, JSON output generation

### Frontend
- `static/index.html`
- `static/styles.css`
- `static/js/`

### Database
- [`database/schema.sql`](../database/schema.sql): base schema
- `database/dicom.db`: live SQLite file
- [`database/README.md`](../database/README.md): schema and maintenance notes

## 3. End-to-End Flow

1. A study arrives through the DICOM listener or the manual upload path.
2. `POST /upload` stores the ZIP in `uploads/` and launches `core/prepare.py` in the background.
3. `core/prepare.py`:
   - extracts the ZIP
   - reads DICOM files and selects the best series
   - converts the selected series with `dcm2niix`
   - writes `output/<case>/id.json`
   - places the final NIfTI at `input/<case>.nii.gz`
   - inserts or updates metadata in `database/dicom.db`
4. `run.py` claims the NIfTI into `processing/` and executes:
   - segmentation tasks such as `total`, `tissue_types`, and conditional `cerebral_bleed`
   - metric generation through `core/metrics.py`
   - SQLite updates (`CalculationResults`, `IdJson`, biometrics when available)
   - final archive move into `nii/`
5. The dashboard and patient APIs read from SQLite and per-case output folders.

## 4. Important Data Contracts

### `id.json`
- Study metadata
- `CaseID`
- `ClinicalName`
- `Pipeline`
- `SelectedSeries`

### `resultados.json`
- `modality`
- `body_regions`
- organ volumetry
- density metrics for CT workflows
- sarcopenia-related outputs
- hemorrhage-related outputs when applicable

### SQLite table `dicom_metadata`
- key: `StudyInstanceUID`
- common fields: `IdJson`, `DicomMetadata`, `CalculationResults`, `Weight`, `Height`, `SMI`

## 5. Relevant Endpoints

- `GET /api/patients`
- `GET /api/patients/{case_id}/results`
- `GET /api/patients/{case_id}/metadata`
- `GET /api/patients/{case_id}/nifti`
- `GET /api/patients/{case_id}/download/{folder_name}`
- `GET /api/patients/{case_id}/images/{filename}`
- `PATCH /api/patients/{case_id}/biometrics`
- `PATCH /api/patients/{case_id}/smi`
- `POST /upload`
- `POST /api/anthropic/ap-thorax-xray`
- `POST /api/medgemma/ap-thorax-xray`

## 6. Dependencies and Operational Requirements

- Python `3.10+`
- `dcm2niix` installed on the host
- `TOTALSEGMENTATOR_LICENSE` set in `.env`
- CUDA-capable GPU recommended for throughput
- Optional OCR support: `pytesseract` plus system `tesseract`

## 7. Attention Points and Technical Risks

- [`config.py`](../config.py) requires `TOTALSEGMENTATOR_LICENSE`; imports fail without it.
- [`core/prepare.py`](../core/prepare.py) exits hard on several preparation failures, which directly affects intake.
- [`run.py`](../run.py) processes multiple cases in parallel and contains retry logic for TotalSegmentator config race conditions.
- Frontend biometric workflows depend on the patient endpoints and the presence of the relevant metrics in `resultados.json`.
- Older environments may need schema migration for `Weight`, `Height`, and `SMI` fields.
- The app proxy configuration and the MedGemma service default port are not aligned by default; verify environment overrides before enabling that flow.

## 8. Quick Playbook by Request Type

- "Create or adjust an endpoint":
  - edit [`app.py`](../app.py) and the relevant router under `api/routes/`
- "Change series selection criteria":
  - edit [`core/prepare.py`](../core/prepare.py)
- "Add a clinical metric":
  - edit [`core/metrics.py`](../core/metrics.py); `run.py` handles persistence flow
- "Adjust pipeline execution":
  - edit [`run.py`](../run.py)
- "Improve PACS or DICOM intake":
  - edit [`services/dicom_listener.py`](../services/dicom_listener.py) and [`config.py`](../config.py)
- "Change UI behavior":
  - edit files under `static/`

## 9. Local Startup Baseline

In separate terminals:

1. `source venv/bin/activate && venv/bin/python app.py`
2. `source venv/bin/activate && venv/bin/python run.py`
3. `source venv/bin/activate && venv/bin/python services/dicom_listener.py`

Optional:

4. `source venv/bin/activate && venv/bin/python api/anthropic.py`
5. `source venv/bin/activate && venv/bin/python api/medgemma.py`
6. `source venv/bin/activate && venv/bin/python api/ctr.py`

Dashboard: `http://localhost:8001`  
API docs: `http://localhost:8001/docs`

## 10. Checklist Before Future Changes

- Confirm impact on:
  - API routes
  - pipeline preparation and worker flow
  - SQLite persistence
  - dashboard behavior
- Verify compatibility across CT and MR paths when relevant.
- Confirm that `id.json` and `resultados.json` remain consistent.
- Avoid breaking the automated intake chain:
  `DICOM listener -> /upload -> prepare -> run`
