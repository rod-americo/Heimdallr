# Heimdallr Operational Guide for Future Maintenance

This document summarizes the architecture, flows, and most likely edit points so future work can start from an accurate repository map instead of rediscovering the codebase.

## 1. Quick Overview

- Primary stack: Python, FastAPI, SQLite, and a static HTML/CSS/JS frontend.
- Domain: radiology pipeline with CT/MR preprocessing and TotalSegmentator-based analysis.
- Main intake paths:
  - DICOM listener runtime at [`heimdallr/intake/gateway.py`](../heimdallr/intake/gateway.py) via C-STORE on port `11114` by default
  - Manual upload through [`clients/uploader.py`](../clients/uploader.py) or `POST /upload`
- Orchestration:
  - [`heimdallr/control_plane/app.py`](../heimdallr/control_plane/app.py) receives ZIP uploads into `runtime/intake/uploads/`
  - [`heimdallr/prepare/worker.py`](../heimdallr/prepare/worker.py) converts DICOM to NIfTI and persists prepared studies under `runtime/studies/`
  - [`heimdallr/processing/worker.py`](../heimdallr/processing/worker.py) monitors the processing queue and runs segmentation
  - [`heimdallr/metrics/worker.py`](../heimdallr/metrics/worker.py) executes post-segmentation derived metrics jobs
- Per-case outputs now live in `runtime/studies/<CaseID>/` and typically include `metadata/id.json`, `metadata/resultados.json`, `artifacts/`, `derived/`, and `logs/`.

## 2. File Map

### API and backend
- [`app.py`](../app.py): compatibility wrapper for the control plane package
- [`heimdallr/control_plane/app.py`](../heimdallr/control_plane/app.py): app factory, router wiring, and static serving
- [`heimdallr/control_plane/routers/upload.py`](../heimdallr/control_plane/routers/upload.py): `POST /upload`
- [`heimdallr/control_plane/routers/patients.py`](../heimdallr/control_plane/routers/patients.py): patient/result/download endpoints
- [`heimdallr/control_plane/routers/proxy.py`](../heimdallr/control_plane/routers/proxy.py): proxy routes for assistive X-ray services
- [`heimdallr/shared/settings.py`](../heimdallr/shared/settings.py): centralized runtime settings, paths, and binary resolution

### Intake and preparation
- [`heimdallr/intake/gateway.py`](../heimdallr/intake/gateway.py): DICOM reception, study grouping, idle close, and upload/spool handoff
- [`heimdallr/prepare/worker.py`](../heimdallr/prepare/worker.py): series discovery, DICOM-to-NIfTI conversion, metadata persistence, queue creation

### Processing
- [`run.py`](../run.py): compatibility wrapper for the processing runtime
- [`heimdallr/processing/worker.py`](../heimdallr/processing/worker.py): segmentation worker, queue claiming, archival, and metrics enqueue
- [`heimdallr/metrics/worker.py`](../heimdallr/metrics/worker.py): post-segmentation metrics worker
- [`core/metrics.py`](../core/metrics.py): legacy monolithic metric implementation still used by transitional flows

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
2. `POST /upload` stores the ZIP in `runtime/intake/uploads/` for the prepare worker.
3. `heimdallr/prepare/worker.py`:
   - extracts the ZIP
   - reads DICOM files and selects the best series
   - converts the selected series with `dcm2niix`
   - writes `runtime/studies/<case>/metadata/id.json`
   - updates the processing queue in SQLite
   - inserts or updates metadata in `database/dicom.db`
4. `heimdallr/processing/worker.py` claims the queued study and executes:
   - segmentation tasks such as `total` and `tissue_types`
   - archival of the canonical NIfTI into `runtime/studies/<case>/derived/`
   - enqueue of the post-segmentation metrics stage
5. `heimdallr/metrics/worker.py` runs the configured metrics jobs and merges their outputs into `metadata/resultados.json`.
6. The dashboard and patient APIs read from SQLite and the per-case runtime folders.

## 4. Important Data Contracts

### `id.json`
- Study metadata
- `CaseID`
- `ClinicalName`
- `Pipeline`
- `AvailableSeries`
- `Pipeline.series_selection`

### `resultados.json`
- `modality`
- `body_regions`
- organ volumetry
- density metrics for CT workflows
- sarcopenia-related outputs
- hemorrhage-related outputs when applicable
- `metrics.<job_name>` entries for post-segmentation derived jobs

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
- `dcm2niix` available either from the host or the bundled `bin/`
- TotalSegmentator runtime available through the dedicated `.venv-totalseg`
- CUDA-capable GPU recommended for throughput
- Optional OCR support: `pytesseract` plus system `tesseract`
- Operational UI support: `rich` and `textual` in the service environment

## 7. Attention Points and Technical Risks

- The repository is in a live migration from flat scripts to the `heimdallr/` package layout; some legacy entrypoints still exist as wrappers.
- The operational `.venv` and the scientific `.venv-totalseg` have different dependency surfaces; do not assume scientific modules exist in the operational environment.
- [`heimdallr/prepare/worker.py`](../heimdallr/prepare/worker.py) remains a critical handoff point for intake and queue consistency.
- [`heimdallr/processing/worker.py`](../heimdallr/processing/worker.py) processes multiple cases in parallel and coordinates queue state with SQLite.
- Frontend biometric workflows depend on the patient endpoints and the presence of the relevant metrics in `resultados.json`.
- Older environments may need schema migration for `Weight`, `Height`, `SMI`, and the new `metrics_queue`.
- The app proxy configuration and the MedGemma service default port are not aligned by default; verify environment overrides before enabling that flow.

## 8. Quick Playbook by Request Type

- "Create or adjust an endpoint":
  - edit the relevant router under `heimdallr/control_plane/routers/`
- "Change series selection criteria":
  - edit [`config/series_selection.json`](../config/series_selection.json) and, when needed, [`heimdallr/prepare/worker.py`](../heimdallr/prepare/worker.py)
- "Add a clinical metric":
  - edit the metric job under `heimdallr/metrics/jobs/` and its helpers under `heimdallr/processing/`
- "Adjust pipeline execution":
  - edit [`heimdallr/processing/worker.py`](../heimdallr/processing/worker.py) and the JSON pipeline configs under `config/`
- "Improve PACS or DICOM intake":
  - edit [`heimdallr/intake/gateway.py`](../heimdallr/intake/gateway.py) and [`heimdallr/shared/settings.py`](../heimdallr/shared/settings.py)
- "Change UI behavior":
  - edit files under `static/`

## 9. Local Startup Baseline

In separate terminals:

1. `source .venv/bin/activate && .venv/bin/python -m heimdallr.control_plane`
2. `source .venv/bin/activate && .venv/bin/python -m heimdallr.prepare`
3. `source .venv/bin/activate && .venv/bin/python -m heimdallr.processing`
4. `source .venv/bin/activate && .venv/bin/python -m heimdallr.metrics`
5. `source .venv/bin/activate && .venv/bin/python -m heimdallr.intake`
6. `source .venv/bin/activate && .venv/bin/python -m heimdallr.tui`

Optional:

7. `source .venv/bin/activate && .venv/bin/python api/anthropic.py`
8. `source .venv/bin/activate && .venv/bin/python api/medgemma.py`
9. `source .venv/bin/activate && .venv/bin/python api/ctr.py`

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
  `intake -> /upload -> prepare -> processing -> metrics`
