# Heimdallr

**Radiology preprocessing ecosystem for imaging intake, quantitative analysis, and assistive reporting**

Heimdallr is a production-oriented radiology pipeline that connects DICOM intake, study preparation, segmentation-driven analytics, and assistive reporting workflows. The repository is now being reorganized around a modular package layout that separates the operational control plane, intake, preparation, processing, and shared platform concerns.

## Current Scope

The repository currently implements three practical layers:

1. **Ingestion and preparation**
   - DICOM C-STORE listener for PACS/modality intake
   - ZIP upload through the web API and ZIP/folder submission through the CLI uploader
   - Study preparation, series selection, and DICOM-to-NIfTI conversion

2. **Quantitative imaging pipeline**
   - Queue-based processing from the runtime queue into per-study runtime folders
   - TotalSegmentator-backed segmentation and derived metrics
   - Structured case outputs in SQLite plus per-case artifact folders

3. **Assistive reporting services**
   - FastAPI dashboard and patient/result APIs
   - Proxy endpoints for AP chest X-ray assist flows
   - Optional standalone services for Anthropic, MedGemma, and CTR extraction

Future-facing modules are tracked in [`docs/UPCOMING.md`](docs/UPCOMING.md).

## Repository Layout

### Modular package
- `heimdallr/control_plane/` - operational API and dashboard app factory plus routers
- `heimdallr/intake/` - intake gateway entrypoints for DICOM ingress
- `heimdallr/prepare/` - study preparation worker entrypoints
- `heimdallr/processing/` - background processing worker runtime
- `heimdallr/shared/` - shared settings, dependencies, and schemas

### Transitional entrypoints
- `app.py` - top-level wrapper around `heimdallr.control_plane.app`
- `run.py` - top-level wrapper around `heimdallr.processing.worker`
- `core/metrics.py` - quantitative extraction and derived metric generation

### Services and clients
- `heimdallr/metrics/` - resident metrics worker plus standalone job modules
- `services/dicom_listener.py` - legacy listener path superseded by `heimdallr/intake/`
- `services/deid_gateway.py` - outbound de-identification controls for external model calls
- `services/anthropic_report_builder.py` - narrative/report structuring helpers
- `clients/uploader.py` - CLI uploader for ZIP files or DICOM folders

### Optional microservices
- `api/anthropic.py` - Anthropic-backed AP chest X-ray analysis service
- `api/medgemma.py` - MedGemma-backed AP chest X-ray analysis service
- `api/ctr.py` - CTR extraction service based on CXAS
- `api/totalsegmentator.py` - alternative HTTP processing service for segmentation and metrics

### Storage and outputs
- `database/schema.sql` - SQLite schema
- `runtime/intake/uploads/` - raw uploaded ZIP payloads waiting for prepare
- `runtime/queue/pending/` - queued processing inputs
- `runtime/queue/active/` - claimed in-flight processing inputs
- `runtime/queue/failed/` - failed processing inputs
- `runtime/studies/<case_id>/` - case artifacts such as `metadata/id.json`, `metadata/resultados.json`, `artifacts/`, `derived/`, and `logs/`
- `runtime/intake/dicom/incoming/` - listener intake staging area

## Runtime Topology

```text
PACS / Modality (DICOM C-STORE)
            |
            v
heimdallr.intake
            |
            v
      POST /upload
            |
            v
heimdallr.control_plane
            |
            v
heimdallr.prepare
 (DICOM select + NIfTI)
            |
            v
 runtime/queue/pending
            |
            v
heimdallr.processing
            |
            v
heimdallr.metrics
            |
            v
 runtime/studies/<case_id> + database/dicom.db

Optional app.py proxy routes:
  /api/anthropic/ap-thorax-xray -> api/anthropic.py
  /api/medgemma/ap-thorax-xray  -> api/medgemma.py

Standalone optional service:
  api/ctr.py -> POST /extract_ctr
  api/totalsegmentator.py -> POST /process
```

## Quick Start

### Prerequisites

- Python `3.10+`
- `dcm2niix`
- TotalSegmentator-compatible environment
- TotalSegmentator license registered in the dedicated `.venv-totalseg` when licensed tasks are enabled
- NVIDIA GPU recommended for segmentation and model-assist workloads
- Optional OCR review support: `pytesseract` package plus system `tesseract`

### Install

```bash
git clone <repository-url>
cd Heimdallr
python3 -m venv .venv
source .venv/bin/activate
.venv/bin/pip install -r requirements.txt
cp .env.example .env
```

For licensed TotalSegmentator tasks such as `tissue_types`, register the license once in the dedicated TotalSegmentator environment instead of exposing it in the service command line:

```bash
/path/to/Heimdallr/.venv-totalseg/bin/totalseg_set_license -l YOUR_LICENSE_KEY
```

This is the preferred deployment model because the license is stored in the TotalSegmentator runtime itself and does not need to appear in `systemd`/`skuld` service definitions.

Optional OCR dependency:

```bash
.venv/bin/pip install pytesseract

# macOS
brew install tesseract

# Ubuntu/Debian
sudo apt-get update && sudo apt-get install -y tesseract-ocr
```

### Run

Run the baseline services in separate terminals:

```bash
# 1) API + dashboard
source .venv/bin/activate
.venv/bin/python -m heimdallr.control_plane

# 2) Processing worker
source .venv/bin/activate
.venv/bin/python -m heimdallr.processing

# 3) Optional: DICOM listener
source .venv/bin/activate
.venv/bin/python -m heimdallr.intake

# 4) Operations TUI
source .venv/bin/activate
.venv/bin/python -m heimdallr.tui
```

Optional assistive services:

```bash
# Anthropic proxy target (default app proxy target: http://localhost:8101/analyze)
source .venv/bin/activate
.venv/bin/python api/anthropic.py

# MedGemma proxy target (default service port: 8004)
source .venv/bin/activate
.venv/bin/python api/medgemma.py

# CTR extraction service (default port 8003)
source .venv/bin/activate
.venv/bin/python api/ctr.py

# Alternative HTTP segmentation service (default port 8005)
source .venv/bin/activate
.venv/bin/python api/totalsegmentator.py
```

The top-level commands `.venv/bin/python app.py` and `.venv/bin/python run.py` continue to resolve to the package runtimes, but the package modules under `heimdallr/` are now the primary execution surface.

The TUI refreshes automatically and reads directly from `runtime/`, `database/dicom.db`, and the host process table. Use `q` to quit, `r` to force refresh, and `p` to pause live updates.

### Access

- Dashboard: `http://localhost:8001`
- TUI dashboard: `.venv/bin/python -m heimdallr.tui`
- OpenAPI docs: `http://localhost:8001/docs`
- CTR service health: `http://localhost:8003/health`
- TotalSegmentator API health: `http://localhost:8005/health`

## Current API Surface

### Upload and dashboard
- `POST /upload`
- `GET /`
- `GET /api/tools/uploader`

### Patients and results
- `GET /api/patients`
- `GET /api/patients/{case_id}/results`
- `GET /api/patients/{case_id}/metadata`
- `GET /api/patients/{case_id}/nifti`
- `GET /api/patients/{case_id}/download/{folder_name}`
- `GET /api/patients/{case_id}/images/{filename}`
- `PATCH /api/patients/{case_id}/biometrics`
- `PATCH /api/patients/{case_id}/smi`

### Assistive report proxies
- `POST /api/anthropic/ap-thorax-xray`
- `POST /api/medgemma/ap-thorax-xray`

### Standalone optional service
- CTR service (`:8003`): `POST /extract_ctr`, `GET /health`
- TotalSegmentator HTTP service (`:8005`): `POST /process`, `GET /health`

See [`docs/API.md`](docs/API.md) for contract notes and examples.

## Operational Notes

- Runtime configuration is centralized in `heimdallr/shared/settings.py` plus the JSON profiles under `config/`, and may be overridden with `HEIMDALLR_*` environment variables where supported.
- The DICOM listener default upload target is `http://127.0.0.1:8001/upload`.
- The CLI uploader lives at [`clients/uploader.py`](clients/uploader.py) and may also be downloaded from `GET /api/tools/uploader`.
- The processing worker claims studies from `runtime/queue/pending/` into `runtime/queue/active/` before segmentation and persists outputs under `runtime/studies/<case_id>/`.
- Retroactive metrics regeneration is available through `venv/bin/python scripts/retroactive_recalculate_metrics.py` with options such as `--case`, `--limit`, `--workers`, and `--skip-overlays`.
- The app proxy expects the Anthropic and MedGemma services to be running separately if those routes are used.
- `api/totalsegmentator.py` is an alternative HTTP execution path and is not part of the default `upload -> prepare -> processing -> metrics` baseline.

## Documentation Map

- Architecture overview: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- API contracts: [`docs/API.md`](docs/API.md)
- Operations runbook: [`docs/OPERATIONS.md`](docs/OPERATIONS.md)
- Validation stage manual: [`docs/validation-stage-manual.md`](docs/validation-stage-manual.md)
- Roadmap and future modules: [`docs/UPCOMING.md`](docs/UPCOMING.md)

## Governance and Compliance

- Security policy: [`SECURITY.md`](SECURITY.md)
- Contributing guide: [`CONTRIBUTING.md`](CONTRIBUTING.md)
- Change history: [`CHANGELOG.md`](CHANGELOG.md)
- Ownership map: [`CODEOWNERS`](CODEOWNERS)
- Notices and third-party attribution: [`NOTICE`](NOTICE)

Heimdallr is distributed under Apache License 2.0. Third-party components may carry additional licensing or use restrictions, especially TotalSegmentator, CXAS, Anthropic-backed services, and MedGemma-backed services. Review deployment obligations before production use.
