# Heimdallr

**Radiology preprocessing ecosystem for imaging intake, quantitative analysis, and assistive reporting**

Heimdallr is a production-oriented radiology pipeline that connects DICOM intake, study preparation, segmentation-driven analytics, and assistive reporting workflows. The repository combines a FastAPI application, a queue worker, a DICOM listener, and optional model-assist microservices.

## Current Scope

The repository currently implements three practical layers:

1. **Ingestion and preparation**
   - DICOM C-STORE listener for PACS/modality intake
   - ZIP upload through the web API and ZIP/folder submission through the CLI uploader
   - Study preparation, series selection, and DICOM-to-NIfTI conversion

2. **Quantitative imaging pipeline**
   - Queue-based processing from `input/` to `output/`
   - TotalSegmentator-backed segmentation and derived metrics
   - Structured case outputs in SQLite plus per-case artifact folders

3. **Assistive reporting services**
   - FastAPI dashboard and patient/result APIs
   - Proxy endpoints for AP chest X-ray assist flows
   - Optional standalone services for Anthropic, MedGemma, and CTR extraction

Future-facing modules are tracked in [`docs/UPCOMING.md`](docs/UPCOMING.md).

## Repository Layout

### Application and pipeline
- `app.py` - FastAPI entry point for uploads, patient APIs, proxy routes, and static dashboard serving
- `run.py` - background processing worker for segmentation and metrics
- `config.py` - centralized paths and runtime configuration
- `core/prepare.py` - upload preparation, DICOM parsing, series selection, and NIfTI conversion
- `core/metrics.py` - quantitative extraction and derived metric generation

### Services and clients
- `services/dicom_listener.py` - DICOM C-STORE SCP with idle-close and automatic upload
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
- `uploads/` - raw uploaded ZIP payloads
- `input/` - ready-to-process NIfTI queue
- `processing/` - claimed in-flight NIfTI jobs
- `nii/` - archived final NIfTI files
- `output/<case_id>/` - case artifacts such as `id.json`, `resultados.json`, PNG overlays, and segmentation folders
- `data/incoming_dicom/` - listener intake staging area

## Runtime Topology

```text
PACS / Modality (DICOM C-STORE)
            |
            v
services/dicom_listener.py
            |
            v
      POST /upload
            |
            v
          app.py
            |
            v
    core/prepare.py
 (DICOM select + NIfTI)
            |
            v
          input/
            |
            v
          run.py
            |
            v
   output/<case_id>/ + database/dicom.db + nii/

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
- TotalSegmentator-compatible environment and license key
- NVIDIA GPU recommended for segmentation and model-assist workloads
- Optional OCR review support: `pytesseract` package plus system `tesseract`

### Install

```bash
git clone <repository-url>
cd Heimdallr
python3 -m venv venv
source venv/bin/activate
venv/bin/pip install -r requirements.txt
cp .env.example .env
```

Set `TOTALSEGMENTATOR_LICENSE` in `.env`. `config.py` raises on import when that variable is missing.

Optional OCR dependency:

```bash
venv/bin/pip install pytesseract

# macOS
brew install tesseract

# Ubuntu/Debian
sudo apt-get update && sudo apt-get install -y tesseract-ocr
```

### Run

Run the baseline services in separate terminals:

```bash
# 1) API + dashboard
source venv/bin/activate
venv/bin/python app.py

# 2) Processing worker
source venv/bin/activate
venv/bin/python run.py

# 3) Optional: DICOM listener
source venv/bin/activate
venv/bin/python services/dicom_listener.py
```

Optional assistive services:

```bash
# Anthropic proxy target (default app proxy target: http://localhost:8101/analyze)
source venv/bin/activate
venv/bin/python api/anthropic.py

# MedGemma proxy target (default service port: 8004)
source venv/bin/activate
venv/bin/python api/medgemma.py

# CTR extraction service (default port 8003)
source venv/bin/activate
venv/bin/python api/ctr.py

# Alternative HTTP segmentation service (default port 8005)
source venv/bin/activate
venv/bin/python api/totalsegmentator.py
```

### Access

- Dashboard: `http://localhost:8001`
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

- Configuration is centralized in `config.py` and may be overridden with `HEIMDALLR_*` environment variables where supported.
- The DICOM listener default upload target is `http://127.0.0.1:8001/upload`.
- The CLI uploader lives at [`clients/uploader.py`](clients/uploader.py) and may also be downloaded from `GET /api/tools/uploader`.
- The worker claims files from `input/` into `processing/` before segmentation, then archives completed NIfTI files into `nii/`.
- The app proxy expects the Anthropic and MedGemma services to be running separately if those routes are used.
- `api/totalsegmentator.py` is an alternative HTTP execution path and is not part of the default `upload -> prepare -> run` baseline.

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
