# Architecture Overview

## Purpose

Heimdallr is an imaging operations pipeline designed to convert incoming radiology studies into structured outputs and assistive reporting artifacts.

## Runtime Components

1. `dicom_listener.py`
- DICOM C-STORE intake service (`AE=HEIMDALLR`, default port `11112`)
- Groups incoming files by study and forwards packaged data to upload endpoint

2. `app.py`
- FastAPI service for upload intake, dashboard data, and assistive-report endpoints
- Serves static frontend from `static/`

3. `core/prepare.py`
- Validates uploaded study package
- Selects target series and converts DICOM to NIfTI (`dcm2niix`)
- Persists initial metadata to SQLite

4. `run.py`
- Watches `input/` queue for NIfTI jobs
- Executes segmentation pipeline and derived metrics extraction
- Writes outputs to `output/<case_id>/` and updates database fields

5. Optional model-assist services
- `api/medgemma.py`
- Anthropic flow through server endpoints plus outbound de-identification gateway (`services/deid_gateway.py`)

6. `api/ctr.py`
   - CTR (Cardiothoracic Ratio / ICT) extraction microservice (default port `8003`)
   - Based on ChestXRayAnatomySegmentation (CXAS) by Constantin Seibold et al. (CC BY-NC-SA 4.0)
   - Loads CXAS UNet_ResNet50 model at startup, exposes `POST /extract_ctr`

## Data and Storage

- Queue/input: `input/`
- Final NIfTI archive: `nii/`
- Outputs and artifacts: `output/<case_id>/`
- Errors: `errors/`
- Raw intake: `uploads/`, `data/incoming_dicom/`
- Database: `database/dicom.db` (`database/schema.sql`)

## Request/Data Flow

```text
PACS/Modality (DICOM) --> dicom_listener.py --> POST /upload (app.py)
                                               |
                                               v
                                         core/prepare.py
                                    (select + convert + persist)
                                               |
                                               v
                                             input/
                                               |
                                               v
                                             run.py
                                 (segment + metrics + DB update)
                                               |
                                               v
                                       output/<case_id>/
                                               |
                               +---------------+----------------+
                               |                                |
                               v                                v
                         Dashboard/API                      Assistive report APIs
```

## External Dependencies

- Python 3.10+
- `dcm2niix`
- TotalSegmentator license (`TOTALSEGMENTATOR_LICENSE`)
- CXAS (ChestXRayAnatomySegmentation) — CC BY-NC-SA 4.0, for CTR extraction
- Optional OCR: `pytesseract` + `tesseract` system binary

## Operational Boundaries

- Assistive outputs are non-autonomous and require qualified reviewer validation.
- External model calls should traverse de-identification controls.
- Production operation expects independent process supervision for `app.py`, `run.py`, and `dicom_listener.py`.

## Cross-References

- Operations runbook: `docs/OPERATIONS.md`
- API contracts: `docs/API.md`
- Validation stages: `docs/validation-stage-manual.md`
- Strategic roadmap: `docs/UPCOMING.md`
