# Architecture Overview

## Purpose

Heimdallr is an imaging operations pipeline designed to convert incoming radiology studies into structured outputs and operational insights.

## Runtime Components

The repository is currently transitioning from script-oriented entrypoints to a modular package layout under `heimdallr/`.

1. `heimdallr/intake/`
- DICOM C-STORE intake service (`AE=HEIMDALLR`, default port `11114`)
- Owns the DICOM ingress gateway runtime

2. `heimdallr/control_plane/`
- FastAPI service for upload intake, dashboard data, and status endpoints
- App factory lives in `heimdallr/control_plane/app.py`

3. `heimdallr/prepare/`
- Validates uploaded study package
- Selects target series and converts DICOM to NIfTI (`dcm2niix`)
- Owns the study preparation worker runtime

4. `heimdallr/processing/`
- Claims prepared studies from `processing_queue`
- Executes segmentation pipeline and baseline processing metrics
- Writes outputs to `runtime/studies/<case_id>/` and updates database fields

5. `heimdallr/shared/`
- Shared settings, request dependencies, and schemas used across the control plane and workers

6. `heimdallr/metrics/`
- Consumes `metrics_queue` for post-segmentation derived jobs
- Runs modular jobs such as bone-health screening and vertebral fracture heuristics

## Data and Storage

- Intake staging: `runtime/intake/uploads/`, `runtime/intake/uploads_failed/`
- DICOM ingress staging: `runtime/intake/dicom/`
- Queue state: `runtime/queue/pending/`, `runtime/queue/active/`, `runtime/queue/failed/`
- Study outputs and artifacts: `runtime/studies/<case_id>/`
- Database: `database/dicom.db` (`database/schema.sql`)

## Request/Data Flow

```text
PACS/Modality (DICOM) --> heimdallr/intake --> /upload (heimdallr/control_plane)
                                                         |
                                                         v
                                                   heimdallr/prepare
                                              (select + convert + persist)
                                                         |
                                                         v
                                                processing_queue
                                                         |
                                                         v
                                               heimdallr/processing
                                         (segment + baseline metrics + DB update)
                                                         |
                                                         v
                                                  metrics_queue
                                                         |
                                                         v
                                                heimdallr/metrics
                                         (derived jobs + job artifacts)
                                                         |
                                                         v
                                            runtime/studies/<case_id>/
                                                         |
                                                         v
                                                   Dashboard/API
```

## External Dependencies

- Python 3.10+
- `dcm2niix`
- TotalSegmentator runtime in `.venv-totalseg`
- Optional OCR: `pytesseract` + `tesseract` system binary

## Operational Boundaries

- Assistive outputs are operational results and require clinical validation before use.
- Production operation expects independent process supervision for the control plane, processing worker, and intake gateway runtimes.

## Cross-References

- Operations runbook: `docs/OPERATIONS.md`
- API contracts: `docs/API.md`
- Validation stages: `docs/validation-stage-manual.md`
- Strategic roadmap: `docs/UPCOMING.md`
