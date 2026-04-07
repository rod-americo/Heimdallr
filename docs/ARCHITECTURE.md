# Architecture Overview

## Purpose

Heimdallr is an imaging operations pipeline designed to convert incoming radiology studies into structured outputs and operational insights.

## Runtime Components

1. `heimdallr/intake/`
- DICOM C-STORE intake service (`AE=HEIMDALLR`, default port `11114`)
- Owns the DICOM ingress gateway runtime

2. `heimdallr/control_plane/`
- FastAPI service for upload intake, dashboard data, and status endpoints
- App factory lives in `heimdallr/control_plane/app.py`

3. `heimdallr/prepare/`
- Validates uploaded study package
- Enumerates candidate series, converts DICOM to NIfTI (`dcm2niix`), and detects contrast phase
- Owns the study preparation worker runtime

4. `heimdallr/segmentation/`
- Claims prepared studies from `segmentation_queue`
- Selects the target series and executes the segmentation pipeline
- Writes outputs to `runtime/studies/<case_id>/` and updates database fields

5. `heimdallr/shared/`
- Shared settings, request dependencies, and schemas used across the control plane and workers

6. `heimdallr/metrics/`
- Consumes `metrics_queue` for post-segmentation derived jobs
- Runs modular jobs such as bone-health screening and vertebral fracture heuristics

7. `heimdallr/dicom_egress/`
- Consumes `dicom_egress_queue` for outbound artifact delivery
- Acts as DICOM SCU and performs C-STORE to configured remote SCP destinations

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
                                                segmentation_queue
                                                         |
                                                         v
                                                heimdallr.segmentation
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
                                               dicom_egress_queue
                                                         |
                                                         v
                                           heimdallr/dicom_egress
                                            (C-STORE outbound SCU)
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
- TotalSegmentator runtime in `.venv`
- Optional OCR: `pytesseract` + `tesseract` system binary

## Operational Boundaries

- Assistive outputs are operational results and require clinical validation before use.
- Production operation expects independent process supervision for the control plane, prepare worker, segmentation worker, metrics worker, intake gateway, and DICOM egress runtimes.

## Cross-References

- Operations runbook: `docs/OPERATIONS.md`
- API contracts: `docs/API.md`
- Validation stages: `docs/validation-stage-manual.md`
