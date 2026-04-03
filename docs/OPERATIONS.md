# Operations Runbook

This document provides a baseline for operating Heimdallr in production-like environments.

## Service Topology

Run as independent services:

1. `python -m heimdallr.control_plane` (API, dashboard, upload endpoints)
2. `python -m heimdallr.prepare` (study preparation watchdog)
3. `python -m heimdallr.processing` (segmentation/processing worker)
4. `python -m heimdallr.metrics` (post-segmentation derived metrics worker)
5. `python -m heimdallr.intake` (DICOM C-STORE intake)
6. `python -m heimdallr.tui` (optional operational dashboard)

## Repository File Map

### Core Package (`heimdallr/`)
- `control_plane/`: FastAPI application, routers, and static serving.
- `intake/gateway.py`: DICOM reception and study spooling.
- `prepare/worker.py`: Series discovery and DICOM-to-NIfTI conversion.
- `processing/worker.py`: Core segmentation worker logic.
- `metrics/worker.py`: Post-segmentation derived metrics execution.
- `shared/settings.py`: Centralized runtime settings and binary paths.

### Configuration and Data
- `config/`: Pipeline JSON profiles and series selection rules.
- `database/schema.sql`: SQLite schema for state management.
- `static/`: Frontend dashboard assets.


## Baseline Startup

```bash
# API + Dashboard
source venv/bin/activate
python -m heimdallr.control_plane

# Prepare worker
source venv/bin/activate
python -m heimdallr.prepare

# Processing worker
source venv/bin/activate
python -m heimdallr.processing

# Metrics worker
source venv/bin/activate
python -m heimdallr.metrics

# DICOM listener
source venv/bin/activate
python -m heimdallr.intake
```

## Environment and Config

Configuration is centralized in `heimdallr/shared/settings.py` plus the pipeline JSON profiles under `config/`, and can be overridden via `HEIMDALLR_*` environment variables.

Common examples:

```bash
export HEIMDALLR_AE_TITLE="HEIMDALLR"
export HEIMDALLR_DICOM_PORT="11114"
export HEIMDALLR_IDLE_SECONDS="30"
```

## End-to-End Pipeline Flow

1. **Intake**: Study arrives via DICOM C-STORE (`intake`) or manual `POST /upload`.
2. **Spooling**: Files are stored in `runtime/intake/uploads/`.
3. **Preparation**: `prepare` worker extracts ZIP, selects the best series using `config/series_selection.json`, converts to NIfTI via `dcm2niix`, and creates `metadata/id.json`.
4. **Processing**: `processing` worker claims the case, runs segmentation (e.g., TotalSegmentator), and archives results in `runtime/studies/<CaseID>/derived/`.
5. **Metrics**: `metrics` worker executes derived calculations (volumetry, density) and updates `metadata/resultados.json`.
6. **Delivery**: Dashboard and APIs serve the final structured data and images.

## Data Contracts

### `metadata/id.json`
Contains study-level metadata, CaseID, and series selection audit trail.

### `metadata/resultados.json`
The primary output for clinical consumption. Includes:
- `modality` and `body_regions`.
- Organ volumetry and quantification.
- Derived metrics (attenuation, indices).
- Metadata for post-processing jobs.

### SQLite (`database/dicom.db`)
Central state for all studies, tracking pipeline stage, patient demographics, and calculated summaries.


## OCR De-identification Dependency

For OCR-driven de-identification review before external model calls:

1. Install Python package: `pip install pytesseract`
2. Install system binary: `tesseract` (`brew install tesseract` or `apt-get install tesseract-ocr`)
3. Default behavior: `DEID_OCR_ACTION=block` (external call is blocked when text is detected)

If OCR dependencies are not installed, the gateway reports `ocr_available=false` in `deid` telemetry.

## PACS Connectivity Checks

Expected defaults:

- AE Title: `HEIMDALLR`
- Port: `11114`
- Protocol: DICOM C-STORE

Quick smoke test using DCMTK:

```bash
dcmsend localhost 11114 -aec HEIMDALLR test.dcm
```

## Health and Monitoring Checks

1. `http://localhost:8001/docs` responds.
2. Listener accepts inbound C-STORE on port `11114`.
3. Queue path `upload -> prepare -> processing -> metrics` completes for a known study.
4. GPU capacity is available for segmentation processing.

## Backup and Restore (SQLite)

Backup example:

```bash
cp database/dicom.db database/dicom_backup_$(date +%Y%m%d_%H%M%S).db
```

Restore example:

```bash
cp database/dicom_backup_<timestamp>.db database/dicom.db
```

Integrity check:

```bash
sqlite3 database/dicom.db "PRAGMA integrity_check;"
```

## Retroactive Recalculation

Use the batch recalculation script when derived metrics need to be regenerated from existing case outputs:

```bash
source venv/bin/activate
venv/bin/python scripts/retroactive_recalculate_metrics.py --limit 10
```

Common variants:

- Skip PNG regeneration: `venv/bin/python scripts/retroactive_recalculate_metrics.py --skip-overlays`
- Process a specific case: `venv/bin/python scripts/retroactive_recalculate_metrics.py --case <case_id>`
- Parallelize cautiously: `venv/bin/python scripts/retroactive_recalculate_metrics.py --workers 2`

Legacy compatibility wrapper:

```bash
venv/bin/python scripts/retroactive_emphysema.py
```

## Incident Triage Shortlist

1. Validate service process state and restart order (`control_plane -> prepare -> processing -> metrics -> intake`).
2. Check PACS destination configuration and network reachability.
3. Inspect `runtime/intake/` and `runtime/studies/` for stuck or failed studies.
4. Confirm data storage permissions for the `runtime/` directory.

## Maintenance Playbook

- **Adjusting endpoints**: Edit the relevant router under `heimdallr/control_plane/routers/`.
- **Changing series selection**: Update `config/series_selection.json`.
- **Adding a clinical metric**: Add a job under `heimdallr/metrics/jobs/` and register it in the pipeline config.
- **Improving intake logic**: Edit `heimdallr/intake/gateway.py`.

## Checklist Before Changes
- Confirm impact on `id.json` and `resultados.json` consistency.
- Verify that `runtime/` path permissions are preserved.
- Test the full automated chain: `intake -> prepare -> processing -> metrics`.


## Incident Severity Model (Suggested)

- `SEV-1`: complete intake/processing outage or confirmed data exposure risk
- `SEV-2`: degraded throughput, repeated failed studies, or unstable report-assist path
- `SEV-3`: isolated case failure without systemic impact

Suggested response:

1. Open incident log with timestamp, owner, and current impact.
2. Stabilize service (stop bleed / rollback / isolate dependency).
3. Capture root cause evidence before cleanup.
4. Document corrective and preventive actions.

## Safety Reminder

Heimdallr is clinical decision support infrastructure. Automated outputs must be reviewed by qualified professionals before clinical action.
