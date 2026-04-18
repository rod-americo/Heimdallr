# Operations Runbook

This document provides a baseline for operating Heimdallr in production-like environments.

## Service Topology

Run as independent services:

1. `python -m heimdallr.control_plane` (API, dashboard, upload endpoints)
2. `python -m heimdallr.prepare` (study preparation watchdog)
3. `python -m heimdallr.segmentation` (segmentation worker)
4. `python -m heimdallr.metrics` (post-segmentation derived metrics worker)
5. `python -m heimdallr.intake` (DICOM C-STORE intake)
6. `python -m heimdallr.integration_dispatcher` (outbound webhook/event delivery)
7. `python -m heimdallr.dicom_egress` (outbound DICOM artifact delivery)
8. `python -m heimdallr.space_manager` (runtime/studies storage reclamation)
9. `python -m heimdallr.resource_monitor` (resident RAM telemetry sampling)
10. `python -m heimdallr.tui` (optional operational dashboard)

## Repository File Map

### Core Package (`heimdallr/`)
- `control_plane/`: FastAPI application, routers, and static serving.
- `intake/gateway.py`: DICOM reception and study spooling.
- `prepare/worker.py`: Series discovery and DICOM-to-NIfTI conversion.
- `segmentation/worker.py`: Core segmentation worker logic.
- `metrics/worker.py`: Post-segmentation derived metrics execution.
- `integration_dispatcher/worker.py`: Queue-driven outbound webhook dispatcher.
- `dicom_egress/worker.py`: Queue-driven outbound C-STORE dispatcher.
- `shared/settings.py`: Centralized runtime settings and binary paths.

### Configuration and Data
- `config/`: Pipeline JSON profiles and series selection rules.
- `database/schema.sql`: SQLite schema for state management.
- `static/`: Frontend dashboard assets.


## Baseline Startup

Single-venv host installation:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
.venv/bin/totalseg_set_license -l YOUR_LICENSE_KEY

The unified runtime targets Python 3.12. `dicom2nifti` runs from the Python
package itself; native GDCM libraries are expected to come from the
`python-gdcm` wheel inside `.venv`, not from a separate binary under `bin/`.
```

All supervised services should point to the same interpreter:
`.venv/bin/python`.

For DICOM peers that only accept JPEG Lossless `Secondary Capture`, keep
`dcmcjpeg` bundled in `bin/linux-amd64/dcmcjpeg` (or `bin/dcmcjpeg`). Heimdallr
resolves this binary before falling back to the system `PATH`, and the matching
upstream notice should live under `bin/licenses/`.

```bash
# API + Dashboard
source .venv/bin/activate
python -m heimdallr.control_plane

# Prepare worker
source .venv/bin/activate
python -m heimdallr.prepare

# Segmentation worker
source .venv/bin/activate
python -m heimdallr.segmentation

# Metrics worker
source .venv/bin/activate
python -m heimdallr.metrics

# DICOM listener
source .venv/bin/activate
python -m heimdallr.intake

# DICOM egress worker
source .venv/bin/activate
python -m heimdallr.dicom_egress

# Space manager
source .venv/bin/activate
python -m heimdallr.space_manager

# Resource monitor
source .venv/bin/activate
python -m heimdallr.resource_monitor
```

## Environment and Config

Configuration is centralized in `heimdallr/shared/settings.py` plus the pipeline JSON profiles under `config/`, and can be overridden via `HEIMDALLR_*` environment variables.

Common examples:

```bash
export HEIMDALLR_AE_TITLE="HEIMDALLR"
export HEIMDALLR_DICOM_PORT="11114"
export HEIMDALLR_IDLE_SECONDS="30"
export HEIMDALLR_INTEGRATION_DISPATCH_CONFIG="config/integration_dispatch.json"
export HEIMDALLR_DICOM_EGRESS_CONFIG="config/dicom_egress.json"
export HEIMDALLR_PRESENTATION_CONFIG="config/presentation.json"
export HEIMDALLR_SEGMENTATION_PIPELINE_CONFIG="config/segmentation_pipeline.json"
export HEIMDALLR_METRICS_PIPELINE_CONFIG="config/metrics_pipeline.json"
export HEIMDALLR_SPACE_MANAGER_CONFIG="config/space_manager.json"
export HEIMDALLR_RESOURCE_MONITOR_CONFIG="config/resource_monitor.json"
```

Before enabling segmentation, metrics, outbound DICOM delivery, or customizing presentation defaults on a host, create the local config files from the repository examples:

```bash
cp config/segmentation_pipeline.example.json config/segmentation_pipeline.json
cp config/metrics_pipeline.example.json config/metrics_pipeline.json
cp config/space_manager.example.json config/space_manager.json
cp config/resource_monitor.example.json config/resource_monitor.json
cp config/integration_dispatch.example.json config/integration_dispatch.json
cp config/dicom_egress.example.json config/dicom_egress.json
cp config/presentation.example.json config/presentation.json
```

These six JSON files are host-local operational config and are ignored by Git:

- `config/segmentation_pipeline.json`
- `config/metrics_pipeline.json`
- `config/space_manager.json`
- `config/resource_monitor.json`
- `config/integration_dispatch.json`
- `config/dicom_egress.json`
- `config/presentation.json`

For outbound webhook authentication, prefer `headers_from_env` in
`config/integration_dispatch.json` over hardcoding secrets directly in the
JSON file.

## End-to-End Pipeline Flow

1. **Intake**: Study arrives via DICOM C-STORE (`intake`) or manual `POST /upload`.
2. **Spooling**: Files are stored in `runtime/intake/uploads/`.
3. **Preparation**: `prepare` worker extracts ZIP, enumerates candidate series, converts them to NIfTI via `dcm2niix`, records phase metadata, and creates `metadata/id.json`.
4. **Segmentation**: `segmentation` worker claims the case, selects the series using `config/series_selection.json`, runs segmentation (e.g., TotalSegmentator), and archives results in `runtime/studies/<CaseID>/derived/`.
5. **Metrics**: `metrics` worker executes derived calculations (volumetry, density) and updates `metadata/resultados.json`.
6. **Integration Dispatch**: `prepare` enqueues patient-identified events into `integration_dispatch_queue`, and `integration_dispatcher` delivers them to configured HTTP endpoints.
7. **DICOM Egress**: `metrics` enqueues generated DICOM artifacts into `dicom_egress_queue`, and `dicom_egress` delivers them to configured remote SCP destinations.
8. **Storage Reclamation**: `space_manager` periodically checks filesystem usage and, above threshold, deletes the oldest non-active case directories under `runtime/studies/` while marking them as purged in SQLite.
9. **Delivery**: Dashboard and APIs serve the final structured data and images.

## Data Contracts

### `metadata/id.json`
Contains study-level metadata, CaseID, and series selection audit trail.

### `metadata/resultados.json`
The primary output for clinical consumption. Includes:
- `modality` and `body_regions`.
- Organ volumetry and quantification.
- Derived metrics (attenuation, indices).
- Metadata for post-segmentation jobs.

### SQLite (`database/dicom.db`)
Central state for all studies, tracking pipeline stage, patient demographics, and calculated summaries.

Queue tables include:
- `segmentation_queue`
- `metrics_queue`
- `integration_dispatch_queue`
- `dicom_egress_queue`


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
3. Queue path `upload -> prepare -> segmentation -> metrics` completes for a known study.
4. If outbound delivery is enabled, `dicom_egress_queue` drains and the remote SCP accepts C-STORE.
5. GPU capacity is available for segmentation workloads.

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
source .venv/bin/activate
.venv/bin/python scripts/retroactive_recalculate_metrics.py --limit 10
```

Common variants:

- Skip PNG regeneration: `.venv/bin/python scripts/retroactive_recalculate_metrics.py --skip-overlays`
- Process a specific case: `.venv/bin/python scripts/retroactive_recalculate_metrics.py --case <case_id>`
- Parallelize cautiously: `.venv/bin/python scripts/retroactive_recalculate_metrics.py --workers 2`

Legacy compatibility wrapper:

```bash
.venv/bin/python scripts/retroactive_emphysema.py
```

## Incident Triage Shortlist

1. Validate service process state and restart order (`control_plane -> prepare -> segmentation -> metrics -> dicom_egress -> intake`).
2. Check PACS destination configuration and network reachability.
3. Inspect `runtime/intake/` and `runtime/studies/` for stuck or failed studies.
4. Confirm data storage permissions for the `runtime/` directory.

## Maintenance Playbook

- **Adjusting endpoints**: Edit the relevant router under `heimdallr/control_plane/routers/`.
- **Changing series selection**: Update `config/series_selection.json`.
- **Adding a clinical metric**: Add a production job under `heimdallr/metrics/jobs/` and enable it in the host-local `config/metrics_pipeline.json`.
- **Keeping an experimental metric out of the default profile**: Place it under `heimdallr/metrics/jobs/tests/` or `heimdallr/metrics/analysis/` and use an explicit `jobs[].module` override only in host-local config.
- **Changing segmentation tasks or CPU/GPU/threading policy**: Update the host-local `config/segmentation_pipeline.json` created from `config/segmentation_pipeline.example.json`.
- **Changing enabled metrics or job parallelism**: Update the host-local `config/metrics_pipeline.json` created from `config/metrics_pipeline.example.json`.
- **Changing outbound patient/event webhooks**: Update the host-local `config/integration_dispatch.json` created from `config/integration_dispatch.example.json`.
- **Changing storage reclamation policy**: Update the host-local `config/space_manager.json` created from `config/space_manager.example.json`.
- **Improving intake logic**: Edit `heimdallr/intake/gateway.py`.
- **Changing outbound DICOM destinations**: Update the host-local `config/dicom_egress.json` created from `config/dicom_egress.example.json`.
- **Changing locale or patient-name presentation defaults**: Update the host-local `config/presentation.json` created from `config/presentation.example.json`.

## Checklist Before Changes
- Confirm impact on `id.json` and `resultados.json` consistency.
- Verify that `runtime/` path permissions are preserved.
- Test the full automated chain: `intake -> prepare -> segmentation -> metrics -> dicom_egress`.


## Incident Severity Model (Suggested)

- `SEV-1`: complete intake/segmentation outage or confirmed data exposure risk
- `SEV-2`: degraded throughput, repeated failed studies, or unstable report-assist path
- `SEV-3`: isolated case failure without systemic impact

Suggested response:

1. Open incident log with timestamp, owner, and current impact.
2. Stabilize service (stop bleed / rollback / isolate dependency).
3. Capture root cause evidence before cleanup.
4. Document corrective and preventive actions.

## Safety Reminder

Heimdallr is clinical decision support infrastructure. Automated outputs must be reviewed by qualified professionals before clinical action.
