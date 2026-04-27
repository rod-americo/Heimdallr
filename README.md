# Heimdallr

**Open-source radiological image MLOps infrastructure for DICOM intake, deterministic preparation, TotalSegmentator orchestration, quantitative analysis, and outbound artifact delivery**

Heimdallr is a production-oriented radiology pipeline packaged as a single Python application stack. It receives DICOM studies, stages them for preparation, converts selected series to NIfTI, runs TotalSegmentator-backed segmentation, computes deterministic quantitative outputs, persists metadata/results in SQLite, and optionally dispatches outbound DICOM artifacts and webhook events to external systems.

The project follows a cloud-native / 12-Factor style:
- zero `.env` files
- a single `.venv` runtime
- host-injected `HEIMDALLR_*` environment variables
- host-local operational JSON under `config/`

> **Scope boundary** — Heimdallr handles the open-source imaging infrastructure only. Proprietary clinical support, LLM-assisted reporting, and intelligence layers belong to the companion **Asha** repository.

## Pipeline Overview

```text
PACS / Modality
      │ DICOM C-STORE
      ▼
heimdallr.intake                 ← DICOM listener (SCP)
      │ ZIP + manifest
      ▼
runtime/intake/uploads/from_prepare/   ← listener handoff spool (priority FIFO)
runtime/intake/uploads/external/       ← external ZIP spool (secondary FIFO)
      │
      ▼
heimdallr.prepare                ← ZIP unpack, metadata extraction,
      │                             DICOM → NIfTI, phase detection
      ├──────────────► heimdallr.integration_dispatcher
      │               patient-identified outbound webhooks
      ├──────────────► heimdallr.integration_delivery
      │               final package push to external submitter
      │
      ▼
runtime/queue/pending/
      │
      ▼
heimdallr.segmentation           ← TotalSegmentator orchestration
      │ masks / segmentation artifacts
      ▼
heimdallr.metrics                ← deterministic derived measurements
      ├──────────────► heimdallr.dicom_egress
      │               outbound C-STORE delivery
      │
      ▼
runtime/studies/<case_id>/ + database/dicom.db
      ▲
      │
heimdallr.space_manager          ← runtime/studies retention / purge guard
```

## Current Capabilities

1. **Ingestion**
   - DICOM C-STORE listener (`heimdallr.intake`)
   - study grouping by `StudyInstanceUID`
   - idle-time handoff as ZIP
   - local spool or HTTP upload handoff modes

2. **Preparation**
   - watchdog-based ZIP consumer (`heimdallr.prepare`)
   - dual-source upload spool:
     - `runtime/intake/uploads/from_prepare/`
     - `runtime/intake/uploads/external/`
   - FIFO within each source, with `from_prepare` prioritized over `external`
   - DICOM metadata extraction, patient normalization, phase detection, DICOM → NIfTI conversion
   - emission of outbound `patient_identified` integration events

3. **Segmentation**
   - profile-driven TotalSegmentator execution (`heimdallr.segmentation`)
   - support for licensed tasks such as `tissue_types`
   - configurable task-level parallelism
   - duplicate-skip reuse when the same selected series and slice count were already segmented successfully

4. **Quantitative Metrics**
   - job-based post-segmentation engine (`heimdallr.metrics`)
   - dynamic job resolution from the metrics pipeline config
   - current production-facing jobs:
     - `l3_muscle_area`
     - `parenchymal_organ_volumetry`
     - `bone_health_l1_hu`

5. **DICOM Egress**
   - queue-driven outbound C-STORE worker (`heimdallr.dicom_egress`)
   - per-destination retries and error isolation
   - JPEG Lossless fallback support for peers that require compressed Secondary Capture

6. **Integration Dispatch**
   - queue-driven outbound webhook/event dispatcher (`heimdallr.integration_dispatcher`)
   - async delivery after `prepare`
   - retry/backoff and multi-destination support

7. **External Submit + Final Delivery**
   - `POST /jobs` upload ingress for external systems
   - per-job callback URL persisted with the submitted study
   - queue-driven final `manifest.json` + `package.zip` callback after `metrics`

8. **Space Manager**
   - resident storage guard (`heimdallr.space_manager`)
   - monitors the filesystem that hosts `runtime/studies/`
   - purges oldest completed studies when a host-local disk usage threshold is exceeded

9. **Resource Monitor**
   - resident RAM telemetry sampler (`heimdallr.resource_monitor`)
   - amostra RSS do worker, RSS da árvore de subprocessos, memória do cgroup e swap do host
   - persiste snapshots temporais em SQLite para análise posterior de capacidade

10. **Control Plane**
   - FastAPI application serving dashboard, upload ingress, patient/results API, artifact download, and deterministic PDF export

11. **Operations TUI**
   - Textual dashboard with live service radar, queue pressure, case spotlight, and upload-origin markers (`P` / `E`)

## Repository Layout

This tree reflects the maintained, versioned structure of the repository.

```text
Heimdallr/
├── heimdallr/                      # Main Python package
│   ├── control_plane/              # FastAPI API, dashboard, upload ingress, PDF export
│   ├── intake/                     # DICOM SCP listener and study handoff
│   ├── prepare/                    # ZIP watchdog, metadata extraction, DICOM→NIfTI
│   ├── segmentation/               # TotalSegmentator orchestration
│   ├── metrics/                    # Deterministic post-segmentation jobs
│   │   ├── analysis/               #   Shared analysis helpers
│   │   └── jobs/                   #   Production jobs + experimental jobs/tests
│   ├── integration_dispatcher/     # Outbound webhook/event delivery worker
│   ├── integration_delivery/       # Outbound final package callback worker
│   ├── dicom_egress/               # Outbound DICOM C-STORE worker
│   ├── space_manager/              # Disk-usage guard for runtime/studies
│   ├── resource_monitor/           # Resident RAM telemetry sampler
│   ├── shared/                     # Settings, SQLite store, paths, i18n, schemas
│   ├── tui/                        # Textual operations dashboard
│   ├── locales/                    # gettext catalogs (`artifacts`, `tui`)
│   └── deid_gateway.py             # OCR-based de-identification helper
├── config/                         # Versioned defaults + host-local example JSON
│   ├── intake_pipeline.json        #   versioned listener/prepare defaults
│   ├── series_selection.json       #   versioned series selection rules
│   └── *.example.json              #   host-local operational config templates
├── database/                       # SQLite schema and documentation
├── bin/                            # Bundled runtime binaries and notices
│   ├── darwin-arm64/dcm2niix
│   ├── linux-amd64/dcm2niix
│   ├── linux-amd64/dcmcjpeg*       #   plus shared libraries and dictionaries
│   └── licenses/
├── scripts/                        # Retroactive and operational utilities
├── static/                         # Dashboard frontend assets and branding
├── tests/                          # Unit and integration coverage
├── runtime/                        # Generated runtime data (gitignored)
│   ├── intake/uploads/{from_prepare,external}/
│   ├── queue/{pending,active,failed}/
│   └── studies/<case_id>/
├── docs/                           # Architecture, API, operations, branding notes
├── .github/                        # Community standards and CI
├── requirements.txt                # Unified single-venv dependency manifest
├── pyproject.toml                  # Packaging metadata
├── AGENTS.md                       # Repo-specific agent instructions
├── LICENSE.md
└── NOTICE.md
```

## Quick Start

### Prerequisites

| Requirement | Notes |
|---|---|
| Python `3.12` | `pyproject.toml` currently requires `>=3.12,<3.13` |
| Single `.venv` | All services should run from the same interpreter |
| `dcm2niix` | Bundled in `bin/` or resolvable from `PATH` |
| `dcmcjpeg` | Optional; used for JPEG Lossless fallback during DICOM egress |
| NVIDIA GPU | Optional but recommended for TotalSegmentator |
| TotalSegmentator license | Required for licensed tasks such as `tissue_types` |

### Install

```bash
git clone git@github.com:rod-americo/Heimdallr.git
cd Heimdallr

python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

The runtime is single-venv. There is no longer a supported split between a
"light" service venv and a separate TotalSegmentator venv.

`dicom2nifti` fallback support comes from the `python-gdcm` wheel installed in
the same `.venv`; no additional standalone helper is required for that path.

For licensed TotalSegmentator tasks, register the license in the same venv:

```bash
.venv/bin/totalseg_set_license -l YOUR_LICENSE_KEY
```

All long-running services on a host should use the same interpreter:
`.venv/bin/python`.

For DICOM peers that only accept `Secondary Capture` in JPEG Lossless transfer
syntaxes, place `dcmcjpeg` in `bin/linux-amd64/dcmcjpeg` (or `bin/dcmcjpeg`)
and keep the matching upstream notice under `bin/licenses/`. Distributions
that carry this binary should also retain the DCMTK notice and the statement:
"This product includes software based in part on the work of the Independent
JPEG Group."

### Run

Start the services in separate terminals or service units:

```bash
# 1) Control Plane — API + dashboard (default :8001)
.venv/bin/python -m heimdallr.control_plane

# 2) Prepare Worker — study preparation watchdog
.venv/bin/python -m heimdallr.prepare

# 3) Segmentation Worker — segmentation pipeline
.venv/bin/python -m heimdallr.segmentation

# 4) Metrics Worker — post-segmentation measurements
.venv/bin/python -m heimdallr.metrics

# 5) DICOM Listener — C-STORE intake (default :11114)
.venv/bin/python -m heimdallr.intake

# 6) DICOM Egress Worker — outbound C-STORE delivery
.venv/bin/python -m heimdallr.dicom_egress

# 7) Integration Dispatcher — outbound patient/event webhooks
.venv/bin/python -m heimdallr.integration_dispatcher

# 8) Final Delivery Worker — push final package to external submitter
.venv/bin/python -m heimdallr.integration_delivery

# 9) Space Manager — runtime/studies storage reclamation
.venv/bin/python -m heimdallr.space_manager

# 10) Resource Monitor — resident RAM telemetry sampler
.venv/bin/python -m heimdallr.resource_monitor

# 11) Operations TUI — terminal dashboard
.venv/bin/python -m heimdallr.tui
```

The minimum processing stack for a headless host is usually:
- `heimdallr.intake`
- `heimdallr.prepare`
- `heimdallr.segmentation`
- `heimdallr.metrics`
- `heimdallr.dicom_egress`

Add these when needed:
- `heimdallr.integration_dispatcher`
- `heimdallr.integration_delivery`
- `heimdallr.space_manager`
- `heimdallr.resource_monitor`
- `heimdallr.control_plane`
- `heimdallr.tui`

### Access

| Endpoint | URL |
|---|---|
| Web Dashboard | `http://localhost:8001` |
| OpenAPI Docs | `http://localhost:8001/docs` |
| DICOM AE | `HEIMDALLR` on port `11114` |
| TUI | `python -m heimdallr.tui` |

## API Surface

### Upload and Dashboard

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Accept an externally submitted `.zip` plus callback metadata |
| `POST` | `/upload` | Accept a `.zip` study payload into the external spool |
| `GET` | `/` | Dashboard shell |
| `GET` | `/api/tools/uploader` | Download the CLI uploader script |

`POST /jobs` expects `multipart/form-data` with:
- `study_file`: ZIP payload
- `client_case_id`: caller-owned identifier echoed back on delivery
- `callback_url`: final package callback target
- `source_system`: optional caller label
- `requested_outputs`: optional JSON object

Accepted jobs are persisted into the external spool plus a sidecar submission
manifest. After `metrics` completes, `heimdallr.integration_delivery` pushes
`manifest.json` + `package.zip` back to the caller's `callback_url`.

### Patients and Results

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/patients` | List processed patients/cases |
| `GET` | `/api/patients/{case_id}/results` | Case calculation results |
| `GET` | `/api/patients/{case_id}/metadata` | DICOM and pipeline metadata |
| `GET` | `/api/patients/{case_id}/nifti` | Download canonical NIfTI |
| `GET` | `/api/patients/{case_id}/report.pdf` | Deterministic PDF export of case outputs |
| `GET` | `/api/patients/{case_id}/download/{folder}` | Download artifact folder as ZIP |
| `GET` | `/api/patients/{case_id}/images/{filename}` | Serve overlay or artifact image |
| `GET` | `/api/patients/{case_id}/artifacts/{path}` | Serve arbitrary case artifact |
| `PATCH` | `/api/patients/{case_id}/biometrics` | Update weight/height |
| `PATCH` | `/api/patients/{case_id}/smi` | Update SMI value |

## Configuration

All settings are read from `HEIMDALLR_*` environment variables or JSON profiles
under `config/`. There are **no `.env` files**. Sensitive or host-specific
values are injected by the host system, `launchd`, `systemd`, `skuld`, or
containers.

### Versioned Configuration

These files are part of the repository contract:
- `config/intake_pipeline.json`
- `config/series_selection.json`

### Host-Local Operational Configuration

These files are expected to vary per host and are ignored by Git:
- `config/segmentation_pipeline.json`
- `config/metrics_pipeline.json`
- `config/integration_dispatch.json`
- `config/integration_delivery.json`
- `config/space_manager.json`
- `config/resource_monitor.json`
- `config/dicom_egress.json`
- `config/presentation.json`

Create them from the example templates:

```bash
cp config/segmentation_pipeline.example.json config/segmentation_pipeline.json
cp config/metrics_pipeline.example.json config/metrics_pipeline.json
cp config/integration_dispatch.example.json config/integration_dispatch.json
cp config/integration_delivery.example.json config/integration_delivery.json
cp config/space_manager.example.json config/space_manager.json
cp config/resource_monitor.example.json config/resource_monitor.json
cp config/dicom_egress.example.json config/dicom_egress.json
cp config/presentation.example.json config/presentation.json
```

### Key Environment Variables

| Variable | Default | Description |
|---|---|---|
| `HEIMDALLR_SERVER_PORT` | `8001` | Control plane HTTP port |
| `HEIMDALLR_DICOM_PORT` | `11114` | DICOM listener port |
| `HEIMDALLR_INTAKE_PIPELINE_CONFIG` | `config/intake_pipeline.json` | Intake/prepare watchdog config |
| `HEIMDALLR_SEGMENTATION_PIPELINE_CONFIG` | `config/segmentation_pipeline.json` | Segmentation profile config |
| `HEIMDALLR_SERIES_SELECTION_CONFIG` | `config/series_selection.json` | Series selection rules |
| `HEIMDALLR_METRICS_PIPELINE_CONFIG` | `config/metrics_pipeline.json` | Metrics profile config |
| `HEIMDALLR_INTEGRATION_DISPATCH_CONFIG` | `config/integration_dispatch.json` | Outbound webhook/event config |
| `HEIMDALLR_INTEGRATION_DELIVERY_CONFIG` | `config/integration_delivery.json` | Final package callback worker config |
| `HEIMDALLR_DICOM_EGRESS_CONFIG` | `config/dicom_egress.json` | Outbound DICOM destination config |
| `HEIMDALLR_PRESENTATION_CONFIG` | `config/presentation.json` | Patient name and locale presentation config |
| `HEIMDALLR_SPACE_MANAGER_CONFIG` | `config/space_manager.json` | Runtime storage reclamation policy |
| `HEIMDALLR_RESOURCE_MONITOR_CONFIG` | `config/resource_monitor.json` | Resident RAM telemetry sampling policy |
| `HEIMDALLR_AE_TITLE` | `HEIMDALLR` | DICOM Application Entity title |
| `HEIMDALLR_TIMEZONE` | `America/Sao_Paulo` | Operational timezone |
| `HEIMDALLR_MAX_PARALLEL_CASES` | `1` | Concurrent segmentation case slots |
| `HEIMDALLR_DICOM_HANDOFF_MODE` | `local_prepare` | `local_prepare` or `http_upload` |
| `HEIMDALLR_DCM2NIIX_BIN` | auto | Override bundled/system `dcm2niix` |
| `HEIMDALLR_DCMCJPEG_BIN` | auto | Override bundled/system `dcmcjpeg` |
| `TOTALSEGMENTATOR_LICENSE` | — | Optional direct license injection |

See [`heimdallr/shared/settings.py`](heimdallr/shared/settings.py) for the
complete reference.

## Operational Scripts

| Script | Purpose |
|---|---|
| `scripts/retroactive_recalculate_metrics.py` | Regenerate metrics for existing cases (`--case`, `--limit`, `--workers`) |
| `scripts/consolidate_metrics_csv.py` | Export metrics database to CSV |
| `scripts/extract_prometheus_bmd.py` | Extract BMD values for Prometheus ingestion |
| `scripts/update_kvp_retroactive.py` | Backfill kVp values from DICOM metadata |
| `scripts/bmd_roi_comparison_preview.py` | Visual preview of BMD ROI placement |
| `scripts/retroactive_emphysema.py` | Retroactive emphysema calculations |
| `scripts/watch_heimdallr.py` | Filesystem watcher for auto-upload from drop folder |

Experimental metrics jobs are not part of the default profile. Keep them under
`heimdallr/metrics/jobs/tests/` and enable them only through explicit
`jobs[].module` overrides in a host-local metrics pipeline config.

## Documentation

| Document | Description |
|---|---|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | System architecture overview |
| [`docs/API.md`](docs/API.md) | API contracts and examples |
| [`docs/OPERATIONS.md`](docs/OPERATIONS.md) | Operations runbook |
| [`docs/validation-stage-manual.md`](docs/validation-stage-manual.md) | Validation stage manual |
| [`database/README.md`](database/README.md) | Database schema documentation |
| [`docs/branding/README.md`](docs/branding/README.md) | Brand asset inventory |

## Governance

| Resource | Location |
|---|---|
| Security Policy | [`.github/SECURITY.md`](.github/SECURITY.md) |
| Contributing Guide | [`.github/CONTRIBUTING.md`](.github/CONTRIBUTING.md) |
| Code Ownership | [`.github/CODEOWNERS`](.github/CODEOWNERS) |
| Third-Party Notices | [`NOTICE.md`](NOTICE.md) |
| AI Agent Guidelines | [`AGENTS.md`](AGENTS.md) |

## License

Heimdallr is distributed under the [Apache License 2.0](LICENSE.md).

Third-party components carry additional licensing requirements. In particular,
**TotalSegmentator** requires a valid license for commercial use. Review
[`NOTICE.md`](NOTICE.md) for the complete attribution list before production
deployment.
