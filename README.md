# Heimdallr

**Open-source radiological image MLOps infrastructure — intake, segmentation, and quantitative analysis**

Heimdallr is a production-oriented radiology pipeline that connects DICOM intake, study preparation, TotalSegmentator-backed segmentation, and deterministic quantitative analysis into a single modular Python package. The architecture is cloud-native (12-Factor), with zero `.env` files and configuration driven entirely by `HEIMDALLR_*` environment variables and JSON profiles.

> **Scope boundary** — Heimdallr handles the open-source imaging infrastructure only. Proprietary clinical support, LLM-assisted reporting, and intelligence layers belong to the companion **Asha** repository.

## Pipeline Overview

```text
PACS / Modality
      │ DICOM C-STORE
      ▼
heimdallr.intake          ← DICOM listener (SCP)
      │ ZIP + manifest
      ▼
runtime/intake/uploads/   ← staging spool
      │ prepare watchdog
      ▼
heimdallr.prepare         ← series enumeration, phase detection, DICOM → NIfTI
      │ selected series
      ▼
runtime/queue/pending/
      │ claimed
      ▼
heimdallr.processing      ← TotalSegmentator segmentation (parallel tasks)
      │ masks
      ▼
heimdallr.metrics         ← deterministic derived measurements (job-based)
      │ results
      ▼
runtime/studies/<case_id>/ + database/dicom.db
```

## Current Capabilities

1. **Ingestion** — DICOM C-STORE listener that groups instances by study, applies idle timeout, and hands off completed studies as ZIP payloads (local spool or HTTP upload).

2. **Preparation** — Watchdog-based worker that unpacks uploads, enumerates series, detects contrast phase via TotalSegmentator, converts DICOM to NIfTI with `dcm2niix`, and queues the selected series for processing.

3. **Segmentation** — Profile-driven TotalSegmentator execution with retry logic, support for licensed tasks (`tissue_types`), and parallel task scheduling.

4. **Quantitative Metrics** — Job-based post-segmentation engine with the following modules:
   - `l3_muscle_area` — L3-level skeletal muscle area and sarcopenia metrics
   - `bone_health_l1_hu` — L1 trabecular HU-based BMD estimation
   - `bone_health_l1_volumetric` — L1 volumetric BMD with cortical erosion
   - `vertebral_fracture_screen` — Morphometric vertebral fracture screening
   - `opportunistic_osteoporosis_composite` — Composite osteoporosis risk score
   - `body_fat_abdominal_volumes` — Abdominal visceral / subcutaneous fat volumes
   - `body_fat_l3_slice` — L3-level body composition analysis

5. **Control Plane** — FastAPI application serving the web dashboard, upload endpoint, patient/results API, and PDF case report generation.

6. **Operations TUI** — Textual-based terminal dashboard with live process monitoring, queue inspection, and study browsing.

7. **De-identification Gateway** — OCR-based burned-in text detection and metadata scrubbing for outbound payloads (`services/deid_gateway.py`).

## Repository Layout

```
Heimdallr/
├── heimdallr/                    # Main Python package
│   ├── control_plane/            # FastAPI app factory, routers, PDF reports
│   │   ├── routers/              #   dashboard, upload, patients
│   │   ├── app.py                #   ASGI application factory
│   │   ├── case_pdf_report.py    #   Per-case PDF report builder
│   │   └── patient_service.py    #   Database-backed patient queries
│   ├── intake/                   # DICOM C-STORE listener + study handoff
│   │   └── gateway.py            #   HeimdallrDicomListener SCP
│   ├── prepare/                  # Study preparation worker
│   │   └── worker.py             #   ZIP unpack, phase detect, NIfTI convert
│   ├── processing/               # Segmentation pipeline worker
│   │   ├── worker.py             #   TotalSegmentator orchestration
│   │   ├── metrics.py            #   Legacy monolithic metrics (volumes, densities)
│   │   ├── body_fat.py           #   Body composition analysis
│   │   ├── bone_health.py        #   Bone mineral density routines
│   │   ├── kidney_stone_triage.py#   Renal stone burden scoring
│   │   └── vertebral_fracture.py #   Vertebral fracture morphometry
│   ├── metrics/                  # Post-segmentation metrics engine
│   │   ├── worker.py             #   Queue-driven job dispatcher
│   │   └── jobs/                 #   Individual measurement modules
│   ├── shared/                   # Cross-cutting concerns
│   │   ├── settings.py           #   Centralized runtime settings
│   │   ├── store.py              #   SQLite data access layer
│   │   ├── paths.py              #   Canonical study path helpers
│   │   ├── db.py                 #   FastAPI database dependency
│   │   ├── spool.py              #   Atomic file write utilities
│   │   └── schemas/              #   Pydantic models
│   └── tui/                      # Textual operations dashboard
│       ├── app.py                #   TUI application
│       ├── snapshot.py           #   Runtime state snapshot collector
│       └── dashboard.tcss        #   Textual CSS stylesheet
├── services/                     # Standalone operational services
│   ├── dicom_listener.py         #   Legacy DICOM listener (see heimdallr.intake)
│   └── deid_gateway.py           #   De-identification gateway
├── config/                       # JSON pipeline profiles
│   ├── intake_pipeline.json      #   Listener and prepare watchdog tuning
│   ├── series_selection.json     #   Series selection strategy
│   ├── segmentation_pipeline.json#   TotalSegmentator task list
│   ├── metrics_pipeline.json     #   Post-segmentation job list
│   └── presentation.json        #   Patient name display profiles
├── database/                     # Persistent storage
│   ├── schema.sql                #   SQLite schema (dicom_metadata, queues)
│   └── README.md                 #   Schema documentation
├── bin/                          # Bundled platform binaries
│   ├── darwin-arm64/dcm2niix     #   macOS ARM
│   ├── linux-amd64/dcm2niix      #   Linux x86_64
│   └── licenses/                 #   Upstream license files
├── scripts/                      # Operational and retroactive scripts
│   ├── retroactive_recalculate_metrics.py
│   ├── consolidate_metrics_csv.py
│   ├── extract_prometheus_bmd.py
│   ├── update_kvp_retroactive.py
│   ├── bmd_roi_comparison_preview.py
│   ├── retroactive_emphysema.py
│   └── watch_heimdallr.py        #   Filesystem watcher for auto-upload
├── static/                       # Web dashboard assets (HTML/CSS/JS)
├── tests/                        # Unit and integration tests
├── runtime/                      # Transient runtime data (gitignored)
│   ├── intake/uploads/           #   Staged ZIP payloads
│   ├── queue/{pending,active,failed}/
│   └── studies/<case_id>/        #   Per-case artifacts, derived, metadata, logs
├── requirements/                 # Dependency manifests
│   ├── requirements.txt          #   Pinned production dependencies
│   ├── operational-services.txt  #   Minimal operational subset
│   └── totalsegmentator.txt      #   TotalSegmentator venv dependencies
├── docs/                         # Extended documentation
├── .github/                      # Community standards and CI
│   ├── SECURITY.md
│   ├── CONTRIBUTING.md
│   ├── CODEOWNERS
│   ├── PULL_REQUEST_TEMPLATE.md
│   ├── ISSUE_TEMPLATE/
│   └── workflows/ci.yml
├── pyproject.toml                # Build system and package metadata
├── AGENTS.md                     # AI agent operating guidelines
├── LICENSE.md                    # Apache License 2.0
└── NOTICE.md                    # Third-party attribution
```

## Quick Start

### Prerequisites

| Requirement | Notes |
|---|---|
| Python `≥ 3.10` | Package requires `zoneinfo`, `tomllib` |
| `dcm2niix` | Bundled in `bin/` or system PATH |
| NVIDIA GPU | Recommended for TotalSegmentator |
| TotalSegmentator license | Required for `tissue_types` task; registered in `.venv-totalseg` |

### Install

```bash
git clone git@github.com:rod-americo/Heimdallr.git
cd Heimdallr

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements/requirements.txt
```

For licensed TotalSegmentator tasks, register the license in the dedicated venv:

```bash
.venv-totalseg/bin/totalseg_set_license -l YOUR_LICENSE_KEY
```

### Run

Start the baseline services in separate terminals:

```bash
# 1) Control Plane — API + dashboard (default :8001)
.venv/bin/python -m heimdallr.control_plane

# 2) Processing Worker — segmentation pipeline
.venv/bin/python -m heimdallr.processing

# 3) Metrics Worker — post-segmentation measurements
.venv/bin/python -m heimdallr.metrics

# 4) DICOM Listener — C-STORE intake (default :11114)
.venv/bin/python -m heimdallr.intake

# 5) Operations TUI — terminal dashboard
.venv/bin/python -m heimdallr.tui
```

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
| `POST` | `/upload` | Accept a `.zip` study payload |
| `GET` | `/` | Dashboard shell |
| `GET` | `/api/tools/uploader` | Download the CLI uploader script |

### Patients and Results

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/patients` | List all processed patients |
| `GET` | `/api/patients/{case_id}/results` | Case calculation results |
| `GET` | `/api/patients/{case_id}/metadata` | DICOM and pipeline metadata |
| `GET` | `/api/patients/{case_id}/nifti` | Download canonical NIfTI |
| `GET` | `/api/patients/{case_id}/report.pdf` | Generated PDF case report |
| `GET` | `/api/patients/{case_id}/download/{folder}` | Download artifact folder as ZIP |
| `GET` | `/api/patients/{case_id}/images/{filename}` | Serve overlay or artifact image |
| `GET` | `/api/patients/{case_id}/artifacts/{path}` | Serve arbitrary case artifact |
| `PATCH` | `/api/patients/{case_id}/biometrics` | Update weight/height |
| `PATCH` | `/api/patients/{case_id}/smi` | Update SMI value |

## Configuration

All settings are read from environment variables (`HEIMDALLR_*`) or JSON profiles under `config/`. There are **no `.env` files** — values are injected by the host system, `launchd`, `systemd`, or Docker.

Key environment variables:

| Variable | Default | Description |
|---|---|---|
| `HEIMDALLR_SERVER_PORT` | `8001` | Control plane HTTP port |
| `HEIMDALLR_DICOM_PORT` | `11114` | DICOM listener port |
| `HEIMDALLR_AE_TITLE` | `HEIMDALLR` | DICOM Application Entity title |
| `HEIMDALLR_TIMEZONE` | `America/Sao_Paulo` | Operational timezone |
| `HEIMDALLR_MAX_PARALLEL_CASES` | `3` | Concurrent processing slots |
| `HEIMDALLR_DICOM_HANDOFF_MODE` | `local_prepare` | `local_prepare` or `http_upload` |
| `HEIMDALLR_METRICS_MODULES` | *(see settings.py)* | Comma-separated enabled metrics |
| `TOTALSEGMENTATOR_LICENSE` | — | TotalSegmentator license key |

See [`heimdallr/shared/settings.py`](heimdallr/shared/settings.py) for the complete reference.

## Operational Scripts

| Script | Purpose |
|---|---|
| `scripts/retroactive_recalculate_metrics.py` | Regenerate metrics for existing cases (`--case`, `--limit`, `--workers`) |
| `scripts/consolidate_metrics_csv.py` | Export metrics database to CSV |
| `scripts/extract_prometheus_bmd.py` | Extract BMD values for Prometheus ingestion |
| `scripts/update_kvp_retroactive.py` | Backfill kVp values from DICOM metadata |
| `scripts/bmd_roi_comparison_preview.py` | Visual preview of BMD ROI placement |
| `scripts/watch_heimdallr.py` | Filesystem watcher for auto-upload from drop folder |

## Documentation

| Document | Description |
|---|---|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | System architecture overview |
| [`docs/API.md`](docs/API.md) | API contracts and examples |
| [`docs/OPERATIONS.md`](docs/OPERATIONS.md) | Operations runbook |
| [`docs/UPCOMING.md`](docs/UPCOMING.md) | Roadmap and future modules |
| [`docs/validation-stage-manual.md`](docs/validation-stage-manual.md) | Validation stage manual |
| [`docs/pipeline-implementation-guidelines.md`](docs/pipeline-implementation-guidelines.md) | Pipeline implementation guidelines |
| [`database/README.md`](database/README.md) | Database schema documentation |

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

Third-party components carry additional licensing requirements. In particular, **TotalSegmentator** requires a valid license for commercial use. Review [`NOTICE.md`](NOTICE.md) for the complete attribution list before production deployment.
