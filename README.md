# Heimdallr

**Open-source radiological image MLOps infrastructure — intake, segmentation, quantitative analysis, and DICOM artifact delivery**

Heimdallr is a production-oriented radiology pipeline that connects DICOM intake, study preparation, TotalSegmentator-backed segmentation, deterministic quantitative analysis, and outbound DICOM artifact delivery into a single modular Python package. The architecture is cloud-native (12-Factor), with zero `.env` files and configuration driven entirely by `HEIMDALLR_*` environment variables and JSON profiles.

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
heimdallr.segmentation      ← TotalSegmentator segmentation (parallel tasks)
      │ masks
      ▼
heimdallr.metrics         ← deterministic derived measurements (job-based)
      │ results + DICOM artifacts
      ▼
heimdallr.integration_dispatcher ← outbound patient/event webhooks
      │ queued HTTP deliveries
      ▼
heimdallr.dicom_egress   ← outbound C-STORE delivery worker
      │ queued deliveries
      ▼
runtime/studies/<case_id>/ + database/dicom.db
```

## Current Capabilities

1. **Ingestion** — DICOM C-STORE listener that groups instances by study, applies idle timeout, and hands off completed studies as ZIP payloads (local spool or HTTP upload).

2. **Preparation** — Watchdog-based worker that unpacks uploads, enumerates series, detects contrast phase via TotalSegmentator, converts DICOM to NIfTI with `dcm2niix`, records patient identity, and queues the prepared study for downstream segmentation and optional external integrations.

3. **Segmentation** — Profile-driven TotalSegmentator execution with retry logic, support for licensed tasks (`tissue_types`), and parallel task scheduling.

4. **Quantitative Metrics** — Job-based post-segmentation engine with the following modules:
   - `l3_muscle_area` — L3-level skeletal muscle area and sarcopenia metrics
   - `parenchymal_organ_volumetry` — Organ volumetry and derived overlays
   - `bone_health_l1_hu` — L1 trabecular HU-based BMD estimation

5. **DICOM Egress** — Queue-driven outbound C-STORE worker that delivers generated DICOM artifacts such as Secondary Capture overlays to fixed remote SCP destinations.

6. **Integration Dispatch** — Queue-driven outbound webhook dispatcher that emits patient-identified events to one or more external applications after `prepare` resolves the study identity.

7. **Space Manager** — Resident storage guard that monitors the filesystem hosting `runtime/studies/` and purges the oldest completed case directories when disk usage reaches a configurable host-local threshold.

8. **Control Plane** — FastAPI application serving the web dashboard, upload endpoint, patient/results API, and deterministic PDF export of case outputs.

9. **Operations TUI** — Textual-based terminal dashboard with live process monitoring, queue inspection, and study browsing.

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
│   ├── segmentation/             # Segmentation pipeline worker
│   │   ├── worker.py             #   TotalSegmentator orchestration
│   ├── metrics/                  # Post-segmentation metrics engine
│   │   ├── analysis/             #   Pure post-segmentation analysis helpers
│   │   │   ├── body_fat.py       #     Body composition analysis
│   │   │   ├── bone_health.py    #     Bone mineral density routines
│   │   │   ├── kidney_stone_triage.py # Renal stone burden scoring
│   │   │   └── opportunistic_osteoporosis_composite.py # Experimental composite entrypoint
│   │   ├── worker.py             #   Queue-driven job dispatcher
│   │   └── jobs/                 #   Individual production measurement modules
│   │       └── tests/            #   Experimental jobs and validation helpers
│   ├── deid_gateway.py           # OCR-based pixel/text de-identification helpers
│   ├── integration_dispatcher/   # Outbound HTTP webhook/event dispatcher
│   │   ├── worker.py             #   Queue-driven dispatcher
│   │   ├── config.py             #   Destination config loader + routing helper
│   │   └── events.py             #   Event payload builders
│   ├── dicom_egress/             # Outbound DICOM artifact delivery worker
│   │   ├── worker.py             #   Queue-driven C-STORE SCU dispatcher
│   │   └── config.py             #   Destination config loader + routing helper
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
├── config/                       # JSON pipeline profiles
│   ├── intake_pipeline.json      #   Listener and prepare watchdog tuning
│   ├── series_selection.json     #   Series selection strategy
│   ├── segmentation_pipeline.example.json # Example TotalSegmentator task list
│   ├── metrics_pipeline.example.json      # Example post-segmentation job list
│   ├── space_manager.example.json         # Example runtime storage GC policy
│   ├── integration_dispatch.example.json  # Example outbound webhook endpoints
│   ├── dicom_egress.example.json          # Example outbound DICOM destinations
│   └── presentation.example.json          # Example patient/presentation profiles
├── database/                     # Persistent storage
│   ├── schema.sql                #   SQLite schema (dicom_metadata, segmentation/metrics/egress queues)
│   └── README.md                 #   Schema documentation
├── bin/                          # Bundled platform binaries
│   ├── darwin-arm64/dcm2niix     #   macOS ARM
│   ├── linux-amd64/dcm2niix      #   Linux x86_64
│   ├── linux-amd64/dcmcjpeg      #   Linux x86_64 JPEG Lossless transcoder
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
├── requirements.txt              # Unified single-venv dependency manifest
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
| `dcmcjpeg` | Optional; bundled in `bin/` or system PATH for JPEG Lossless peers |
| NVIDIA GPU | Recommended for TotalSegmentator |
| TotalSegmentator license | Required for `tissue_types` task; registered in `.venv` |

### Install

```bash
git clone git@github.com:rod-americo/Heimdallr.git
cd Heimdallr

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

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

Start the baseline services in separate terminals:

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

# 8) Space Manager — runtime/studies storage reclamation
.venv/bin/python -m heimdallr.space_manager

# 9) Operations TUI — terminal dashboard
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
| `GET` | `/api/patients/{case_id}/report.pdf` | Deterministic PDF export of case outputs |
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
| `HEIMDALLR_INTEGRATION_DISPATCH_CONFIG` | `config/integration_dispatch.json` | Outbound webhook/event config |
| `HEIMDALLR_DICOM_EGRESS_CONFIG` | `config/dicom_egress.json` | Outbound DICOM destination config |
| `HEIMDALLR_PRESENTATION_CONFIG` | `config/presentation.json` | Patient name and locale presentation config |
| `HEIMDALLR_SPACE_MANAGER_CONFIG` | `config/space_manager.json` | Runtime storage reclamation policy |
| `HEIMDALLR_AE_TITLE` | `HEIMDALLR` | DICOM Application Entity title |
| `HEIMDALLR_TIMEZONE` | `America/Sao_Paulo` | Operational timezone |
| `HEIMDALLR_MAX_PARALLEL_CASES` | `3` | Concurrent segmentation slots |
| `HEIMDALLR_DICOM_HANDOFF_MODE` | `local_prepare` | `local_prepare` or `http_upload` |
| `HEIMDALLR_METRICS_MODULES` | *(see settings.py)* | Comma-separated enabled metrics |
| `TOTALSEGMENTATOR_LICENSE` | — | TotalSegmentator license key |

Before enabling segmentation, metrics, outbound delivery, or customized display/locale on a host, create the local config files from the versioned examples:

```bash
cp config/segmentation_pipeline.example.json config/segmentation_pipeline.json
cp config/metrics_pipeline.example.json config/metrics_pipeline.json
cp config/space_manager.example.json config/space_manager.json
cp config/dicom_egress.example.json config/dicom_egress.json
cp config/presentation.example.json config/presentation.json
```

These five JSON files are treated as host-local operational config and are ignored by Git:

- `config/segmentation_pipeline.json`
- `config/metrics_pipeline.json`
- `config/space_manager.json`
- `config/dicom_egress.json`
- `config/presentation.json`

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
