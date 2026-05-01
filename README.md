# Heimdallr

Open-source radiological image MLOps infrastructure for DICOM intake, study
preparation, DICOM-to-NIfTI conversion, TotalSegmentator-backed segmentation,
deterministic quantitative metrics, SQLite state, operational dashboards, and
outbound artifact delivery.

Heimdallr is an existing Python service stack, not a greenfield starter. The
current architecture is organized around the `heimdallr/` package, host-local
JSON configuration under `config/`, SQLite state under `database/`, and mutable
runtime artifacts under `runtime/` (ignored by Git).

## What This Repository Is

- A multi-entrypoint Python 3.12 radiological image processing and operations
  stack.
- A DICOM C-STORE listener, ZIP spool processor, segmentation worker, metrics
  worker, DICOM egress worker, integration delivery worker, FastAPI control
  plane, and Textual operations dashboard.
- The open-source infrastructure layer for image ingestion, conversion,
  deterministic calculations, artifact generation, queue state, and operational
  observability.

## What This Repository Is Not

- It is not a proprietary clinical reporting system or final report drafting
  system.
- It is not the home for LLM, NLP, prompt-engineering, MedGemma, OpenAI,
  Anthropic, or intelligence-layer workflows. Those responsibilities belong to
  the companion Asha repository.
- It is not a generic project template; starter-kit governance is adapted here
  only where it reflects the current code and operation.
- It is not declared production-ready for autonomous clinical decision-making.
  Outputs are assistive infrastructure artifacts and require qualified review.

## Current State

- phase: `active structural recovery / validation-oriented operations`
- runtime principal: Python `>=3.12,<3.13`, single `.venv`
- primary entrypoints:
  - `python -m heimdallr.control_plane`
  - `python -m heimdallr.intake`
  - `python -m heimdallr.prepare`
  - `python -m heimdallr.segmentation`
  - `python -m heimdallr.metrics`
  - `python -m heimdallr.dicom_egress`
  - `python -m heimdallr.integration_dispatcher`
  - `python -m heimdallr.integration_delivery`
  - `python -m heimdallr.space_manager`
  - `python -m heimdallr.resource_monitor`
  - `python -m heimdallr.tui`
- critical external dependencies:
  - DICOM peers/PACS for C-STORE intake and outbound C-STORE
  - `dcm2niix` for conversion
  - TotalSegmentator for segmentation tasks
  - SQLite filesystem access for queue and case state
  - host supervision (`systemd`, `launchd`, `skuld`, containers, or equivalent)

## System Overview

```text
PACS / Modality
      |
      | DICOM C-STORE
      v
heimdallr.intake
      |
      | ZIP + intake manifest
      v
runtime/intake/uploads/from_prepare/
runtime/intake/uploads/external/
      |
      v
heimdallr.prepare
      |  metadata/id.json, selected series, NIfTI, SQLite upsert
      |------------------> integration_dispatch_queue
      v
segmentation_queue
      |
      v
heimdallr.segmentation
      |  TotalSegmentator outputs, canonical NIfTI, coverage state
      v
metrics_queue
      |
      v
heimdallr.metrics
      |  deterministic metrics, overlays, PDF/SC artifacts
      |------------------> dicom_egress_queue
      |------------------> integration_delivery_queue
      v
runtime/studies/<case_id>/ + database/dicom.db
      |
      v
FastAPI dashboard/API + optional Textual TUI
```

## Component Maturity

| Component | Real status | Notes |
| --- | --- | --- |
| `heimdallr.intake` | operational | DICOM C-STORE SCP with idle study handoff and duplicate suppression state. |
| `heimdallr.prepare` | operational, complex hotspot | ZIP spool watcher, DICOM scan, metadata extraction, conversion, phase detection, queue enqueue. |
| `heimdallr.segmentation` | operational, dependency-heavy hotspot | Runs TotalSegmentator tasks from host-local profile; depends on binary/license/GPU or CPU capacity. |
| `heimdallr.metrics` | operational, mixed production/experimental surface | Production-facing jobs are enabled through `config/metrics_pipeline.example.json`; experimental jobs must stay opt-in. |
| `heimdallr.dicom_egress` | operational | Queue-driven C-STORE SCU with retry and compression fallback. |
| `heimdallr.integration_dispatcher` | operational if configured | Delivers patient-identified events to configured HTTP destinations. |
| `heimdallr.integration_delivery` | operational if configured | Sends final `manifest.json` and `package.zip` callbacks for external submissions. |
| `heimdallr.control_plane` | operational | FastAPI dashboard, upload ingress, patient/results API, PDF export. Built-in auth is not present. |
| `heimdallr.tui` | operational support tool | Reads SQLite/process state for live operations. |
| `heimdallr.space_manager` | operational guardrail | Purges completed study artifacts when configured disk thresholds are exceeded. |
| `heimdallr.resource_monitor` | operational telemetry | Samples service and case memory state into SQLite. |
| `heimdallr.deid_gateway` and LLM-related settings/deps | boundary hotspot | Existing compatibility residue around external model calls; do not expand here. Move future intelligence work to Asha. |

## Repository Layout

```text
Heimdallr/
├── heimdallr/                  # Main package and all production entrypoints
├── config/                     # Versioned JSON defaults and host-local examples
├── database/                   # SQLite schema and database documentation
├── docs/                       # Architecture, contracts, operations, decisions, API
├── scripts/                    # Operational and retroactive maintenance utilities
├── static/                     # Dashboard frontend and branding assets
├── tests/                      # unittest-based coverage for core workers/jobs
├── bin/                        # Bundled conversion/DICOM helper binaries and notices
├── runtime/                    # Mutable runtime state, ignored by Git
├── PROJECT_GATE.md             # Repository existence and boundary gate
├── START_CHECKLIST.md          # Current recovery checklist and next-round guardrails
├── CHANGELOG.md                # Human-readable project change history
└── AGENTS.md                   # Collaboration protocol for agents and maintainers
```

## Quick Start

### 1. Clone

```bash
git clone git@github.com:rod-americo/Heimdallr.git
cd Heimdallr
```

### 2. Prepare environment

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

All long-running services should use the same interpreter:
`.venv/bin/python`.

### 3. Configure

```bash
cp config/segmentation_pipeline.example.json config/segmentation_pipeline.json
cp config/metrics_pipeline.example.json config/metrics_pipeline.json
cp config/integration_dispatch.example.json config/integration_dispatch.json
cp config/integration_delivery.example.json config/integration_delivery.json
cp config/dicom_egress.example.json config/dicom_egress.json
cp config/presentation.example.json config/presentation.json
cp config/space_manager.example.json config/space_manager.json
cp config/resource_monitor.example.json config/resource_monitor.json
```

Only example files are versioned. The concrete host-local files above are
ignored by Git. Secrets and host-specific values must be injected by the host
environment, not by `.env` files.

For licensed TotalSegmentator tasks:

```bash
.venv/bin/totalseg_set_license -l YOUR_LICENSE_KEY
```

or inject `TOTALSEGMENTATOR_LICENSE` through host supervision.

### 4. Run

```bash
.venv/bin/python -m heimdallr.control_plane
.venv/bin/python -m heimdallr.prepare
.venv/bin/python -m heimdallr.segmentation
.venv/bin/python -m heimdallr.metrics
.venv/bin/python -m heimdallr.intake
.venv/bin/python -m heimdallr.dicom_egress
.venv/bin/python -m heimdallr.integration_dispatcher
.venv/bin/python -m heimdallr.integration_delivery
.venv/bin/python -m heimdallr.space_manager
.venv/bin/python -m heimdallr.resource_monitor
```

Run each resident service under its own terminal, supervisor unit, or container.
The TUI is optional:

```bash
.venv/bin/python -m heimdallr.tui
```

## Configuration

Configuration is centralized in `heimdallr/shared/settings.py` plus JSON files
under `config/`.

| Entry | Type | Required | Origin | Example |
| --- | --- | --- | --- | --- |
| `HEIMDALLR_SERVER_PORT` | env | no | host | `8001` |
| `HEIMDALLR_DICOM_PORT` | env | no | host | `11114` |
| `HEIMDALLR_INTAKE_PIPELINE_CONFIG` | env/file | no | repo or host | `config/intake_pipeline.json` |
| `HEIMDALLR_SEGMENTATION_PIPELINE_CONFIG` | env/file | yes for segmentation | host-local | `config/segmentation_pipeline.json` |
| `HEIMDALLR_METRICS_PIPELINE_CONFIG` | env/file | yes for metrics | host-local | `config/metrics_pipeline.json` |
| `HEIMDALLR_DICOM_EGRESS_CONFIG` | env/file | yes for egress | host-local | `config/dicom_egress.json` |
| `HEIMDALLR_INTEGRATION_DISPATCH_CONFIG` | env/file | no | host-local | `config/integration_dispatch.json` |
| `HEIMDALLR_INTEGRATION_DELIVERY_CONFIG` | env/file | no | host-local | `config/integration_delivery.json` |
| `TOTALSEGMENTATOR_LICENSE` | env | task-dependent | host secret | injected by supervisor |

See [`docs/OPERATIONS.md`](docs/OPERATIONS.md) for the full runtime and restart
model.

## Contracts and Boundaries

Canonical contracts are documented in [`docs/CONTRACTS.md`](docs/CONTRACTS.md).
High-value references:

- inbound DICOM studies grouped by `StudyInstanceUID`
- ZIP study payloads accepted by `/upload` and `/jobs`
- study directory state under `runtime/studies/<case_id>/`
- `metadata/id.json` and `metadata/resultados.json`
- queue tables in `database/dicom.db`
- outbound DICOM artifacts and external delivery callbacks

API details remain in [`docs/API.md`](docs/API.md). Database details remain in
[`database/README.md`](database/README.md).

## Validation

Minimum structural validation:

```bash
python3 scripts/check_project_gate.py
python3 scripts/project_doctor.py
python3 scripts/project_doctor.py --audit-config
```

Python syntax validation:

```bash
.venv/bin/python -m compileall heimdallr scripts tests
```

Relevant unit coverage can be run with:

```bash
.venv/bin/python -m unittest discover -s tests
```

Full end-to-end smoke requires a known non-PHI study payload, DICOM peer
configuration, conversion binaries, TotalSegmentator readiness, and enough
compute capacity. Do not treat unit tests as proof of clinical readiness.

## Governance

| Resource | Location |
| --- | --- |
| Project gate | [`PROJECT_GATE.md`](PROJECT_GATE.md) |
| Agent protocol | [`AGENTS.md`](AGENTS.md) |
| Start checklist | [`START_CHECKLIST.md`](START_CHECKLIST.md) |
| Architecture | [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) |
| Contracts | [`docs/CONTRACTS.md`](docs/CONTRACTS.md) |
| Operations | [`docs/OPERATIONS.md`](docs/OPERATIONS.md) |
| Decisions | [`docs/DECISIONS.md`](docs/DECISIONS.md) |
| API contracts | [`docs/API.md`](docs/API.md) |
| Database schema notes | [`database/README.md`](database/README.md) |
| Security policy | [`.github/SECURITY.md`](.github/SECURITY.md) |
| Third-party notices | [`NOTICE.md`](NOTICE.md) |

## License

Heimdallr is distributed under the [Apache License 2.0](LICENSE.md).

Third-party components carry additional licensing requirements. In particular,
TotalSegmentator requires a valid license for commercial use. Review
[`NOTICE.md`](NOTICE.md) before production deployment.
