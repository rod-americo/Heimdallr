# Architecture

## 1. Purpose

Heimdallr is the imaging infrastructure layer for receiving radiology studies,
preparing them for deterministic processing, running segmentation and metrics,
persisting operational state, and delivering generated artifacts. It exists to
keep DICOM/PACS operations, filesystem runtime state, SQLite queues,
TotalSegmentator orchestration, and deterministic calculations outside
proprietary reporting or intelligence repositories.

## 2. Scope

Includes:

- DICOM C-STORE intake and study handoff.
- ZIP upload intake for local and external study submissions.
- DICOM metadata extraction and DICOM-to-NIfTI conversion.
- Selected-series policy and phase/coverage metadata.
- TotalSegmentator task orchestration from host-local profiles.
- Deterministic metrics jobs and generated artifacts.
- SQLite queue and study state.
- DICOM egress, HTTP integration dispatch, and final package delivery.
- FastAPI dashboard/API and Textual operations dashboard.
- Runtime storage guard and resource monitor services.

Does not include:

- Proprietary clinical report drafting or final report generation.
- LLM, NLP, prompt-engineering, MedGemma, OpenAI, Anthropic, or other
  intelligence-layer workflows.
- Autonomous diagnosis or clinical decision automation.
- Checked-in PHI fixtures, DICOM samples, NIfTI outputs, local databases, or
  host secrets.

## 3. System Context

External inputs:

- PACS/modality DICOM C-STORE associations.
- `.zip` study uploads through `POST /upload`.
- external job submissions through `POST /jobs`.
- host-local JSON configuration files.
- environment variables injected by host supervision.

External outputs:

- generated study artifacts under `runtime/studies/<case_id>/`.
- SQLite state in `database/dicom.db`.
- DICOM C-STORE artifacts to configured remote SCP destinations.
- HTTP JSON event dispatches to configured destinations.
- multipart final package callbacks to external submitters.
- dashboard/API/TUI views of case and queue state.

Critical dependencies:

- Python 3.12 single `.venv`.
- `dcm2niix` and optional `dcmcjpeg` binaries.
- `python-gdcm`/`dicom2nifti` fallback path.
- TotalSegmentator runtime, license when task requires it, and CPU/GPU capacity.
- filesystem permissions for `runtime/` and `database/`.
- DICOM peer behavior and network reachability.

## 4. Main Modules

### 4.1 Composition Roots and Interfaces

Each resident service has its own module entrypoint:

- `heimdallr/control_plane/__main__.py`
- `heimdallr/intake/__main__.py`
- `heimdallr/prepare/__main__.py`
- `heimdallr/segmentation/__main__.py`
- `heimdallr/metrics/__main__.py`
- `heimdallr/dicom_egress/__main__.py`
- `heimdallr/integration/dispatch/__main__.py`
- `heimdallr/integration/delivery/__main__.py`
- `heimdallr/space_manager/__main__.py`
- `heimdallr/resource_monitor/__main__.py`
- `heimdallr/tui/__main__.py`

`heimdallr/control_plane/app.py` is the FastAPI composition root. It builds the
application, ensures runtime directories, includes routers, and mounts static
assets when present.

### 4.2 Orchestration Workers

- `heimdallr/intake/gateway.py`: DICOM SCP, study grouping, idle flush, ZIP
  handoff, duplicate handoff metadata.
- `heimdallr/prepare/worker.py`: upload spool claiming, DICOM scan, conversion,
  metadata persistence, `id.json` creation, dispatch enqueue, segmentation
  enqueue.
- `heimdallr/segmentation/worker.py`: queue claiming, selected-series reuse,
  TotalSegmentator tasks, canonical NIfTI materialization, metrics enqueue.
- `heimdallr/metrics/worker.py`: metrics profile loading, job dependency graph,
  deterministic job execution, artifacts, DICOM egress enqueue, final delivery
  enqueue.
- `heimdallr/dicom_egress/worker.py`: outbound C-STORE retry worker.
- `heimdallr/integration/dispatch/worker.py`: outbound JSON event retry
  worker.
- `heimdallr/integration/delivery/worker.py`: final package callback retry
  worker.
- `heimdallr/space_manager/worker.py`: disk threshold monitor and completed
  study purge.
- `heimdallr/resource_monitor/worker.py`: process and case memory telemetry
  sampler.

### 4.3 Shared Infrastructure

- `heimdallr/shared/settings.py`: environment parsing, config paths, runtime
  paths, binary resolution, service stdio behavior.
- `heimdallr/shared/paths.py`: canonical study path helpers.
- `heimdallr/shared/store.py`: SQLite schema, migrations, queue lifecycle,
  retry/claim helpers, resource monitor persistence.
- `heimdallr/shared/sqlite.py`: SQLite connection helper.
- `heimdallr/shared/spool.py`: atomic spool file operations.
- `heimdallr/shared/study_manifest.py`: intake manifest fingerprinting.
- `heimdallr/integration/submissions.py`: `/jobs` payload and sidecar helpers.
- `heimdallr/integration_dispatcher/` and `heimdallr/integration_delivery/`:
  legacy compatibility shims for previous module entrypoints.

### 4.4 Deterministic Domain Logic

The code does not currently expose a separate `domain/` package. Domain rules
are concentrated in:

- series selection rules in `config/series_selection.json` and segmentation
  worker helpers.
- metrics jobs under `heimdallr/metrics/jobs/`.
- analysis helpers under `heimdallr/metrics/analysis/`.
- patient-name presentation helpers under `heimdallr/shared/patient_names.py`.
- de-identification helper behavior under `heimdallr/deid_gateway.py`.

Future extraction should be behavior-driven and tested, not directory-first.

## 5. Main Flow

1. A study arrives through DICOM C-STORE, `/upload`, or `/jobs`.
2. Intake or the control plane writes a ZIP into the upload spool.
3. `prepare` claims a stable ZIP, extracts it, scans DICOM metadata, filters
   candidate series, converts DICOM to NIfTI, writes study metadata, and
   enqueues segmentation.
4. `segmentation` claims the case, resolves the active segmentation profile,
   selects the target series, runs or reuses TotalSegmentator outputs, writes
   pipeline state, and enqueues metrics.
5. `metrics` claims the case, resolves the active metrics profile, executes
   enabled jobs and dependencies, writes `metadata/resultados.json`, creates
   artifacts, and enqueues outbound delivery where configured.
6. `dicom_egress`, `integration.dispatch`, and `integration.delivery` drain
   their queues independently.
7. `control_plane` and `tui` read SQLite and runtime files to expose current
   state to operators.
8. `space_manager` and `resource_monitor` provide operational guardrails around
   storage and memory.

## 6. Contracts and Invariants

- canonical input: DICOM study instances or ZIP study payload.
- canonical output: `runtime/studies/<case_id>/` plus SQLite state.
- primary DICOM identifier: `StudyInstanceUID`.
- primary Heimdallr operational identifier: `case_id`.
- external submitter identifier: `job_id` and caller-owned `client_case_id`.

Invariants:

- Do not assume `StudyInstanceUID`, `case_id`, accession number, and
  `client_case_id` are equivalent.
- Host-local operational config remains ignored; examples are the tracked
  contract.
- Queue claim, heartbeat, retry, and completion semantics live in
  `heimdallr/shared/store.py`.
- `metadata/id.json` and `metadata/resultados.json` are externally meaningful
  artifacts and require contract updates when their shape changes.
- Metrics additions must update the tracked example profile when production
  facing.

## 7. Persistence

- main database: `database/dicom.db` (ignored by Git).
- schema and migrations: `heimdallr/shared/store.py`, with reference schema in
  `database/schema.sql`.
- runtime state: `runtime/` (ignored by Git).
- case workspace: `runtime/studies/<case_id>/`.
- upload spool: `runtime/intake/uploads/`.
- queue filesystem paths: `runtime/queue/pending/`, `runtime/queue/active/`,
  `runtime/queue/failed/`.

SQLite queue tables include:

- `segmentation_queue`
- `metrics_queue`
- `integration_dispatch_queue`
- `integration_delivery_queue`
- `dicom_egress_queue`

## 8. Configuration

Configuration sources:

- `HEIMDALLR_*` environment variables.
- versioned JSON defaults:
  - `config/intake_pipeline.json`
  - `config/series_selection.json`
- host-local JSON files created from examples:
  - `config/segmentation_pipeline.json`
  - `config/metrics_pipeline.json`
  - `config/dicom_egress.json`
  - `config/integration_dispatch.json`
  - `config/integration_delivery.json`
  - `config/presentation.json`
  - `config/space_manager.json`
  - `config/resource_monitor.json`

`.env` files and `python-dotenv` are not part of the architecture.

## 9. Observability

Current observability:

- FastAPI OpenAPI/docs endpoint for API surface.
- SQLite queues and metadata tables.
- per-case pipeline logs under study runtime directories.
- resident worker stdout/stderr with line buffering.
- `heimdallr.resource_monitor` samples stored in SQLite.
- Textual TUI for live queue/case state.

Known gap:

- logging is not uniformly structured JSON across all resident workers.

Minimum health signals:

- `GET http://localhost:8001/docs` when the control plane is running.
- DICOM listener accepts associations on the configured AE/port.
- SQLite queue rows move from pending/claimed to done or failed.
- generated artifacts appear under `runtime/studies/<case_id>/`.
- outbound queues drain when destinations are configured and reachable.

## 10. Hotspots and Debt

- `prepare`, `segmentation`, and `metrics` workers are large orchestration
  modules with broad side effects.
- Queue semantics are centralized in `store.py`; small changes can affect many
  workers.
- FastAPI endpoints do not include built-in authentication.
- Runtime state currently lives inside the repository worktree by default,
  though ignored.
- LLM-adjacent dependencies/settings remain as compatibility residue and should
  not grow inside Heimdallr.
- Full end-to-end validation depends on external DICOM peers, TotalSegmentator,
  and non-PHI imaging samples.

## 11. Open Decisions

- Whether to introduce a central structured logger across resident workers.
- Whether to move operational SQLite/runtime state outside the worktree by
  default for managed deployments.
- Whether to remove unused LLM-adjacent dependencies after an impact audit.
- Whether to enforce gate/doctor in CI in addition to local hook support.
