# Operations

This runbook describes the real Heimdallr boot, configuration, runtime state, validation, restart, and troubleshooting model.

## 1. Purpose

Heimdallr is operated as multiple independent Python services that share a single package, SQLite database, runtime filesystem, and host-local configuration. Operators should supervise each resident service separately.

## 2. Environments

| Environment | Purpose | Runtime | Notes |
| --- | --- | --- | --- |
| `local` | development and focused tests | Python 3.14 `.venv` | May run only one service or tests at a time. |
| `thor` | POC code-test host | `~/Heimdallr/.venv` | Keep Git state equal to local before comparing tests. |
| `validation` | controlled non-PHI or approved clinical validation | supervised Python `.venv` | Requires DICOM peer config, TotalSegmentator readiness, and documented run notes. |
| `production-like` | operational host under maintainer control | supervised Python `.venv` | Requires backup, restart policy, network controls, and smoke evidence. |
| `desktop-poc` | planned local macOS desktop proof of concept | Swift app + Go daemon + managed Python runtime | Not notarized initially; must keep mutable state under Application Support. |

## 3. How to Run

### Local Boot

```bash
python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Create host-local config files from examples when the related service is used:

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

### Primary Boot

```bash
.venv/bin/python -m heimdallr.control_plane
.venv/bin/python -m heimdallr.prepare
.venv/bin/python -m heimdallr.segmentation
.venv/bin/python -m heimdallr.metrics
.venv/bin/python -m heimdallr.intake
.venv/bin/python -m heimdallr.dicom_egress
.venv/bin/python -m heimdallr.integration.dispatch
.venv/bin/python -m heimdallr.integration.delivery
.venv/bin/python -m heimdallr.space_manager
.venv/bin/python -m heimdallr.resource_monitor
```

Run each command in its own process supervisor unit or terminal.
Restart `heimdallr.metrics` after deploying metrics job or metrics overlay code
changes so resident workers render new payload and DICOM artifact behavior.

`heimdallr.dicom_egress` starts a configurable worker pool for outbound C-STORE
delivery. Set `worker_count` in `config/dicom_egress.json` to control concurrent
queue items; the default is 10. Generated DICOM artifacts should prefer
JPEG-LS Lossless compression for storage. During C-STORE delivery, egress
negotiates the peer's accepted presentation context and transcodes only for
transfer when needed.
The optional dashboard TUI is:

```bash
.venv/bin/python -m heimdallr.tui
```

A simpler queue-first TUI, styled after the WebRISAhead operational console, is:

```bash
.venv/bin/python -m heimdallr.tui.simple
```

The compact queue TUI lists the 20 most recent studies in the upper queue table
with stable visible indexes (`01`, `02`, ...). Press `q` to exit, `r` to refresh,
`pNN` to prioritize a visible queued study as the next pending queue item, or
`xNN` to cancel one and remove it from the active pipeline queues, for example
`p10` or `x04`. In the processed table, `Pipeline` is the sum of active prepare,
segmentation, and metrics elapsed times; `Duration` is the end-to-end elapsed
time when recorded by the pipeline.

## 4. Operational Configuration

Configuration is centralized in `heimdallr/shared/settings.py` and JSON files under `config/`.

Critical environment variables:

- `HEIMDALLR_SERVER_HOST`
- `HEIMDALLR_SERVER_PORT`
- `HEIMDALLR_AE_TITLE`
- `HEIMDALLR_DICOM_PORT`
- `HEIMDALLR_DICOM_HANDOFF_MODE`
- `HEIMDALLR_INTAKE_PIPELINE_CONFIG`
- `HEIMDALLR_SERIES_SELECTION_CONFIG`
- `HEIMDALLR_SEGMENTATION_PIPELINE_CONFIG`
- `HEIMDALLR_METRICS_PIPELINE_CONFIG`
- `HEIMDALLR_DICOM_EGRESS_CONFIG`
- `HEIMDALLR_INTEGRATION_DISPATCH_CONFIG`
- `HEIMDALLR_INTEGRATION_DELIVERY_CONFIG`
- `HEIMDALLR_PRESENTATION_CONFIG`
- `HEIMDALLR_SPACE_MANAGER_CONFIG`
- `HEIMDALLR_RESOURCE_MONITOR_CONFIG`
- `HEIMDALLR_DCM2NIIX_BIN`
- `HEIMDALLR_DCMCJPEG_BIN`
- `HEIMDALLR_TOTALSEG_GET_PHASE_DEVICE`
- `HEIMDALLR_TOTALSEG_GET_PHASE_TIMEOUT_SECONDS`
- `HEIMDALLR_TOTALSEG_GET_PHASE_THREAD_LIMIT`
- `HEIMDALLR_TOTALSEG_GET_PHASE_MAX_PARALLEL`
- `TOTALSEGMENTATOR_LICENSE`

Runtime state:

- upload spool: `runtime/intake/uploads/`
- DICOM intake staging: `runtime/intake/dicom/`
- queue filesystem state: `runtime/queue/`
- study outputs: `runtime/studies/<case_id>/`
- prepared source DICOM series:
  `runtime/studies/<case_id>/source/dicom/series/<series-stem>/`
- SQLite database: `database/dicom.db`
- static dashboard assets: `static/`

For the planned macOS desktop track, managed user state should live under
`~/Library/Application Support/Heimdallr/` instead of the app bundle. The
desktop track is documented in `docs/DESKTOP.md`; until code lands there, the
canonical operation model remains the supervised Python worker model above.

Tracked config:

- `config/intake_pipeline.json`
- `config/series_selection.json`
- `config/*.example.json`
- `config/doctor.json`

Ignored host-local config:

- `config/segmentation_pipeline.json`
- `config/metrics_pipeline.json`
- `config/dicom_egress.json`
- `config/integration_dispatch.json`
- `config/integration_delivery.json`
- `config/presentation.json`
- `config/space_manager.json`
- `config/resource_monitor.json`
- `config/host_stack/*.json`

When `resource_monitor` samples services installed as user-scoped systemd units, prefix the unit name with `user:` in `config/resource_monitor.json`, for example `user:heimdallr-segmentation.service`. Unprefixed names are read from the system systemd manager.

`config/space_manager.json` controls runtime study retention. The resident
worker scans `runtime/studies` every `scan_interval_seconds` and deletes the
oldest purge-eligible study directories until all enabled limits are satisfied:

- `usage_threshold_percent`: maximum filesystem usage percentage; set `0` to disable.
- `minimum_free_gb`: minimum free space to preserve; set `0` to disable.
- `max_resident_studies`: maximum resident study directories; set `0` to disable.
- `max_case_age_days`: maximum study directory age by mtime; set `0` to disable.

Active queue items in segmentation, metrics, or DICOM egress remain protected
from purge while their queue status is `pending` or `claimed`.

Project presentation default:

- `en_US` is the default artifact and TUI locale.
- `pt_BR` remains a supported locale for explicit host-local overrides and i18n
tests.

## 5. Minimum Validation

```bash
python3 scripts/check_project_gate.py && python3 scripts/project_doctor.py && python3 scripts/project_doctor.py --audit-config
```

Additional checks by change type:

- Python syntax: `.venv/bin/python -m compileall heimdallr scripts tests`
- Unit tests: `.venv/bin/python -m unittest discover -s tests`
- Host stack guardrail: `.venv/bin/python scripts/check_host_stack_manifest.py`
- Control plane smoke: `curl -fsS http://localhost:8001/docs >/dev/null`
- Queue capacity smoke: `curl -fsS http://localhost:8001/ops/queues`
- DICOM listener smoke: send a known non-PHI DICOM sample with a DCMTK tool such
  as `dcmsend localhost 11114 -aec HEIMDALLR sample.dcm`
- SQLite integrity: `sqlite3 database/dicom.db "PRAGMA integrity_check;"`

End-to-end smoke is only meaningful when a known non-PHI study, DICOM peer
configuration, conversion binaries, TotalSegmentator readiness, and compute
capacity are available.

### Local Smoke Datasets

Large DICOM smoke datasets must stay outside Git. Use ignored runtime storage
for host-local fixtures:

```bash
runtime/test_datasets/
```

Manual prepare runs against ZIP files outside the upload spool preserve the
input archive after processing. ZIPs claimed from
`runtime/intake/uploads/from_prepare/`, `runtime/intake/uploads/external/`, or
the legacy `runtime/intake/uploads/` root remain pipeline transport artifacts
and are deleted after successful prepare.

`prepare` runs `totalseg_get_phase` once per converted CT series. Set
`HEIMDALLR_TOTALSEG_GET_PHASE_DEVICE` explicitly per host:

- `thor`: `gpu`
- Linux CPU hosts: `cpu`
- local macOS/MPS host: `cpu`

Do not use `mps` for `totalseg_get_phase` on the local macOS stack until that
upstream path is validated; it has crashed in local testing. On macOS, keep
`HEIMDALLR_TOTALSEG_GET_PHASE_THREAD_LIMIT=1` so the CPU path does not overrun
PyTorch/nnU-Net worker threads. Use
`HEIMDALLR_TOTALSEG_GET_PHASE_MAX_PARALLEL=1` on the local Apple Silicon stack
because concurrent phase-detector subprocesses can still fan out into multiple
PyTorch/nnU-Net child processes even when CPU thread pools are bounded. When
unset, Heimdallr defaults the phase device to `cpu` on macOS, applies thread
limit `1`, and runs one phase subprocess at a time.

On `thor`, the current local smoke fixture is:

```text runtime/test_datasets/prometheus_smoke/heimdallr_smoke_001_anonymized.zip runtime/test_datasets/prometheus_smoke/heimdallr_smoke_001_manifest.json
```

It was generated from a local Prometheus ZIP using:

```bash
.venv/bin/python scripts/anonymize_dicom_zip.py SOURCE.zip \
  runtime/test_datasets/prometheus_smoke/heimdallr_smoke_001_anonymized.zip \
  --manifest runtime/test_datasets/prometheus_smoke/heimdallr_smoke_001_manifest.json
```

The helper performs metadata anonymization, rewrites DICOM UIDs, replaces direct patient identifiers with `HEIMDALLR-SMOKE-001` / `Heimdallr^Smoke`, and writes safe archive member names. It does not OCR-scrub burned-in pixel text; do not publish the output or commit it to the repository.

When a smoke dataset needs pixel-level burned-in text screening, run OCR with a
local `tesseract` binary and write the report under ignored runtime storage:

```bash
.venv/bin/python scripts/verify_dicom_burned_in_text.py \
  runtime/test_datasets/head_complete/head_complete_001_anonymized.zip \
  --report runtime/test_datasets/head_complete/head_complete_001_ocr_report.json
```

By default the OCR report stores hashes and lengths, not recognized text, to
avoid re-exposing possible identifiers. Use `--include-text` only for local
manual investigation in ignored paths. OCR can miss small, rotated, or
low-contrast text and should complement manual review rather than replace it.

### Thor POC Validation

Before using `thor`, ensure local and host Git states match:

```bash
git status --short --branch
git rev-parse --short HEAD
ssh thor 'cd ~/Heimdallr && git status --short --branch && git rev-parse --short HEAD'
```

Expected: same branch, same upstream target, same commit hash, and no unexpected
worktree changes on either side.

Use the current POC venv for code tests:

```bash
ssh thor 'cd ~/Heimdallr && .venv/bin/python --version'
ssh thor 'cd ~/Heimdallr && .venv/bin/python -m pip check'
ssh thor 'cd ~/Heimdallr && .venv/bin/python scripts/check_runtime_requirements.py'
```

Thor has an NVIDIA RTX 3090 and the in-repository `.venv` has CUDA-capable PyTorch. Do not run large smoke segmentation with `config/segmentation_pipeline.example.json`, because that example is CPU-first. For Thor smoke and resident segmentation, create the ignored host-local config from the GPU template:

```bash cp config/segmentation_pipeline.gpu.example.json config/segmentation_pipeline.json
```

Then restart the segmentation worker so it reloads
`config/segmentation_pipeline.json`. The concrete `config/segmentation_pipeline.json`
file is host-local and must not be committed.

The segmentation worker passes each task's configured `extra_args` through to
TotalSegmentator, including `total`. Use host-local segmentation profiles for
host-specific accelerator choices:

- `thor`: `total` with `--device gpu`, without `--fast`.
- local macOS/Odin: `total` with `--fast --device mps`.
- CPU POC: `total` with `--fast --device cpu`.

The tracked automatic CT examples use `ct_automatic_segmentation` and
`ct_automatic_metrics` by default. The segmentation worker runs `total` first,
writes `artifacts/segmentation_inventory.json`, and uses that inventory to
select compatible metrics and any additional segmentation tasks. L3-dependent
body jobs require a complete L3 mask, organ volumetry requires at least one
present parenchymal organ mask, and `cerebral_bleed` plus `brain_structures`
require a complete `total/brain.nii.gz` mask. The `total/skull.nii.gz` mask is
optional crop and diagnostic context; skull truncation is reported but does not
block the head workflow.
For dedicated complete-head CT validation, hosts can still use profiles derived
from the tracked examples:

- `HEIMDALLR_SEGMENTATION_PIPELINE_PROFILE=ct_head_complete_segmentation`
- `HEIMDALLR_METRICS_PIPELINE_PROFILE=ct_head_complete_metrics`

That dedicated path runs TotalSegmentator `total` for the `skull` and `brain`
ROI subset plus `cerebral_bleed` and `brain_structures`, then runs
`head_complete_qc` to validate the `brain` mask without scan-bound truncation
and produce the normalized axial head CT NIfTI artifact, canonical RAS 2 mm
NIfTI artifact,
brain-mask geometry 1 mm slice-spacing NIfTI artifact, derived axial CT DICOM
series encoded with the job's `derived_ct_transfer_syntax` while preserving
source in-plane pixel spacing, advancing 1 mm between images, and tagging 2 mm nominal slice
thickness. The derived CT stack is exported in spatial order so viewers detect a
constant slice interval, with the brain-center slice tagged but not reordered.
It also emits translated
brain-structure volume-table DICOM, and 3 mm burned-in overlay series for
`brain_structures` rendered on the brain-geometry normalized CT grid without a
text panel. The volume table includes the overlay color map. The
brain-mask geometry artifact uses `total/brain.nii.gz`
to define the output plane, uses `brain_structures/septum_pellucidum.nii.gz`
as an in-plane midline guide when available, and does not require the
orbitomeatal line. When the
cerebral-bleed mask is positive, the job also emits a 5 mm burned-in bleed
overlay series on the brain-geometry normalized CT grid containing positive
slabs plus adjacent slabs, without a text panel, using a red transparent
contour for the positive mask, and sets
`measurement.cerebral_bleed.has_cerebral_bleed=true`.
`brain_structures` is a licensed TotalSegmentator task in 2.13.0, so the
segmentation worker needs a valid license before this profile is used. Restart
segmentation and metrics workers after changing either profile.

Do not mutate `thor` host config, runtime state, or the POC venv unless the
task explicitly calls for host-side changes.

### Host Stack Guardrails

Keep one ignored manifest per known host under `config/host_stack/`:

- `odin.json`: local macOS MPS host; `mps` preferred, `cpu` allowed as fallback.
- `thor.json`: CUDA POC host; `gpu` required for segmentation tasks.
- `ms-heimdallr.json`: CPU POC host; only `cpu` is allowed.

Before changing host-local segmentation or metrics profiles, run:

```bash
.venv/bin/python scripts/check_host_stack_manifest.py
```

For stored manifests that describe another host from the current machine, use
manifest-only validation:

```bash
.venv/bin/python scripts/check_host_stack_manifest.py \
  --manifest config/host_stack/thor.json \
  --skip-hostname-check \
  --manifest-only
```

The guardrail fails when the active segmentation profile uses a device outside
the host policy, when segmentation concurrency exceeds the host limit, or when
metrics parallelism exceeds the host limit.

## 6. Logs and Diagnosis

Current logging:

- resident worker stdout/stderr, line-buffered by
  `settings.configure_service_stdio()`
- per-case logs under `runtime/studies/<case_id>/logs/` where the worker writes
  them
- SQLite queue and status fields
- resource monitor samples in SQLite
- supervisor logs from `systemd`, `launchd`, `skuld`, or terminals

Common diagnostic commands:

```bash
git status --short --branch
python3 scripts/project_doctor.py
sqlite3 database/dicom.db ".tables"
sqlite3 database/dicom.db "SELECT status, count(*) FROM segmentation_queue GROUP BY status;"
sqlite3 database/dicom.db "SELECT status, count(*) FROM metrics_queue GROUP BY status;"
sqlite3 database/dicom.db "SELECT status, count(*) FROM dicom_egress_queue GROUP BY status;"
curl -fsS http://localhost:8001/ops/queues
find runtime/intake/uploads -maxdepth 2 -type f | sort | tail
find runtime/studies -maxdepth 2 -type d | sort | tail
```

Common failure signals:

- ZIP remains in upload spool and `prepare` is not running or cannot claim it.
- queue rows stay `claimed` until claim TTL recovery on restart.
- `segmentation` fails because TotalSegmentator binary, license, or compute is
unavailable.
- `metrics` skips jobs because masks or required profile inputs are missing.
- `dicom_egress` retries because the remote SCP rejects association, syntax, or
generated artifact type.
- `integration_delivery` retries because callback URL is unreachable or
returns non-2xx.

## 7. Restart Policy

| Change | Restart impact |
| --- | --- |
| `heimdallr/shared/settings.py` | restart all resident services that read settings at import. |
| `heimdallr/shared/store.py` | restart all workers and control plane after schema/queue changes. |
| `heimdallr/intake/` | restart intake listener. |
| `heimdallr/prepare/` | restart prepare worker. |
| `heimdallr/segmentation/` | restart segmentation worker. |
| `heimdallr/metrics/` | restart metrics worker. |
| `heimdallr/dicom_egress/` | restart DICOM egress worker. |
| `heimdallr/integration/dispatch/` | restart integration dispatch worker. |
| `heimdallr/integration/delivery/` | restart integration delivery worker. |
| `heimdallr/integration/delivery/package.py` | restart integration delivery worker before validating callback package contents. |
| `heimdallr/integration/submissions.py` | restart control plane, prepare worker, and segmentation worker when submission metadata affects selection behavior. |
| `heimdallr/control_plane/` or `static/` | restart control plane; browser refresh may be needed. |
| `config/series_selection.json` | restart prepare/segmentation services that load selection behavior. |
| `config/metrics_pipeline.json` | restart metrics worker; restart segmentation worker too because external requested metrics can narrow segmentation tasks from this profile. |
| host-local pipeline config | restart the affected worker unless the code explicitly reloads it per cycle. |
| docs-only or governance-script changes | no resident service restart. |

Recommended restart order for a full stack restart:

1. stop intake first to prevent new handoffs
2. stop prepare, segmentation, metrics, delivery, egress, monitors
3. restart control plane if changed
4. start prepare, segmentation, metrics, delivery, egress, monitors
5. start intake last

## 8. Persistence, Backup, and Cleanup

Primary state:

- `database/dicom.db`
- `runtime/studies/<case_id>/`
- host-local config files under `config/`

Prepared case workspaces include source DICOM instances grouped by series under
`source/dicom/series/`, derived NIfTI files, segmentation masks, metrics,
generated artifacts, logs, and metadata. The upload ZIP is deleted after prepare
and is not the retained reprocessing source.

Backup example:

```bash sqlite3 database/dicom.db ".backup 'database/dicom_backup_$(date +%Y%m%d_%H%M%S).db'"
```

Restore example:

```bash
cp database/dicom_backup_<timestamp>.db database/dicom.db
sqlite3 database/dicom.db "PRAGMA integrity_check;"
```

Safe cleanup candidates:

- old failed upload ZIPs after investigation
- generated runtime artifacts and prepared source DICOM series only when
`space_manager` policy or an operator decision says the whole case workspace is
no longer needed
- local caches outside tracked source

Never remove without explicit intent:

- `database/dicom.db`
- host-local config JSON
- active queue files or active study directories
- evidence packages for validation runs

## 9. Troubleshooting Checklist

1. Confirm which service owns the failing stage.
2. Confirm the service process is running under the expected `.venv`.
3. Confirm host-local config exists and is valid JSON.
4. Check queue status counts in SQLite.
5. Inspect the relevant case directory under `runtime/studies/<case_id>/`.
6. Check worker output and per-case logs.
7. Confirm external dependency reachability: DICOM peer, callback URL,
TotalSegmentator binary/license, conversion binaries.
8. Confirm filesystem permissions for `runtime/` and `database/`.
9. Run structural validation if docs/contracts were changed.
10. Capture exact error text before cleaning failed runtime state.

## 10. Operational Scripts

| Script | Purpose |
| --- | --- |
| `scripts/check_project_gate.py` | Validate repository gate completeness. |
| `scripts/project_doctor.py` | Validate structural documentation coherence. |
| `scripts/check_runtime_requirements.py` | Compare active Python environment against `requirements.txt`. |
| `scripts/check_host_stack_manifest.py` | Validate host-local accelerator and worker guardrails. |
| `scripts/verify_dicom_burned_in_text.py` | OCR-screen local DICOM smoke datasets for burned-in text. |
| `scripts/install_git_hooks.sh` | Opt-in local pre-commit hook installation. |
| `scripts/retroactive_recalculate_metrics.py` | Regenerate metrics for existing cases. |
| `scripts/consolidate_metrics_csv.py` | Export metrics database to CSV. |
| `scripts/extract_prometheus_bmd.py` | Extract BMD values for Prometheus ingestion. |
| `scripts/update_kvp_retroactive.py` | Backfill kVp values from DICOM metadata. |
| `scripts/bmd_roi_comparison_preview.py` | Visual preview of BMD ROI placement. |
| `scripts/retroactive_emphysema.py` | Legacy compatibility wrapper for emphysema recalculation. |
| `scripts/watch_heimdallr.py` | Filesystem watcher for upload from a drop folder. |

## 11. Related Documents

- Architecture: `docs/ARCHITECTURE.md`
- Contracts: `docs/CONTRACTS.md`
- Desktop track: `docs/DESKTOP.md`
- Runtime requirements: `docs/RUNTIME_REQUIREMENTS.md`
- API: `docs/API.md`
- Database: `database/README.md`
- Validation stages: `docs/validation-stage-manual.md`
- Decisions: `docs/DECISIONS.md`
