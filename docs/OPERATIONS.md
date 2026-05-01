# Operations

This runbook describes the real Heimdallr boot, configuration, runtime state,
validation, restart, and troubleshooting model.

## 1. Purpose

Heimdallr is operated as multiple independent Python services that share a
single package, SQLite database, runtime filesystem, and host-local
configuration. Operators should supervise each resident service separately.

## 2. Environments

| Environment | Purpose | Runtime | Notes |
| --- | --- | --- | --- |
| `local` | development and focused tests | Python 3.12 `.venv` | May run only one service or tests at a time. |
| `thor` | POC code-test host | `~/Heimdallr/.venv` | Keep Git state equal to local before comparing tests. |
| `validation` | controlled non-PHI or approved clinical validation | supervised Python `.venv` | Requires DICOM peer config, TotalSegmentator readiness, and documented run notes. |
| `production-like` | operational host under maintainer control | supervised Python `.venv` | Requires backup, restart policy, network controls, and smoke evidence. |

## 3. How to Run

### Local Boot

```bash
python3.12 -m venv .venv
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

Run each command in its own process supervisor unit, terminal, or container.
The optional dashboard TUI is:

```bash
.venv/bin/python -m heimdallr.tui
```

## 4. Operational Configuration

Configuration is centralized in `heimdallr/shared/settings.py` and JSON files
under `config/`.

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
- `TOTALSEGMENTATOR_LICENSE`

Runtime state:

- upload spool: `runtime/intake/uploads/`
- DICOM intake staging: `runtime/intake/dicom/`
- queue filesystem state: `runtime/queue/`
- study outputs: `runtime/studies/<case_id>/`
- SQLite database: `database/dicom.db`
- static dashboard assets: `static/`

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

When `resource_monitor` samples services installed as user-scoped systemd
units, prefix the unit name with `user:` in `config/resource_monitor.json`, for
example `user:heimdallr-segmentation.service`. Unprefixed names are read from
the system systemd manager.

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
- Control plane smoke: `curl -fsS http://localhost:8001/docs >/dev/null`
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

On `thor`, the current local smoke fixture is:

```text
runtime/test_datasets/prometheus_smoke/heimdallr_smoke_001_anonymized.zip
runtime/test_datasets/prometheus_smoke/heimdallr_smoke_001_manifest.json
```

It was generated from a local Prometheus ZIP using:

```bash
.venv/bin/python scripts/anonymize_dicom_zip.py SOURCE.zip \
  runtime/test_datasets/prometheus_smoke/heimdallr_smoke_001_anonymized.zip \
  --manifest runtime/test_datasets/prometheus_smoke/heimdallr_smoke_001_manifest.json
```

The helper performs metadata anonymization, rewrites DICOM UIDs, replaces direct
patient identifiers with `HEIMDALLR-SMOKE-001` / `Heimdallr^Smoke`, and writes
safe archive member names. It does not OCR-scrub burned-in pixel text; do not
publish the output or commit it to the repository.

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

Thor has an NVIDIA RTX 3090 and the in-repository `.venv` has CUDA-capable
PyTorch. Do not run large smoke segmentation with
`config/segmentation_pipeline.example.json`, because that example is CPU-first.
For Thor smoke and resident segmentation, create the ignored host-local config
from the GPU template:

```bash
cp config/segmentation_pipeline.gpu.example.json config/segmentation_pipeline.json
```

Then restart the segmentation worker so it reloads
`config/segmentation_pipeline.json`. The concrete `config/segmentation_pipeline.json`
file is host-local and must not be committed.

Do not mutate `thor` host config, runtime state, or the POC venv unless the
task explicitly calls for host-side changes.

## 6. Logs and Diagnosis

Current logging:

- resident worker stdout/stderr, line-buffered by
  `settings.configure_service_stdio()`
- per-case logs under `runtime/studies/<case_id>/logs/` where the worker writes
  them
- SQLite queue and status fields
- resource monitor samples in SQLite
- supervisor logs from `systemd`, `launchd`, `skuld`, containers, or terminals

Common diagnostic commands:

```bash
git status --short --branch
python3 scripts/project_doctor.py
sqlite3 database/dicom.db ".tables"
sqlite3 database/dicom.db "SELECT status, count(*) FROM segmentation_queue GROUP BY status;"
sqlite3 database/dicom.db "SELECT status, count(*) FROM metrics_queue GROUP BY status;"
sqlite3 database/dicom.db "SELECT status, count(*) FROM dicom_egress_queue GROUP BY status;"
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
| `heimdallr/integration/submissions.py` | restart control plane and prepare worker. |
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

Backup example:

```bash
sqlite3 database/dicom.db ".backup 'database/dicom_backup_$(date +%Y%m%d_%H%M%S).db'"
```

Restore example:

```bash
cp database/dicom_backup_<timestamp>.db database/dicom.db
sqlite3 database/dicom.db "PRAGMA integrity_check;"
```

Safe cleanup candidates:

- old failed upload ZIPs after investigation
- generated runtime artifacts only when `space_manager` policy or an operator
  decision says they are no longer needed
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
- Runtime requirements: `docs/RUNTIME_REQUIREMENTS.md`
- API: `docs/API.md`
- Database: `database/README.md`
- Validation stages: `docs/validation-stage-manual.md`
- Decisions: `docs/DECISIONS.md`
