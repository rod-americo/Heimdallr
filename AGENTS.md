# Heimdallr Agentic Guidelines

This repository is frequently edited by AI agents and coding assistants. Read and follow this file before making code or documentation changes.

Heimdallr is an existing radiological image MLOps stack. Do not treat it as a greenfield starter and do not reconstruct services that were deleted, split, or relocated in the past.

## 1. Minimum Reading Order

Before significant changes, read in this order:

1. `README.md`
2. `PROJECT_GATE.md`
3. `docs/ARCHITECTURE.md`
4. `docs/CONTRACTS.md`
5. `docs/OPERATIONS.md`
6. `docs/DECISIONS.md`
7. the module or worker you intend to edit
8. the related tests under `tests/`

When touching persistence, queues, metrics profiles, or host-local config templates, also read:

- `database/README.md`
- `database/schema.sql`
- `config/*.example.json`
- `config/series_selection.json`
- `config/intake_pipeline.json`

## 2. Domain Boundary

Heimdallr is strictly focused on open-source radiological image infrastructure:

- DICOM C-STORE intake and outbound C-STORE delivery
- DICOM-to-NIfTI preparation
- deterministic study metadata extraction and queue state
- TotalSegmentator orchestration
- deterministic quantitative metrics and generated artifacts
- SQLite state, runtime filesystem paths, operational dashboards, and workers

Do not add or revive:

- proprietary clinical support services
- final report drafting services
- prompt engineering, LLM orchestration, NLP, OpenAI, Anthropic, MedGemma, or
intelligence-layer workflows
- advanced assisted report conversion routines for external model APIs

Those responsibilities belong to Asha. Existing compatibility residue around external model settings must not be expanded in this repository without an explicit architectural decision.

## 3. Layer and Module Rules

The current maintained layout is `heimdallr/`, not `src/heimdallr/`. Preserve it unless packaging requirements are intentionally redesigned.

Use the existing module boundaries:

- `heimdallr/control_plane/`: FastAPI app, dashboard routes, upload ingress,
patient/results API, deterministic PDF export.
- `heimdallr/intake/`: DICOM listener and study handoff.
- `heimdallr/prepare/`: ZIP spool consumer, metadata extraction, DICOM-to-NIfTI
conversion, phase detection, segmentation enqueue.
- `heimdallr/segmentation/`: TotalSegmentator task orchestration, selected
series reuse, canonical NIfTI, segmentation queue handling.
- `heimdallr/metrics/`: deterministic post-segmentation jobs and generated
artifacts.
- `heimdallr/dicom_egress/`: outbound DICOM C-STORE queue worker.
- `heimdallr/integration/`: external job submission helpers, outbound event
dispatch, and final package callback delivery.
- `heimdallr/space_manager/`: disk usage guard for `runtime/studies/`.
- `heimdallr/resource_monitor/`: memory telemetry sampler.
- `heimdallr/shared/`: settings, paths, SQLite store, schemas, i18n, spool
helpers, and cross-worker utilities.

Do not create production code as loose top-level files. New runtime behavior belongs in the relevant `heimdallr/` module with tests and docs updated in the same change.

## 4. Configuration, Runtime, and Persistence

- Never create `.env` files.
- Never add `python-dotenv`.
- Secrets and host-specific values must be injected by the host supervisor,
runtime supervisor, or environment.
- Version examples such as `config/*.example.json`.
- Keep host-local operational files ignored, including:
  - `config/segmentation_pipeline.json`
  - `config/metrics_pipeline.json`
  - `config/dicom_egress.json`
  - `config/integration_dispatch.json`
  - `config/integration_delivery.json`
  - `config/presentation.json`
  - `config/space_manager.json`
  - `config/resource_monitor.json`
  - `config/qc_evidence.json`
  - `config/host_stack/*.json`
- `config/metrics_pipeline.json` is host-local. When adding a production
metrics module, update `config/metrics_pipeline.example.json` in the same change.
- Default presentation locale for this project is `en_US`. Keep `pt_BR` as a
supported locale, not as the default.
- Mutable data belongs in `runtime/` or `database/*.db`, both ignored by Git.
- Operational bundles or transfer artifacts must not include `runtime/`,
database files, host-local config JSON, PHI-bearing datasets, or local virtual
environments.

## 5. Documentation Rules

- Human-facing repository documentation must be in en-US.
- Technical identifiers, code symbols, environment variables, filenames, and
commit messages must be in en-US.
- Update docs in the same change when behavior, entrypoints, contracts,
runtime paths, restart policy, or operational commands change.
- Keep the runbook in `docs/OPERATIONS.md`; do not duplicate full operational
instructions in module-specific notes unless the module needs a narrow supplement.
- Keep API details in `docs/API.md` and database details in `database/README.md`;
link them from structural docs instead of copying them wholesale.
- Keep external integration consumer contracts in `heimdallr/integration/docs/`.
Update them in the same change when `/jobs`, callback delivery, outbound events, payload fields, retry semantics, or integration security assumptions change.

## 6. Minimum Validation

Minimum validation command: `python3 scripts/check_project_gate.py && python3 scripts/project_doctor.py && python3 scripts/project_doctor.py --audit-config`

Before concluding any change:

- run the minimum structural validation above
- run syntax validation for changed Python files
- run focused tests for touched modules
- run broader `.venv/bin/python -m unittest discover -s tests` when shared behavior,
queues, metrics contracts, or worker orchestration changed and dependencies are available
- review `git status --short --branch` and `git diff`
- state what was validated and what could not be validated

If the change affects resident services, document restart impact in `docs/OPERATIONS.md`.

## 7. Thor POC Host Protocol

The host accelerator policy is fixed unless the user explicitly changes it:

- local `odin`: `mps` for TotalSegmentator segmentation tasks
- `thor`: `gpu`
- `ms-heimdallr`: `cpu`

The local `odin` phase detector uses MPS with one internal thread after the
TotalSegmentator 2.15.0 validation; see `docs/OPERATIONS.md` before changing
`totalseg_get_phase` device settings.

Use `thor` as the POC code-test host when the task requires host-level validation. The current project venv is:

```text /home/rodrigo/Heimdallr/.venv
```

Before comparing test results, local and `thor` must have the same Git branch,
upstream, commit, and expected worktree cleanliness:

```bash
git status --short --branch
git rev-parse --short HEAD
ssh thor 'cd ~/Heimdallr && git status --short --branch && git rev-parse --short HEAD'
```

Keep local, `thor`, and `ms-heimdallr` moving together during active code-test work. Prefer
small, reviewable commits, and create those commits automatically when a change
is ready unless the user asks to hold them uncommitted. After every local push,
update both `thor` and `ms-heimdallr` with `git pull --ff-only` so all three
repositories keep the same commit. Before pulling `ms-heimdallr`, fetch and
inspect its worktree plus incoming changes under `config/`. Never edit
host-local configuration on `ms-heimdallr` as part of routine synchronization.
Warn the user before pulling when a tracked config change could alter or impair
that host's CPU pipeline; stop and resolve the risk instead of applying a
questionable config change. Resident services on the local host and on `thor`
may be restarted when needed for a code or configuration change to take effect.
Do not restart services on `ms-heimdallr`, or edit its code, host-local config,
runtime state, or venv, unless the user explicitly requests that host-side
operation.

Use `docs/RUNTIME_REQUIREMENTS.md` and `scripts/check_runtime_requirements.py` when auditing or rebuilding Python environments.

Thor has an NVIDIA RTX 3090. Do not use `config/segmentation_pipeline.example.json` for large Thor segmentation smoke; that template is CPU-first. Use ignored host-local `config/segmentation_pipeline.json` created from `config/segmentation_pipeline.gpu.example.json`, then restart the segmentation worker before timing or validating segmentation behavior.

## 8. Known Hotspots

- `heimdallr/prepare/worker.py`: large orchestration surface for ZIP claiming,
DICOM scan, metadata extraction, conversion, queue enqueue, and duplicate suppression.
- `heimdallr/segmentation/worker.py`: process supervision, claim heartbeat,
TotalSegmentator execution, output reuse, and canonical artifact layout.
- `heimdallr/metrics/worker.py`: dynamic job loading, dependency graph
execution, generated artifacts, DICOM egress enqueue, external delivery enqueue.
- `heimdallr/shared/store.py`: SQLite schema creation, migration, queue
lifecycle, retry semantics, and resource monitor state.
- `config/metrics_pipeline.example.json`: production metrics profile contract;
`requires_segmentation_tasks` also affects external requested-metrics segmentation planning.
- `config/series_selection.json`: selected-series behavior; changes can alter
clinical measurement inputs.
- `docs/API.md`, `docs/CONTRACTS.md`, and `database/README.md`: must stay
consistent with API and SQLite behavior.
- LLM-adjacent dependencies/settings: existing residue only; do not expand.

## 9. Change Guardrails

- Preserve current service boundaries unless an explicit decision is recorded.
- Do not refactor directories just to match a starter-kit shape.
- Do not invent readiness, coverage, authentication, monitoring, or deployment
guarantees that the code does not implement.
- Do not replace host-local JSON configuration with checked-in concrete
operational config.
- Do not hide experimental metrics in the default production-facing profile.
- Do not move complexity from code into documentation without changing the real
behavior.
- Do not overwrite local runtime state, database files, or ignored host config.
- Do not revert user changes in the worktree.

## 10. Current Open Guardrails

The structural recovery baseline is established. Remaining gaps are operational or validation boundaries, not startup blockers:

- Shared worker logging is not yet fully structured across all resident
services.
- Built-in authentication for the FastAPI control plane is not implemented in
this repository; use the network, gateway, VPN, or deployment boundary.
- End-to-end smoke against the local anonymized sample on `thor` is still a
separate validation step.
- Full unit test execution depends on local dependency availability.
- Clinical validation remains outside automated repository checks.

## 11. Git Workflow

Commit directly on `main` for Heimdallr work. Do not create or switch branches
unless the user explicitly asks for that branch workflow.

Commits should be short, focused, and automatic once a change is ready unless
the user asks to hold the work uncommitted. After every local push, pull the same
commit on both `thor` and `ms-heimdallr` with `git pull --ff-only`, subject to
the `ms-heimdallr` configuration-impact audit above.

Commit messages must be en-US, imperative, and semantic:

```text type(scope): summary
```

Allowed common types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`,
`perf`, `ci`, `build`, `revert`.

Keep subjects at 72 characters or less and never include PHI/PII, dumps, local
secrets, or patient identifiers.

## 12. Local Gate and Doctor

Use:

```bash
python3 scripts/check_project_gate.py
python3 scripts/project_doctor.py
python3 scripts/project_doctor.py --strict
python3 scripts/project_doctor.py --audit-config
```

Policy file:

```text config/doctor.json
```

If `project_doctor.py` reports a semantic false positive, prefer a small
`token_alias_groups` entry with a clear reason in nearby docs. Use
`ignored_warnings` only for a conscious divergence, and re-run
`python3 scripts/project_doctor.py --audit-config`.
