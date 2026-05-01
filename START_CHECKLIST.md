# Start Checklist

This checklist tracks the structural recovery baseline for the existing
Heimdallr repository. It is not a new-project checklist; completed items reflect
what was observed or added during recovery.

## 0. Repository Existence

- [x] Repository scope is explicit in `README.md`.
- [x] Non-scope is explicit and separates Heimdallr from Asha.
- [x] `PROJECT_GATE.md` explains why this should remain a repository rather
  than only a module in a reporting or intelligence codebase.

## 1. Baseline Documentation

- [x] `README.md` describes identity, boundaries, entrypoints, config, and
  validation.
- [x] `AGENTS.md` defines reading order, layer rules, validation, and hotspots.
- [x] `PROJECT_GATE.md` is filled with repository-specific answers.
- [x] `CHANGELOG.md` exists.
- [x] `docs/ARCHITECTURE.md` maps the real system.
- [x] `docs/CONTRACTS.md` documents inputs, outputs, identifiers, queues, and
  integrations.
- [x] `docs/OPERATIONS.md` describes boot, config, runtime, smoke, restart,
  backup, and troubleshooting.
- [x] `docs/DECISIONS.md` records current architectural decisions.

## 2. Structure

- [x] Main production package remains `heimdallr/`.
- [x] Root stays intentionally small; production code is not spread across
  loose top-level modules.
- [x] Tests already exist under `tests/`.
- [x] Versioned config examples exist under `config/`.
- [x] Mutable runtime state is expected under ignored `runtime/`.
- [ ] Shared worker logging is not yet fully structured across all resident
  services.

## 3. Configuration and Runtime

- [x] `.env` files are forbidden by repository policy.
- [x] Host-local operational JSON files are ignored.
- [x] `config/metrics_pipeline.example.json` is the tracked metrics profile
  template.
- [x] Runtime paths are centralized in `heimdallr/shared/settings.py` and
  `heimdallr/shared/paths.py`.
- [x] SQLite schema creation and migration live in `heimdallr/shared/store.py`.
- [ ] Authentication for the FastAPI control plane is not implemented in this
  repository and must be handled at the network/gateway boundary.

## 4. Contracts

- [x] Canonical DICOM, ZIP, JSON, SQLite, queue, artifact, and callback
  contracts are documented.
- [x] `StudyInstanceUID`, `case_id`, `job_id`, and queue IDs are called out as
  distinct identifiers.
- [x] Experimental metrics boundaries are documented.
- [x] LLM/reporting boundaries are documented as out of scope.
- [ ] End-to-end smoke evidence for a non-PHI sample study is not stored in the
  repository.

## 5. Validation

- [x] `scripts/check_project_gate.py` exists.
- [x] `scripts/project_doctor.py` exists.
- [x] `config/doctor.json` exists.
- [x] Optional local hook files exist.
- [ ] Full unit test execution still depends on local dependency availability.
- [ ] Clinical validation remains outside the scope of automated repository
  checks.

## 6. Next-Round Guardrails

- Do not reorganize the package into `src/` without a packaging decision and a
  migration plan.
- Do not add concrete host-local config files to Git.
- Do not add LLM/NLP/report intelligence to Heimdallr.
- Do not declare operational readiness without service supervision, backup,
  smoke evidence, and incident paths.
- Do not add production metrics jobs without updating
  `config/metrics_pipeline.example.json`, tests, and contracts.
- Do not treat unit tests as clinical validation.
