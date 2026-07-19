# Changelog

This file records notable behavior, architecture, documentation, and operation changes.

## [Unreleased]

### Added

- Added opt-in, versioned multi-acquisition CT anatomy evidence with
host-default/API-override resolution, independent queueing, immutable analysis
history, and `/api/v1/studies` read endpoints. MR studies receive inventory and
classification without a configured segmenter.

- Added `series_selection_policy` to external `/jobs` submissions so callers
can override series selection for a submitted study without changing the host
global profile.

### Changed

- The fixed native-kidney `<100 cm³` red alert is now suppressed for patients
  younger than 16 years at the study date, while renal volumes remain visible
  and the age-derived presentation decision is recorded in the metrics payload.
- Renal anatomy QC now recovers solitary L3-L4 components as native when they
  overlap a single contralateral kidney, publishes otherwise indeterminate
  solitary components without a native-volume alert, and continues to withhold
  unresolved multiple-component aggregates.
- CT preparation now pipelines conversion and phase detection through separate
  bounded pools, skips deterministic derived/localizer series from additional
  QC conversion while retaining them in the inventory, and reports phase wait
  separately from inference. GPU phase detection and segmentation can share an
  optional host-wide accelerator-slot admission limit.
- QC acquisition segmentation now enforces TotalSegmentator `total --ml` and
  stores one multilabel image per acquisition. Evidence extraction records only
  anatomy state, boundary contact, label identity, provenance, and execution
  timing; it no longer creates or repeatedly reads 117 binary masks.
- `vat_sat_ratio` now emits a burned-in Secondary Capture DICOM overlay when
  `emit_secondary_capture_dicom` is enabled, and the tracked metrics profile
  example enables that DICOM output for the production-facing VAT/SAT metric.
- Polished the README opening narrative and replaced the structural-baseline
status badge with a release badge.
- Lowered the tracked CT series-selection minimum from 120 to 60 slices.

## [0.2.1] - 2026-05-08

### Added

- Added project gate, structural doctor policy, and adapted starter-kit
governance baseline for the existing Heimdallr repository.
- Added explicit contracts, decisions, and recovery checklist documentation.
- Added POC host requirements audit documentation and runtime requirements
comparison script.
- Added a local DICOM ZIP metadata anonymization helper for host-only smoke
datasets.
- Added a GPU segmentation pipeline example for Thor smoke/runtime use.
- Added canonical `heimdallr.integration` package structure.
- Added consumer-facing integration contracts under
`heimdallr/integration/docs/`.
- Added caller-selectable integration package outputs, including metrics JSON,
overlays, report PDF, and report Encapsulated PDF DICOM.
- Added metrics-to-segmentation task requirements so external requested metrics
can avoid unnecessary TotalSegmentator tasks when the active profile declares those requirements.
- Added external job status lookup through `GET /jobs/{job_id}`.
- Added terminal `case.failed` callbacks for externally submitted jobs that
fail in prepare, segmentation, or metrics.

### Changed

- Reworked repository documentation around real Heimdallr services, runtime
paths, queue contracts, domain boundaries, and known hotspots.
- Marked stale prototype-fat test modules as skipped when the removed prototype
scripts are not present, instead of reconstructing deleted scripts.
- Updated agent instructions to require Git parity between local worktrees and
the `thor` POC host before host-side test comparisons.
- Set `en_US` as the default presentation locale for artifacts and TUI output.
- Documented ignored `runtime/test_datasets/` usage for large local smoke
fixtures.
- Documented that Thor segmentation smoke should use the GPU host-local profile,
not the CPU-first portable example.
- Consolidated integration documentation around `integration.dispatch`,
`integration.delivery`, and `integration.submissions`.
- Changed `bone_health_l1_hu` overlay generation to write `overlay.png` in
addition to `overlay_sc.dcm` when overlays are enabled.
- Clarified that integration consumers must use the delivery manifest, not
per-metric result artifacts, as the authoritative ZIP inventory.
- Restored the Heimdallr naming rationale and added conservative README badges.
- Aligned GitHub Actions with the declared Python 3.12 runtime and structural
gate checks.
- Updated the default control-plane title to radiological image MLOps wording.

### Removed

- Removed legacy `heimdallr.integration_dispatcher` and
`heimdallr.integration_delivery` compatibility module paths.
- Moved the external-model de-identification gateway residue to Asha.
- Removed unused `openai` and `anthropic` runtime dependency pins.

### Operational Notes

- This documentation/governance recovery does not change resident service
runtime behavior and does not require a service restart.
- Local hook support is provided but not automatically installed.
- Smoke datasets generated from clinical sources remain host-local and must not
be committed or published.
