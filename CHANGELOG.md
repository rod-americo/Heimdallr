# Changelog

This file records notable behavior, architecture, documentation, and operation
changes.

## [Unreleased]

### Added

- Added project gate, structural doctor policy, and adapted starter-kit
  governance baseline for the existing Heimdallr repository.
- Added explicit contracts, decisions, and recovery checklist documentation.
- Added POC host requirements audit documentation and runtime requirements
  comparison script.
- Added a local DICOM ZIP metadata anonymization helper for host-only smoke
  datasets.
- Added a GPU segmentation pipeline example for Thor smoke/runtime use.

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

### Operational Notes

- This documentation/governance recovery does not change resident service
  runtime behavior and does not require a service restart.
- Local hook support is provided but not automatically installed.
- Smoke datasets generated from clinical sources remain host-local and must not
  be committed or published.
