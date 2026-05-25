# Desktop Agent Guidelines

These instructions apply to files under `desktop/`.

The desktop track is a macOS distribution and supervision layer. Preserve the
engine boundary:

- Swift UI belongs under `desktop/macos/`.
- Go daemon code belongs under `desktop/daemon/`.
- macOS packaging, LaunchAgent, signing, and notarization assets belong under
  `desktop/packaging/macos/`.
- Runtime and engine manifest examples belong under `desktop/manifests/`.
- Clinical pipeline behavior remains under `heimdallr/`.

Do not commit:

- `.app` bundles, `.dmg` files, notarization archives, or release zips;
- Developer ID certificates, provisioning profiles, API keys, passwords, or
  notarization credentials;
- managed Python runtimes, virtual environments, model weights, caches,
  SQLite databases, DICOM data, NIfTI outputs, logs, or host-local config.

When adding desktop code, update [`docs/DESKTOP.md`](../docs/DESKTOP.md) or the
local README when contracts, commands, build outputs, runtime paths, or
validation steps change.

Validation expectations:

- Go daemon changes: run `go test ./...` from `desktop/daemon/`.
- Swift app changes: run the documented Swift or Xcode build command.
- Packaging changes: run the local install/uninstall smoke when available.
- Python engine changes required by desktop: follow the repository-level
  validation rules in the root `AGENTS.md`.
