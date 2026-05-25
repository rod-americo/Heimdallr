# Desktop Manifests

This directory contains tracked manifest examples for the desktop-managed
runtime. Concrete host-local manifests and downloaded runtime assets must stay
outside Git.

The manifest pins the engine version, expected Python version, worker
entrypoints, bundled helper binaries, and managed runtime layout used by the
daemon and packaging scripts.

Use `*.example.json` for tracked examples. Do not commit concrete manifests
that contain local paths, credentials, PHI, or machine-specific state.
