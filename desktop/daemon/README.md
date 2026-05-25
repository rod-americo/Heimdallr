# Go Daemon

This directory is reserved for `heimdallrd`, the future Go supervisor used by
the macOS desktop app.

## Intended Responsibilities

- prepare and validate managed runtime directories;
- supervise Heimdallr Python workers;
- render host-local config files from tracked examples and UI settings;
- expose a localhost API for the Swift app;
- run health checks, port checks, C-ECHO checks, and log collection;
- install, remove, and inspect the user LaunchAgent through packaging helpers.

## Initial API Surface

The first daemon API should stay local-only and narrow:

```text
GET  /v1/status
GET  /v1/services
POST /v1/services/start
POST /v1/services/stop
POST /v1/services/restart
GET  /v1/config
PUT  /v1/config/dicom
PUT  /v1/config/segmentation
POST /v1/validate/inbound-dicom
POST /v1/validate/egress-dicom
GET  /v1/logs
```

The API must not expose patient identifiers or case-specific paths unless a
later explicit contract allows it. Queue summaries should prefer the existing
non-identifying operational capacity contract.

## Development Notes

The first implementation may support a development checkout path. Release
builds should use a manifest-pinned engine artifact and a private runtime under
`~/Library/Application Support/Heimdallr/`.
