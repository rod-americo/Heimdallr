# Bundled Binaries

This directory is reserved for operational binaries distributed with Heimdallr.

Current convention:

- `darwin-arm64/dcm2niix`: bundled macOS ARM/universal converter
- `linux-amd64/dcm2niix`: bundled Linux x86_64 converter
- `linux-amd64/dcmcjpeg`: bundled Linux x86_64 JPEG Lossless transcoder
- `licenses/dcm2niix-license.txt`: upstream license/notice file
- `licenses/dcmtk-license.txt`: upstream license/notice file for `dcmcjpeg`

When distributing `dcmcjpeg`, accompanying documentation should state:
"This product includes software based in part on the work of the Independent
JPEG Group."

Resolution order for `dcm2niix`:

1. `HEIMDALLR_DCM2NIIX_BIN`
2. platform-specific bundled binary under `bin/`
3. `bin/dcm2niix`
4. `dcm2niix` from the system `PATH`

Resolution order for `dcmcjpeg`:

1. `HEIMDALLR_DCMCJPEG_BIN`
2. platform-specific bundled binary under `bin/`
3. `bin/dcmcjpeg`
4. `dcmcjpeg` from the system `PATH`

If you commit a bundled binary here, keep the matching upstream license/notice
file alongside it.
