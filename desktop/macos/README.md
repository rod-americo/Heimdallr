# macOS App

This directory is reserved for the future Swift menu bar app.

## Intended Responsibilities

- show current daemon and worker state;
- guide OsiriX/Horos inbound C-STORE setup;
- collect OsiriX/Horos egress destination settings;
- select segmentation profile and conservative device policy;
- start, stop, and restart services through `heimdallrd`;
- show log and validation summaries with actionable status.

## First UI Scope

The first proof of concept should include:

- menu bar status indicator;
- setup view for inbound Heimdallr DICOM settings;
- setup view for outbound OsiriX/Horos listener settings;
- segmentation profile picker;
- service controls;
- diagnostic actions for ports and DICOM connectivity;
- link to logs.

The app should treat the Go daemon as its backend. It should not start Python
workers directly once the daemon exists.
