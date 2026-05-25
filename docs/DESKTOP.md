# Desktop Track

This document defines the first monorepo plan for the macOS desktop shell. The
desktop track exists to make Heimdallr easier to install, configure, supervise,
and validate on a local macOS workstation without moving clinical pipeline logic
out of the maintained Python engine.

The current repository remains the canonical Heimdallr engine. The desktop
track is a wrapper, supervisor, and packaging surface.

## 1. Product Goal

The target user should be able to install `Heimdallr.app`, open a menu bar
application, configure OsiriX/Horos DICOM send and receive settings, select a
segmentation profile, and keep the Heimdallr workers running as resident local
services with clear status and actionable diagnostics.

The preferred user flow is:

1. Install and open `Heimdallr.app`.
2. Let the app prepare or verify its managed runtime.
3. Configure inbound DICOM settings for OsiriX/Horos.
4. Configure outbound DICOM egress back to OsiriX/Horos.
5. Select the active segmentation profile and device policy.
6. Start resident services.
7. Validate C-STORE intake and egress with built-in checks.

Notarization is not required for the first local proof of concept. The packaging
track should still preserve a clean path to Developer ID signing and
notarization because the maintainer has an Apple Developer account.

## 2. Repository Layout

Desktop-specific files live under `desktop/`:

```text
desktop/
├── README.md                  # Desktop track overview and boundaries.
├── AGENTS.md                  # Local instructions for agents editing desktop code.
├── daemon/                    # Future Go `heimdallrd` supervisor.
├── macos/                     # Future Swift menu bar app.
├── manifests/                 # Versioned runtime and engine manifest examples.
└── packaging/
    └── macos/                 # LaunchAgent, bundle, signing, and release notes.
```

Keep production pipeline behavior in `heimdallr/`. Desktop code must not
reimplement series selection, segmentation planning, metrics, artifact
generation, queue lifecycle, or DICOM clinical behavior.

## 3. Runtime Model

The desktop distribution should not require users to install Heimdallr manually.
It should manage a private runtime:

```text
/Applications/Heimdallr.app
  Swift menu bar UI
  bundled heimdallrd supervisor
  bootstrap and diagnostics helpers

~/Library/Application Support/Heimdallr/
  engine/<version>/
  python/
  config/
  runtime/
  database/
  logs/
  models/
  cache/
```

The first proof of concept may point the daemon at a local checkout through an
explicit development setting. Release builds should install a pinned engine
artifact from a manifest and keep mutable state under Application Support.

Do not write mutable desktop runtime state inside the app bundle or the source
checkout. Do not commit managed Python runtimes, model weights, databases,
study artifacts, host-local JSON files, Apple certificates, provisioning
profiles, or notarized build products.

## 4. Component Boundaries

### Swift Menu Bar App

Responsibilities:

- show service health, queue summaries, and recent diagnostic state;
- guide OsiriX/Horos inbound and egress configuration;
- collect user-facing settings and send them to `heimdallrd`;
- expose start, stop, restart, open logs, and validation actions;
- show segmentation profile choices without exposing raw JSON first.

Non-responsibilities:

- running TotalSegmentator directly;
- writing SQLite queue state;
- parsing clinical DICOM content beyond display-safe configuration checks;
- generating clinical artifacts.

### Go Daemon

Responsibilities:

- supervise Heimdallr Python worker processes;
- create and validate managed directories;
- write host-local config files from versioned examples and UI settings;
- expose a localhost API for the Swift app;
- run process, port, C-ECHO, C-STORE smoke, and runtime checks;
- manage user LaunchAgent installation in the macOS packaging track;
- maintain logs and service state suitable for operator diagnosis.

Non-responsibilities:

- reimplementing Heimdallr queue semantics from `heimdallr/shared/store.py`;
- changing clinical processing behavior;
- storing secrets in tracked files.

### Python Engine

The Python engine remains authoritative for:

- DICOM C-STORE intake and handoff;
- ZIP prepare, DICOM scan, NIfTI conversion, and series selection;
- TotalSegmentator orchestration;
- deterministic metrics and artifact generation;
- DICOM egress queue behavior;
- SQLite schema and queue lifecycle.

Desktop-driven changes to the Python engine should be limited to contracts that
make supervised desktop operation cleaner, such as explicit runtime root
configuration, local-only health endpoints, and stable status output.

## 5. OsiriX/Horos Wizard Contract

The desktop UI should display inbound settings for sending studies to
Heimdallr:

```text
Destination name: Heimdallr
Address: <this Mac IP address or 127.0.0.1 for same-host testing>
Port: 11114
Called AE Title: HEIMDALLR
Protocol: DICOM C-STORE
```

The UI should collect outbound egress settings for returning generated DICOM
artifacts to OsiriX/Horos:

```text
OsiriX/Horos listener host: 127.0.0.1 or this Mac IP address
OsiriX/Horos listener port: <listener port configured in OsiriX/Horos>
OsiriX/Horos local AE Title: <local AE title configured in OsiriX/Horos>
Heimdallr calling AE Title: HEIMDALLR
Preferred artifact transfer syntax: JPEG-LS Lossless
```

The daemon should translate those settings into the managed
`dicom_egress.json` equivalent:

```json
{
  "local_ae_title": "HEIMDALLR",
  "destinations": [
    {
      "name": "osirix_local",
      "enabled": true,
      "host": "127.0.0.1",
      "port": 11112,
      "called_aet": "OSIRIX",
      "artifact_types": ["secondary_capture"]
    }
  ]
}
```

The UI should include validation actions for:

- inbound listener port availability;
- C-ECHO to Heimdallr when available through a local helper;
- outbound C-ECHO to OsiriX/Horos;
- a small non-PHI C-STORE egress test when a safe fixture is available.

## 6. Segmentation Profile UX

The first UI should map human choices to existing profile names and device
policy:

| UI choice | Engine profile target | Notes |
| --- | --- | --- |
| CT automatic | `ct_automatic_segmentation` | Default broad CT profile. |
| CT native only | `ct_native_segmentation_only` | Native-phase constrained profile. |
| Complete head CT | `ct_head_complete_segmentation` | Requires careful license and artifact expectations. |

Device choices should be conservative:

- CPU: always available when runtime dependencies are installed, slowest path.
- MPS: experimental until a controlled smoke validates the selected
  TotalSegmentator tasks on the local Apple Silicon stack.
- CUDA: not a local macOS target, but may remain available through Thor or a
  future remote worker model outside the initial desktop scope.

The desktop UI should clearly mark licensed TotalSegmentator tasks and should
store license material through macOS Keychain or another host secret mechanism,
not a tracked file.

## 7. Multi-Agent Execution Plan

Use separate agents with narrow ownership. Agents may work in parallel only when
their output contracts are already agreed and they do not edit the same files.

### Agent A: Architecture and Contracts

Scope:

- keep `docs/DESKTOP.md`, `docs/ARCHITECTURE.md`, `docs/CONTRACTS.md`, and
  `docs/OPERATIONS.md` consistent;
- define desktop-to-daemon and daemon-to-engine contracts;
- decide which Python engine changes are required before code lands.

Deliverables:

- desktop architecture decision records;
- daemon API contract;
- managed runtime path contract;
- restart and validation impact notes.

Validation:

- structural gate and project doctor;
- documentation review against AGENTS boundaries.

### Agent B: Python Engine Adaptation

Scope:

- add runtime-root configurability where the current worktree assumptions block
  managed Application Support operation;
- add narrow health/status endpoints or CLI status helpers if needed;
- preserve all existing worker entrypoints and queue semantics.

Deliverables:

- engine changes with focused tests;
- updated config examples when behavior changes;
- no desktop-specific clinical logic.

Validation:

- minimum structural validation;
- Python syntax validation for changed files;
- focused tests for touched modules;
- broader unit discovery when shared settings, queues, or worker orchestration
  change.

### Agent C: Go Daemon

Scope:

- create `heimdallrd`;
- supervise the Python engine processes;
- generate managed host-local config files;
- expose the localhost API consumed by Swift;
- implement service and runtime diagnostics.

Deliverables:

- Go module under `desktop/daemon/`;
- process supervisor;
- config renderer;
- local API;
- daemon tests.

Validation:

- `go test ./...`;
- local daemon start/stop smoke against a development checkout;
- process cleanup checks.

### Agent D: Swift macOS App

Scope:

- build a menu bar app under `desktop/macos/`;
- call the daemon API;
- provide OsiriX/Horos setup, segmentation profile selection, service controls,
  logs, and validation flows.

Deliverables:

- Swift package or Xcode project;
- menu bar UI;
- settings and status screens;
- user-facing copy in en-US for the repository version.

Validation:

- local build;
- UI smoke against a mock daemon;
- manual screenshot review for layout and text fit.

### Agent E: Packaging and Managed Runtime

Scope:

- build the local development bundle;
- define engine manifests;
- create LaunchAgent templates;
- manage Python runtime bootstrap;
- prepare the later signing and notarization lane.

Deliverables:

- packaging scripts under `desktop/packaging/macos/`;
- `LaunchAgent` template;
- manifest resolver;
- non-notarized local build procedure;
- signing/notarization checklist for later execution.

Validation:

- install and uninstall smoke on local macOS;
- no mutable state inside the `.app`;
- no secrets or runtime artifacts in Git.

### Agent F: DICOM Integration Validation

Scope:

- validate OsiriX/Horos wizard assumptions;
- test inbound C-STORE to Heimdallr;
- test outbound egress to OsiriX/Horos;
- record transfer syntax behavior and fallback requirements.

Deliverables:

- DICOM validation checklist;
- safe non-PHI fixture expectations;
- observed AE title, port, and transfer syntax compatibility notes.

Validation:

- C-ECHO and C-STORE smoke with non-PHI data;
- SQLite queue state review;
- artifact receipt verification in OsiriX/Horos.

### Agent G: Privacy, Security, and Release Readiness

Scope:

- audit local storage paths for PHI risk;
- ensure secrets stay in host secret stores;
- review local network exposure;
- prepare Developer ID signing and notarization when requested.

Deliverables:

- privacy checklist;
- firewall and localhost binding recommendations;
- release risk notes;
- notarization checklist.

Validation:

- no PHI fixtures or databases committed;
- no certificates, tokens, provisioning profiles, or local config committed;
- app transport and local API exposure reviewed.

## 8. End-to-End Milestones

### Milestone 0: Repository Preparation

Status: this document and the `desktop/` scaffold.

Exit criteria:

- desktop track has documented boundaries;
- structural gate passes;
- no runtime behavior changes.

### Milestone 1: Development Runtime Contract

Exit criteria:

- daemon can run against a local checkout;
- managed paths are documented;
- engine can be launched with explicit config, runtime, and database paths;
- start/stop leaves no orphaned worker processes.

### Milestone 2: Go Daemon Proof of Concept

Exit criteria:

- daemon starts control plane, intake, prepare, segmentation, metrics, and egress
  in development mode;
- daemon reports worker status and log locations;
- daemon writes generated host-local config under a temporary managed root;
- `go test ./...` passes.

### Milestone 3: Swift Menu Bar Proof of Concept

Exit criteria:

- app shows daemon status;
- app can start and stop the daemon in development mode;
- app displays OsiriX/Horos inbound and egress instructions;
- app can save segmentation profile choices through the daemon.

### Milestone 4: DICOM Local Loop

Exit criteria:

- OsiriX/Horos can send a non-PHI study to Heimdallr using the wizard settings;
- Heimdallr processes at least through prepare on the managed runtime;
- generated DICOM artifacts can be sent back to OsiriX/Horos when metrics
  produce them;
- failures are visible in the app with actionable logs.

### Milestone 5: Managed Python Runtime

Exit criteria:

- app installs or verifies a private Python runtime;
- engine version is pinned by manifest;
- dependency audit passes;
- TotalSegmentator binary and model cache are discoverable;
- license handling does not use tracked files.

### Milestone 6: Local Unsigned App Bundle

Exit criteria:

- build creates a local `.app` for testing;
- LaunchAgent can start the daemon at login when enabled;
- uninstall removes LaunchAgent and leaves user data only when requested;
- no notarization required for this milestone.

### Milestone 7: Signed and Notarized Distribution

Exit criteria:

- Developer ID signing works;
- notarization succeeds;
- Gatekeeper launch is verified on a clean macOS user profile;
- release notes document runtime size, first-run downloads, and DICOM firewall
  requirements.

## 9. Initial Backlog

- Define daemon localhost API shapes for status, services, config, validation,
  and logs.
- Add engine support for a single managed runtime root if existing environment
  variables are insufficient.
- Create the Go daemon module and process supervisor skeleton.
- Create the Swift menu bar shell with mock daemon data.
- Add LaunchAgent template and install/uninstall scripts.
- Build OsiriX/Horos C-ECHO validation helper strategy.
- Decide how to package or download Python 3.14 and heavyweight dependencies.
- Decide how model weights are downloaded, cached, and verified.
- Add a local non-PHI desktop smoke fixture strategy without committing DICOM
  data.
- Prepare signing and notarization checklist, but do not require it for the
  first proof of concept.

## 10. Validation Expectations

Every desktop change should state which layer was validated:

- repository structural validation;
- Python engine syntax and tests when touched;
- `go test ./...` when Go daemon code is touched;
- Swift build or Xcode build when macOS app code is touched;
- packaging smoke when LaunchAgent or bundle scripts are touched;
- DICOM smoke only with approved non-PHI fixtures.

Desktop validation does not prove clinical readiness. Clinical validation
remains outside automated repository checks.
