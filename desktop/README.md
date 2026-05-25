# Heimdallr Desktop

This directory contains the planned macOS desktop wrapper for Heimdallr. The
desktop track is intentionally part of the monorepo during the first
implementation phase so the app, daemon, packaging, and Python engine contracts
can evolve together.

The desktop track must stay a wrapper and supervisor around the engine. It must
not reimplement clinical pipeline behavior.

## Layout

```text
desktop/
├── AGENTS.md
├── README.md
├── daemon/
├── macos/
├── manifests/
└── packaging/
    └── macos/
```

## Responsibilities

Desktop code may:

- provide a Swift menu bar application;
- provide a Go daemon that supervises Heimdallr workers;
- create managed local configuration from tracked examples;
- guide OsiriX/Horos setup;
- start, stop, restart, and diagnose local services;
- package a private runtime for end users.

Desktop code must not:

- move the maintained engine out of `heimdallr/`;
- write production pipeline code as loose top-level files;
- commit virtual environments, model weights, databases, runtime studies,
  secrets, Apple certificates, provisioning profiles, or notarized products;
- expand LLM, NLP, prompt, report drafting, or intelligence-layer behavior.

## Current Status

This is a planning scaffold. The first implementation should create:

1. a Go daemon skeleton under `desktop/daemon/`;
2. a Swift menu bar app skeleton under `desktop/macos/`;
3. local packaging scripts under `desktop/packaging/macos/`;
4. a managed runtime manifest under `desktop/manifests/`;
5. the smallest Python engine adjustments needed for an Application Support
   runtime root.

See [`docs/DESKTOP.md`](../docs/DESKTOP.md) for the end-to-end execution plan.
