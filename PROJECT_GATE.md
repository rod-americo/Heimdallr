# Project Gate

This gate records why Heimdallr exists as a separate repository and which boundaries prevent uncontrolled scope growth.

## 1. Why does this project exist?

- real problem: Radiology imaging workflows need a governed open-source
infrastructure layer for receiving DICOM studies, preparing them for segmentation, running deterministic image-processing pipelines, and preserving auditable state.
- target user or operator: A technical operator or radiology engineering owner
responsible for PACS connectivity, study processing, queue health, runtime artifacts, and deterministic quantitative outputs.
- expected result: A host can run the Heimdallr workers and control plane to
transform incoming imaging studies into traceable metadata, NIfTI files, segmentation artifacts, metrics payloads, outbound DICOM artifacts, and integration callbacks.

## 2. Why should this NOT be only a module?

- candidate repository that could absorb this: Asha could consume Heimdallr
outputs, but it should not own DICOM intake, segmentation workers, PACS egress, SQLite queues, or runtime artifact lifecycle.
- why that coupling would be inadequate: Merging imaging infrastructure with intelligence or reporting code would couple PACS operations, GPU-heavy segmentation, clinical-adjacent artifacts, external model experiments, and report workflows into one failure domain.
- boundary that justifies a separate repository: Heimdallr owns imaging MLOps infrastructure and deterministic artifacts, while Asha owns proprietary clinical assistance, LLM, NLP, and final report intelligence workflows.

## 3. What does this project share with the ecosystem?

- configuration: Versioned JSON examples under `config/`, host-local JSON
overrides ignored by Git, and `HEIMDALLR_*` environment variables injected by the host.
- logging: Resident workers currently use line-buffered service output and
per-case logs; structured logging is a known improvement area rather than a solved platform guarantee.
- runtime: A single Python 3.12 `.venv`, host supervision, mutable `runtime/`
paths, bundled helper binaries under `bin/`, and SQLite state under `database/dicom.db`.
- contracts: DICOM studies, ZIP study payloads, `metadata/id.json`,
`metadata/resultados.json`, SQLite queue rows, generated artifacts, HTTP upload/job APIs, outbound webhook events, and DICOM C-STORE deliveries.
- authentication or transport: DICOM association metadata, FastAPI endpoints
without built-in auth middleware, optional bearer upload token for intake HTTP handoff, host/network controls, HTTP callbacks, and outbound C-STORE.

## 4. What can this project NOT carry?

- responsibilities out of scope: Proprietary report drafting, LLM orchestration,
prompt engineering, clinical narrative generation, and autonomous diagnosis must stay outside Heimdallr.
- integrations that belong to another system: OpenAI, Anthropic, MedGemma,
NLP/reporting assistants, and other intelligence-layer clients belong in Asha or another explicit consumer repository.
- data that should not live here: Real PHI fixtures, checked-in DICOM payloads,
generated NIfTI files, local SQLite databases, host secrets, callback tokens, model weights, and runtime queues must not be versioned.

## 5. What is the expected maintenance cost?

- primary host or environment: A supervised Python 3.12 host with filesystem
access, SQLite write access, DICOM networking, conversion binaries, and enough CPU/GPU capacity for segmentation.
- most fragile external dependency: TotalSegmentator runtime readiness,
licensing, compute capacity, and DICOM peer compatibility are the highest operational dependencies.
- restart need: Python worker code changes require restarting the affected
resident process; shared settings, store, or contract changes may require a coordinated restart of all services.
- backup need: `database/dicom.db` and selected `runtime/studies/<case_id>/`
artifacts require host-level backup policy when they are operationally authoritative.
- operational risk: A queue, segmentation, DICOM peer, or storage failure can
leave cases stuck, partially processed, duplicated, or unable to deliver artifacts even when the API remains reachable.

## 6. Exit Condition

This repository should continue to exist only while:

- it has a defensible imaging infrastructure scope
- canonical inputs and outputs remain identifiable
- it owns runtime services or evolution cycles that should not be coupled to
Asha
- the cost of a separate repository is lower than the risk of mixing image
MLOps, PACS operations, and report intelligence in one codebase
