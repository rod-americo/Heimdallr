# API Contracts

This document summarizes high-value API contracts used in Heimdallr workflows.

## Base URL

- Local default: `http://localhost:8001`
- OpenAPI schema/docs: `GET /docs`

## Versioning and Compatibility

- Current contract line: `v1` (path is unversioned today).
- Backward-compatible changes may be shipped in-place.
- Breaking changes should be documented in the affected API and operations docs before release.

## Authentication and Access Model

- Local development mode currently exposes endpoints without built-in auth middleware.
- Production deployment should enforce access controls at gateway/reverse proxy and network boundary.
- For sensitive environments, isolate dashboard/API access to trusted networks and audited identities.

## Core Endpoints

### Upload and Tooling

- `POST /jobs`
- `GET /jobs/{job_id}`
- `POST /upload`
- `GET /api/tools/uploader`

`POST /upload` accepts a `.zip` payload and hands it off asynchronously to the `heimdallr.prepare` flow.

`POST /jobs` accepts `multipart/form-data` with:
- `study_file`
- `client_case_id`
- `callback_url`
- optional `source_system`
- optional `requested_outputs` JSON, which selects returned files such as
metrics JSON, overlays, report PDF, and report Encapsulated PDF DICOM
- optional `requested_metrics_modules` (JSON array or CSV string), which
selects enabled metrics jobs to run from the active profile

It returns an immediate acceptance payload with `job_id` and `status=queued`. `GET /jobs/{job_id}` returns the best available asynchronous status for that external job. When processing finishes, `heimdallr.integration.delivery` performs an outbound multipart callback. Successful jobs emit `case.completed` with `manifest.json` plus `package.zip`; terminal failed jobs emit `case.failed` with `manifest.json` and no package. The consumer-facing callback contract is documented in `heimdallr/integration/docs/JOB_SUBMISSION.md`.

When `requested_outputs` is provided, omitted output keys are treated as `false`. Consumers should request every file type they expect in the final package.

If `requested_metrics_modules` is provided, Heimdallr constrains the case to that subset of metrics jobs and automatically includes declared dependencies from the active metrics profile. When metrics jobs declare `requires_segmentation_tasks`, segmentation is also constrained to the required TotalSegmentator tasks.

### Patients and Results

- `GET /api/patients`
- `GET /api/patients/{case_id}/results`
- `GET /api/patients/{case_id}/metadata`
- `GET /api/patients/{case_id}/nifti`
- `GET /api/patients/{case_id}/download/{folder_name}`
- `GET /api/patients/{case_id}/images/{filename}`
- `PATCH /api/patients/{case_id}/biometrics`
- `PATCH /api/patients/{case_id}/smi`

## Common Response Semantics

- `2xx`: request accepted/processed
- `4xx`: invalid input, unsupported file, not found, or contract violation
- `5xx`: internal segmentation failure or dependency failure

When integrating clients:

1. Treat `5xx` as retryable only when operation is idempotent.
2. Log `case_id`/`identificador` correlation fields when present.
3. Persist full error payload for incident triage.

1. API outputs are assistive and must not be used as autonomous diagnosis.
2. Validation, timeout, and retry behavior should be enforced by calling clients.
3. Upload handling is asynchronous; a successful `/upload` response means preparation started, not that final metrics are already available.
4. `/jobs` is also asynchronous; consumers should treat `GET /jobs/{job_id}` as
operational status and the callback as the terminal handoff contract.
