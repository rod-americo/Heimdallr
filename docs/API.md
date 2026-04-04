# API Contracts

This document summarizes high-value API contracts used in Heimdallr workflows.

## Base URL

- Local default: `http://localhost:8001`
- OpenAPI schema/docs: `GET /docs`

## Versioning and Compatibility

- Current contract line: `v1` (path is unversioned today).
- Backward-compatible changes may be shipped in-place.
- Breaking changes should be documented in `CHANGELOG.md` before release.

## Authentication and Access Model

- Local development mode currently exposes endpoints without built-in auth middleware.
- Production deployment should enforce access controls at gateway/reverse proxy and network boundary.
- For sensitive environments, isolate dashboard/API access to trusted networks and audited identities.

## Core Endpoints

### Upload and Tooling

- `POST /upload`
- `GET /api/tools/uploader`

`POST /upload` accepts a `.zip` payload and launches the `heimdallr.prepare` worker flow asynchronously using the repository virtual environment Python binary.

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
