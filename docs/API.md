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

### Patients and Results

- `GET /api/patients`
- `GET /api/patients/{case_id}/results`
- `GET /api/patients/{case_id}/metadata`
- `PATCH /api/patients/{case_id}/biometrics`
- `PATCH /api/patients/{case_id}/smi`

### Report Assistance

Anthropic chest X-ray endpoint:

- `POST /api/anthropic/ap-thorax-xray`

Example:

```bash
curl -X POST http://localhost:8001/api/anthropic/ap-thorax-xray \
  -F "file=@/path/to/image.dcm" \
  -F "age=45 year old" \
  -F "identificador=case_123"
```

MedGemma chest X-ray endpoint:

- `POST /api/medgemma/ap-thorax-xray`

Example:

```bash
curl -X POST http://localhost:8001/api/medgemma/ap-thorax-xray \
  -F "file=@/path/to/image.png" \
  -F "age=45 year old"
```

Both endpoints return a `deid` object with gateway details (for example: `metadata_removed`, `pixel_redaction`, `age_coarsened`, `review_required`, `bounding_boxes`).

## Common Response Semantics

- `2xx`: request accepted/processed
- `4xx`: invalid input, unsupported file, not found, or contract violation
- `5xx`: internal processing failure or dependency failure

When integrating clients:

1. Treat `5xx` as retryable only when operation is idempotent.
2. Log `case_id`/`identificador` correlation fields when present.
3. Persist full error payload for incident triage.

## Timeout and Retry Guidance

- Upload and model-assist routes may involve long-running operations.
- Clients should set explicit request timeouts and bounded retries.
- Use exponential backoff for transient failures (`429`, `503`, transport errors).

## Contract Notes

1. API outputs are assistive and must not be used as autonomous diagnosis.
2. Validation, timeout, and retry behavior should be enforced by calling clients.
3. External model calls are routed through the de-identification gateway before outbound requests.
4. OCR-based review requires `pytesseract` + system `tesseract` installed on the service host.
