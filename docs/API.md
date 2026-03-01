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

`POST /upload` accepts a `.zip` payload and launches `core/prepare.py` asynchronously using the repository virtual environment Python binary.

### Patients and Results

- `GET /api/patients`
- `GET /api/patients/{case_id}/results`
- `GET /api/patients/{case_id}/metadata`
- `GET /api/patients/{case_id}/nifti`
- `GET /api/patients/{case_id}/download/{folder_name}`
- `GET /api/patients/{case_id}/images/{filename}`
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

Proxy notes:

1. `POST /api/anthropic/ap-thorax-xray` forwards to `ANTHROPIC_SERVICE_URL` (default `http://localhost:8101/analyze`).
2. `POST /api/medgemma/ap-thorax-xray` forwards to `MEDGEMMA_SERVICE_URL` (default `http://localhost:8004/analyze` unless overridden).
3. Proxy routes surface upstream availability and timeout failures as `503` or `504`.

### CTR Extraction (standalone service)

CTR (Cardiothoracic Ratio / ICT) extraction runs as an independent microservice on port `8003`.

Based on ChestXRayAnatomySegmentation (CXAS) by Constantin Seibold et al., CC BY-NC-SA 4.0.

- `POST /extract_ctr` — upload a chest X-ray image, returns CTR score and cardiomegaly flag
- `GET /health` — health check and model status

Example:

```bash
curl -X POST http://localhost:8003/extract_ctr \
  -F "file=@/path/to/chest_xray.png"
```

Response:

```json
{
  "ctr": "0.482310",
  "cardiomegaly_flag": "0"
}
```

> **Note**: `cardiomegaly_flag` is `"1"` when CTR > 0.50.

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
5. Upload processing is asynchronous; a successful `/upload` response means preparation started, not that final metrics are already available.
