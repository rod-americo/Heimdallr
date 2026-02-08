# API Contracts

This document summarizes high-value API contracts used in Heimdallr workflows.

## Base URL

- Local default: `http://localhost:8001`

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

## Contract Notes

1. API outputs are assistive and must not be used as autonomous diagnosis.
2. Validation, timeout, and retry behavior should be enforced by calling clients.
3. External model calls should follow institutional de-identification policy.

## Live Schema

For full and current request/response schemas, use:

- `GET /docs`
