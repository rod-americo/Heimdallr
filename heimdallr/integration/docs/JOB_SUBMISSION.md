# External Job Submission

This contract is for applications that send a DICOM ZIP to Heimdallr and expect
an asynchronous callback when the case is complete.

## Endpoint

```text
POST /jobs
Content-Type: multipart/form-data
```

Required fields:

| Field | Type | Notes |
| --- | --- | --- |
| `study_file` | file | Must have a `.zip` filename. Archive content is processed asynchronously after acceptance. |
| `client_case_id` | string | Caller-owned case identifier. Echoed back in the final callback. |
| `callback_url` | string | Absolute HTTP endpoint that receives the final multipart callback. |

Optional fields:

| Field | Type | Notes |
| --- | --- | --- |
| `source_system` | string | Caller system identifier. Echoed back in callback metadata. |
| `requested_outputs` | JSON object string | Controls optional files in the final package. |
| `requested_metrics_modules` | JSON array string or CSV string | Limits metrics jobs to requested modules plus dependencies from the active metrics profile. Not fully validated at admission time. |

Example request:

```bash
curl -X POST "http://localhost:8001/jobs" \
  -F "study_file=@study.zip;type=application/zip" \
  -F "client_case_id=external-123" \
  -F "source_system=partner_a" \
  -F "callback_url=http://receiver.local/heimdallr/callback" \
  -F 'requested_outputs={"include_report_pdf":true,"include_artifacts_tree":true}' \
  -F 'requested_metrics_modules=["l3_muscle_area","bone_health_l1_hu"]'
```

## Acceptance Response

Heimdallr returns after the file and sidecar metadata are stored in the external
upload spool. This response does not mean the study was prepared, segmented,
measured, or delivered.

Example response:

```json
{
  "accepted": true,
  "job_id": "9d3fdaf7-82df-4ee8-a0c0-fb927bc8c3d1",
  "client_case_id": "external-123",
  "status": "queued",
  "received_at": "2026-05-01T14:30:00-03:00",
  "stored_file": "study_20260501143000.zip",
  "requested_metrics_modules": [
    "l3_muscle_area",
    "bone_health_l1_hu"
  ]
}
```

External applications should persist `job_id` and `client_case_id`. Heimdallr
also creates an internal `case_id` later in the prepare stage; callers must not
assume it is known at submission time.

## Requested Outputs

Supported `requested_outputs` keys:

| Key | Default | Current behavior |
| --- | --- | --- |
| `include_id_json` | `true` | `metadata/id.json` is currently always included as minimum package metadata. |
| `include_metadata_json` | `true` | Includes `metadata/metadata.json` when present. |
| `include_resultados_json` | `true` | Includes `metadata/resultados.json` when present. |
| `include_report_pdf` | `true` | Builds and includes `metadata/report.pdf` when possible. |
| `include_artifacts_tree` | `true` | Includes files under `artifacts/metrics/` when present. |

Boolean-like strings such as `"true"`, `"yes"`, `"on"`, and `"1"` are treated
as true when the sidecar is normalized.

## Final Callback

When metrics processing enqueues final delivery and the delivery worker is
enabled, Heimdallr sends:

```text
POST <callback_url>
Content-Type: multipart/form-data
```

Multipart parts:

| Part | Filename | Content type | Notes |
| --- | --- | --- | --- |
| `manifest` | `manifest.json` | `application/json` | Callback manifest with package metadata. |
| `package` | `heimdallr_<safe-client-case-id>.zip` | `application/zip` | Final package artifact. |

Any HTTP `2xx` response marks the delivery queue item as done. Non-`2xx`
responses and transport errors are retried according to
`config/integration_delivery.json`.

Example callback manifest:

```json
{
  "event_type": "case.completed",
  "event_version": 1,
  "event_id": "case.completed:9d3fdaf7-82df-4ee8-a0c0-fb927bc8c3d1",
  "job_id": "9d3fdaf7-82df-4ee8-a0c0-fb927bc8c3d1",
  "case_id": "Case123_20260501_001",
  "study_instance_uid": "1.2.840.113619.2.55.3.604688432.123.1714560000.1",
  "client_case_id": "external-123",
  "source_system": "partner_a",
  "status": "done",
  "received_at": "2026-05-01T14:30:00-03:00",
  "completed_at": "2026-05-01T14:42:10-03:00",
  "package_name": "heimdallr_external-123.zip",
  "package_sha256": "7f4fd1b7e54a0d6b3e5f3f1f9c5e6a1c2f6bb1f9a4e0f0f2b9c2c4d6a9e0f111",
  "package_size_bytes": 1842752,
  "contents": {
    "metadata_id_json": true,
    "metadata_json": true,
    "resultados_json": true,
    "report_pdf": true,
    "metrics_artifact_files": 12
  }
}
```

Package ZIP layout:

```text
manifest.json
metadata/id.json
metadata/metadata.json
metadata/resultados.json
metadata/report.pdf
artifacts/metrics/<metric_key>/...
```

Some files may be absent when they were not generated, not requested, or not
available for the completed case. The package-level `manifest.json` inside the
ZIP has the same high-level identity and content summary, but is not a detached
signature.

## Receiver Requirements

Callback receivers should:

- accept multipart HTTP `POST`
- treat `job_id` plus `event_type` as the idempotency key
- return any `2xx` status only after persisting the package or a durable
  handoff record
- ignore unknown JSON fields
- verify `package_sha256` after reading the ZIP
- store the original manifest for audit

## Current Limitations

- `/jobs` has no built-in authentication middleware.
- Callback delivery has no built-in HMAC, mTLS, or signature.
- Callback delivery does not currently support custom headers.
- There is no dedicated external polling endpoint for job status.
- Archive contents are validated later by the prepare worker, not at admission
  time.
- Failed terminal deliveries remain in SQLite queue state; a full dead-letter
  queue export process is not implemented yet.
