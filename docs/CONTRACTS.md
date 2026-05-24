# Contracts

This document records the canonical inputs, outputs, identifiers, queue contracts, integrations, and invariants that matter for safe Heimdallr changes.

## 1. Purpose

Heimdallr transforms incoming radiological imaging studies into traceable runtime artifacts, deterministic measurements, database state, and outbound delivery events. Contracts here describe what the current code supports; they do not claim clinical readiness.

## 2. Canonical Inputs

| Name | Origin | Format | Required | Notes |
| --- | --- | --- | --- | --- |
| DICOM C-STORE study | PACS/modality to `heimdallr.intake` | DICOM instances grouped by `StudyInstanceUID` | yes for listener flow | Default AE title is `HEIMDALLR`; default port is `11114`. |
| Upload ZIP | `POST /upload` or intake local handoff | `.zip` containing DICOM files | yes for prepare flow | Stored in `runtime/intake/uploads/external/` or `from_prepare/`. |
| External job ZIP | `POST /jobs` | multipart `study_file` ZIP plus form fields | no | Requires `client_case_id` and `callback_url`; optional requested outputs, metrics modules, and per-job series-selection policy. |
| Intake pipeline config | repo or host | JSON | yes | `config/intake_pipeline.json` is versioned. |
| Series selection config | repo or host | JSON | yes | `config/series_selection.json` defines selection rules. |
| Segmentation pipeline config | host-local | JSON | yes for segmentation | Created from `config/segmentation_pipeline.example.json`. |
| Metrics pipeline config | host-local | JSON | yes for metrics | Created from `config/metrics_pipeline.example.json`; concrete file stays ignored. |
| DICOM egress config | host-local | JSON | yes for DICOM egress | Created from `config/dicom_egress.example.json`. |
| Integration dispatch config | host-local | JSON | no | Enables outbound patient/event webhooks. |
| Integration delivery config | host-local | JSON | no | Controls final package callback retries and timeouts. |
| Presentation config | host-local | JSON | no | Controls patient-name and artifact locale behavior. |
| Host stack manifest | host-local | JSON | no | `config/host_stack/*.json` records host accelerator type, allowed TotalSegmentator device, and worker limits; concrete manifests stay ignored. |

## 3. Canonical Outputs

| Name | Destination | Format | Guarantees |
| --- | --- | --- | --- |
| Study directory | `runtime/studies/<case_id>/` | filesystem tree | Current canonical case workspace. |
| Study identity metadata | `metadata/id.json` | JSON | Contains study identifiers, available-series metadata, selected-series audit, and pipeline state when available. |
| Results payload | `metadata/resultados.json` | JSON | Stores deterministic metrics and generated-artifact references after metrics execution. |
| Source DICOM series | `source/dicom/series/<series-stem>/` | DICOM files grouped by series | Preserves the scanned study instances after ZIP extraction for reprocessing and audit until the case workspace is purged. |
| Canonical NIfTI | `derived/<case_id>.nii.gz` | NIfTI | Produced or materialized by segmentation for the selected series. |
| Segmentation artifacts | `artifacts/<task>/` | files | TotalSegmentator task outputs according to active profile. |
| Head normalization artifact | `artifacts/metrics/head_complete_qc/normalized_axial_head_ct.nii.gz` | NIfTI | Produced by `head_complete_qc` after the automatic brain-mask gate passes. The required mask is `total/brain.nii.gz`; `total/skull.nii.gz` is optional diagnostic/crop context and may be truncated. |
| Head RAS 2 mm artifact | `artifacts/metrics/head_complete_qc/normalized_ras_head_ct_2mm.nii.gz` | NIfTI | Canonical RAS isotropic 2 mm volume. Anatomical orbitomeatal and midline alignment is reported as `landmarks_required` until validated landmarks are available. |
| Head brain-geometry 2 mm artifact | `artifacts/metrics/head_complete_qc/normalized_brain_geometry_head_ct_2mm.nii.gz` | NIfTI | Volume resampled so the `total/brain.nii.gz` mask PCA axes define the output plane. It uses `brain_structures/septum_pellucidum.nii.gz` as an in-plane midline guide when available, preserves source in-plane spacing by default, and uses 1 mm slice spacing. Does not require the orbitomeatal line. |
| Head brain-geometry CT DICOM series | `artifacts/metrics/head_complete_qc/brain_geometry_ct_2mm_dicom/` | DICOM CT series | Derived axial CT series from the brain-geometry volume, encoded with the job's `derived_ct_transfer_syntax` while preserving source in-plane pixel spacing, using 1 mm spacing between images, and tagging 2 mm nominal slice thickness. `SeriesDate` and `SeriesTime` are preserved from the original selected source series when available, while `ContentDate`, `ContentTime`, and instance creation time describe artifact generation. Slices are exported in spatial order so DICOM viewers detect a constant stack interval; the brain-center slice is tagged in `ImageComments` without changing stack order. The output field of view uses `total/skull.nii.gz` with a configurable margin when available, otherwise it falls back to `total/brain.nii.gz`. Skull truncation is reported but does not block export. |
| Head volume table DICOM | `artifacts/metrics/head_complete_qc/volume_table_dicom/volume_table_0001.dcm` | DICOM Secondary Capture | Translated table containing total brain volume from `total/brain.nii.gz`, individual `brain_structures` volumes, and the color map used by the brain-structure overlay. |
| Head brain-structure overlay DICOM series | `artifacts/metrics/head_complete_qc/brain_structures_dicom/` | DICOM Secondary Capture series | Burned-in 3 mm slab overlays for available `brain_structures` masks. The CT base image is the brain-geometry normalized volume, and masks are nearest-neighbor resampled onto that same grid before rendering. No text panel is burned into the overlay images. |
| Head bleed overlay DICOM series | `artifacts/metrics/head_complete_qc/cerebral_bleed_dicom/` | DICOM Secondary Capture series | Conditional burned-in 5 mm slab overlays for positive bleed-mask slabs plus adjacent slabs. The CT base image is the brain-geometry normalized volume, and the bleed mask is nearest-neighbor resampled onto that same grid before rendering. No text panel is burned into the overlay images; positive mask regions are marked with a red transparent contour. Emitted only when bleed is present. |
| Metrics artifacts | `artifacts/` and `metadata/` | PNG/PDF/DICOM/JSON | Generated by enabled metrics jobs and artifact builders. |
| SQLite state | `database/dicom.db` | SQLite | Stores study metadata, queues, delivery state, and resource monitor samples. |
| DICOM egress items | remote SCP | DICOM C-STORE | Queue worker attempts configured artifact delivery. |
| Integration dispatch events | external HTTP endpoint | JSON HTTP POST | Delivered when configured destinations accept the event. |
| Job status lookup | `GET /jobs/{job_id}` | JSON | Best-effort state for accepted external jobs. |
| Queue capacity lookup | `GET /ops/queues` | JSON | Non-identifying queue counts, concurrency, and disk capacity for external feeders. |
| Terminal delivery callback | external submitter | multipart HTTP POST | Sends `case.completed` with `manifest.json` and `package.zip`, or `case.failed` with manifest only. |

## 4. Identifiers and Keys

| Concept | Canonical field | Notes |
| --- | --- | --- |
| DICOM study | `StudyInstanceUID` | Primary DICOM grouping key; also primary key in `dicom_metadata`. |
| Heimdallr case | `case_id` / `ClinicalName` | Filesystem-safe operational identifier generated during prepare. New cases use `AccessionNumber_NameInitials`; historical cases may use older naming. Do not assume it equals study UID. |
| External submitted job | `job_id` | Generated for `/jobs` submissions and used by integration delivery. |
| External caller case | `client_case_id` | Caller-owned identifier echoed back in final delivery. |
| Series selected for segmentation | `SeriesInstanceUID` plus slice count and geometry summary | Used for selection audit and reuse decisions. Geometry fields may include measured `CoverageMm`, `ZSpacingMm`, `SliceThicknessMm`, and selection thresholds when available. |
| Queue item | queue table `id` | Internal claim/retry identity, not an external contract. |
| Artifact digest | `artifact_digest` | Used to preserve or reset DICOM egress queue state when artifacts change. |
| Handoff duplicate state | `study_uid` + `manifest_digest` | Used to suppress repeated intake handoffs while prepare is pending or complete. |

## 5. Pipeline Events and Stages

| Stage | Input | Output | Expected failures |
| --- | --- | --- | --- |
| `intake` | inbound DICOM instances | ZIP payload and intake manifest | invalid DICOM, association failure, idle flush timing, upload/local handoff failure |
| `prepare` | claimable ZIP payload | study directory, metadata, queue rows | bad ZIP, no valid DICOM series, conversion failure, insufficient series images |
| `segmentation` | prepared case queue item | segmentation artifacts and metrics queue item | TotalSegmentator failure, missing license, resource exhaustion, stale claim |
| `metrics` | metrics queue item | results JSON, overlays, PDFs, DICOM artifacts, delivery queues | missing masks, incomplete masks, unsupported profile, job dependency errors, artifact generation failure |
| `integration.dispatch` | dispatch queue item | HTTP event delivery state | unreachable endpoint, non-2xx response, config error |
| `integration.delivery` | delivery queue item | final callback delivery state | missing case outputs, callback failure, package build failure |
| `dicom_egress` | DICOM egress queue item | remote C-STORE delivery state | peer rejects association, transfer syntax mismatch, compression fallback unavailable |
| `space_manager` | completed study artifacts | reclaimed disk and purge flags | permission failure, active case protection, insufficient purge candidates |
| `resource_monitor` | process/case state | SQLite telemetry samples | process disappeared, unsupported procfs/cgroup details |

## 6. Integration Contracts

### HTTP Uploads

`POST /upload` accepts only `.zip` uploads and returns acceptance after the file is stored in the external spool. Acceptance does not mean processing completed.

`POST /jobs` requires:

- `study_file`
- `client_case_id`
- `callback_url`

Optional fields:

- `source_system`
- `requested_outputs` as JSON object for returned files
- `requested_metrics_modules` as JSON array or CSV string for requested metrics
jobs from the active profile. Declared metrics dependencies and enabled
`automatic=true` jobs are included, and declared `requires_segmentation_tasks`
values can narrow the segmentation task set before metrics run.
- `artifact_locale` as an optional presentation locale for generated
presentation artifacts when supported, including localized burned-in overlays
and case-report DICOM metadata.
- `series_selection_policy` as JSON object for per-job overrides of the active
series-selection profile

If `requested_outputs` is omitted or if keys are omitted inside it, those
outputs are `false`. Heimdallr does not add package outputs by default for the
external `/jobs` contract.

If `series_selection_policy` is present, Heimdallr deep-merges it over the
configured series-selection profile for that job only. Top-level metadata keys
such as `name`, `profile_name`, `base_profile`, and `schema_version` are treated
as audit labels, not selection rules. The selection audit in `metadata/id.json`
records whether the active policy came from config or external delivery.

The external consumer contract is maintained in `heimdallr/integration/docs/JOB_SUBMISSION.md`.

### Operational Capacity

`GET /ops/queues` returns non-identifying operational capacity for feeders that
need backpressure before submitting more studies. The response includes queue
status counts, oldest pending timestamps, `MAX_PARALLEL_CASES`, segmentation
active counts, and runtime disk usage. It must not include `case_id`, study
UIDs, patient identifiers, file paths inside a case, callback URLs, or package
contents.

### Final Delivery

For `/jobs` submissions, successful final callbacks are multipart and include:

- `manifest.json`
- `package.zip`

Terminal failed callbacks use `event_type=case.failed` and include a multipart `manifest.json` without `package.zip`. `GET /jobs/{job_id}` exposes best-effort status for accepted jobs while processing and delivery are still in progress.

Outbound event dispatch consumers are documented in `heimdallr/integration/docs/EVENT_DISPATCH.md`.

### DICOM

Inbound DICOM defaults:

- AE title: `HEIMDALLR`
- port: `11114`
- supported storage contexts from `pynetdicom`

Outbound DICOM destinations are host-local and defined in `config/dicom_egress.json`.
The same file controls DICOM egress queue concurrency with `worker_count`; the
default is 10 workers.

## 7. Invariants

- Runtime state is mutable and must not be committed.
- Host-local operational JSON files must remain ignored.
- Host stack manifests are operational guardrails, not deployment guarantees.
They must not contain secrets, PHI, PACS credentials, callback tokens, or case
paths.
- `config/metrics_pipeline.example.json` must be updated when adding a new
production metrics module.
- The automatic CT segmentation workflow passes each task's configured
`extra_args` through to TotalSegmentator, including `total`. It runs
`tissue_types` only when `total/vertebrae_L3.nii.gz` is present,
geometry-compatible, non-empty, and complete along the scan axis. The
complete-head workflow treats `total/brain.nii.gz` as the required gate mask and
requires it to be present, non-empty, geometry matched, and not touching scan
bounds. `total/skull.nii.gz` is optional crop and diagnostic context; skull
truncation is reported but does not block `cerebral_bleed` or
`brain_structures` TotalSegmentator tasks. The
machine-readable bleed notification field is
`measurement.cerebral_bleed.has_cerebral_bleed` and mirrors
`measurement.cerebral_bleed.notification_bool` when bleed segmentation has run.
- Complete-head geometric normalization includes
`normalized_brain_geometry_head_ct_2mm.nii.gz`, which uses the `total/brain`
mask to define a reproducible output plane. This is the preferred head geometry
artifact when an exact orbitomeatal line is not required.
- Experimental metrics modules must not silently enter the default
production-facing profile.
- `StudyInstanceUID`, `case_id`, and `job_id` are not interchangeable.
- Series selection should prefer maximum measured anatomical coverage first and
the thinnest available reconstruction only among coverage-equivalent eligible
series. If geometric metadata is absent, the selector falls back to the legacy
slice-count ranking.
- Prepared studies preserve source DICOM instances grouped by series under the
case workspace. `AvailableSeries` and `DiscardedSeries` may include
`SourceDicomSeriesPath` and `SourceDicomInstanceCount` so later operators can
audit or reprocess from the same series set. The upload ZIP remains a transport
artifact and is deleted after successful prepare.
- Volumetry artifacts should not be published when every candidate organ is
missing, empty, or incomplete. The metrics result JSON should preserve the
reason and per-organ status for audit.
- Generated Secondary Capture overlays should use a bounded matrix size. The
shared helper defaults the largest image dimension to 512 pixels; the validated
`l3_muscle_area` and `vat_sat_ratio` jobs set 1024 pixels in the tracked metrics
profile. The complete-head volume table keeps its native table canvas so text
remains legible after DICOM import.
- Secondary Capture DICOM transfer syntax is configurable per metrics job in
`config/metrics_pipeline.json` and per external `/jobs` submission through
`artifact_dicom_policy.secondary_capture_transfer_syntax`. The supported
lossless options are original uncompressed, Deflated Explicit VR Little Endian,
JPEG-LS lossless, JPEG 2000 lossless, and RLE lossless. The repository default
is JPEG-LS lossless.
- Head derived CT DICOM uses `derived_ct_transfer_syntax` with the same option
vocabulary. DICOM egress negotiates the peer's accepted presentation context and
transcodes only for transfer when the peer does not accept the artifact's stored
transfer syntax.
- Compression validation on 512 x 512 RGB parenchymal volumetry overlays found
approximate per-slice sizes of ~787 KB uncompressed, ~82-113 KB Deflated,
~114-164 KB JPEG-LS, ~173-219 KB JPEG 2000, and ~187-264 KB RLE. These are
operational measurements, not contractual file-size guarantees.
- Prefer JPEG-LS lossless for OsiriX-facing artifact storage. Deflated remains
available as a supported contract value, but it has shown less reliable OsiriX
handling in operational validation.
- Derived Secondary Capture artifacts should preserve source patient and study
identity tags from the reference DICOM while assigning new derived series and
instance UIDs.
- FastAPI upload acceptance is asynchronous and not proof that segmentation,
metrics, or delivery succeeded.
- Clinical review is required before outputs influence patient care.

## 8. Assumptions and Partial Areas

- Built-in FastAPI authentication is not implemented; production access control
is assumed to be enforced outside the app.
- Worker logs are not uniformly structured JSON; some workers use line-buffered
`print()` and per-case log files.
- End-to-end smoke depends on external DICOM peer behavior, TotalSegmentator
readiness, and non-PHI sample data that is not tracked in this repository.
- LLM-adjacent runtime clients are intentionally not part of Heimdallr's
dependency set; intelligence-layer behavior belongs outside this repository.

## 9. Breaking Contract Log

- No breaking contract is introduced by the structural recovery documentation
and governance scripts.
