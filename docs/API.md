# API Contracts

This document summarizes high-value API contracts used in Heimdallr workflows.

## Base URL

- Local default: `http://localhost:8001`
- OpenAPI schema/docs: `GET /docs`

## Versioning and Compatibility

- Existing operational endpoints remain unversioned. The study-evidence contract is explicitly versioned under `/api/v1`.
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
- `GET /ops/queues`
- `POST /upload`
- `GET /api/tools/uploader`

`POST /upload` accepts a `.zip` payload and hands it off asynchronously to the `heimdallr.prepare` flow.
It also accepts optional multipart `qc_evidence=true|false`. Omission inherits the
host default; invalid boolean strings return `422`.

`POST /jobs` accepts `multipart/form-data` with:
- `study_file`
- `client_case_id`
- `callback_url`
- optional `source_system`
- optional `requested_outputs` JSON, which selects returned files such as
metrics JSON, overlays, report PDF, and report Encapsulated PDF DICOM
- optional `requested_metrics_modules` (JSON array or CSV string), which
selects enabled metrics jobs to run from the active profile
- optional `artifact_locale`, which selects the presentation locale for
generated presentation artifacts when supported, including localized burned-in
overlays and case-report DICOM metadata
- optional `series_selection_policy` JSON object, which overrides the active
series-selection profile for this submitted job only
- optional `artifact_dicom_policy` JSON object, which overrides generated DICOM
artifact encoding for this submitted job only
- optional `qc_evidence=true|false`; an explicit value overrides the host
  default for this submission

It returns an immediate acceptance payload with `job_id` and `status=queued`. `GET /jobs/{job_id}` returns the best available asynchronous status for that external job. When processing finishes, `heimdallr.integration.delivery` performs an outbound multipart callback. Successful jobs emit `case.completed` with `manifest.json` plus `package.zip`; terminal failed jobs emit `case.failed` with `manifest.json` and no package. The consumer-facing callback contract is documented in `heimdallr/integration/docs/JOB_SUBMISSION.md`.

QC resolution uses `explicit API value > host default`. DICOM listener handoffs
cannot carry an override. QC segmentation is independent from the primary
metrics pipeline and callback completion; job status and callback manifests
expose the associated analysis state when available.

### Versioned Study Evidence

- `GET /api/v1/studies/{study_uid}/analyses`
- `GET /api/v1/studies/{study_uid}/analysis`
- `GET /api/v1/studies/{study_uid}/series`
- `GET /api/v1/studies/{study_uid}/series/{series_uid}`
- `GET /api/v1/studies/{study_uid}/coverage`

The endpoints accept optional `analysis_id` for immutable historical lookup
(`analyses` returns the matching singleton list). Without it, Heimdallr returns the latest complete analysis, falling
back to the latest created analysis when none has completed. A study processed
without QC returns `404` with `detail.code=qc_analysis_not_available`.

Execution states distinguish `not_segmented`, `not_segmentable`,
`segmentation_pending`, and `segmentation_failed`. Anatomy states distinguish
`anatomy_not_detected`, `anatomy_present`, `anatomy_complete`,
`anatomy_truncated`, and `unknown`; missing or pending inference is never
reported as anatomy absence. QC acquisition inference uses a single
TotalSegmentator `total --ml` labelmap. Per-anatomy evidence includes label
identity, boundary-contact audit, and provenance; voxel counts, volumes, and
anatomy bounds are intentionally not exposed as QC measurements.

When `requested_outputs` is omitted or when keys are omitted inside it, those
outputs are treated as `false`. Consumers must request every file type they
expect in the final package.

If `requested_metrics_modules` is provided, Heimdallr constrains the case to
that subset of metrics jobs, automatically includes declared dependencies from
the active metrics profile, and still includes enabled jobs marked
`automatic=true`. When metrics jobs declare `requires_segmentation_tasks`,
segmentation is also constrained to the union of requested, dependency, and
automatic job task requirements. In automatic CT profiles, the final compatible
job set is additionally filtered by the segmentation inventory produced after
the `total` task.

The automatic CT pipeline passes the active segmentation profile's
`extra_args` to each TotalSegmentator task, including `total`. It writes
`artifacts/segmentation_inventory.json`, enables L3-dependent metrics only when
`total/vertebrae_L3.nii.gz` is complete, enables organ volumetry when at least
one parenchymal organ mask is present, and runs `cerebral_bleed` plus
`brain_structures` only when the `total/brain.nii.gz` mask is present,
geometry-compatible, non-empty, and does not touch scan bounds.
For L3 muscle area and VAT/SAT, the L3 tissue masks are cleaned by removing
components that touch dilated, near-slice projections of upper-appendicular
`total` masks when available, and the raw and cleaned pixel/area values are
preserved in the metric JSON.
`total/skull.nii.gz` is retained as optional crop and diagnostic context; skull
truncation does not block head QC or DICOM export when the brain mask and
required head tasks are complete. The `head_complete_qc` job is enabled in the
tracked default metrics profile, but it emits only a result JSON when the brain
gate fails or when required `cerebral_bleed` output is missing or incompatible.
`brain_structures` QC is per structure: masks that are missing, incompatible,
empty, or truncated are omitted from the volume table and overlay instead of
blocking the whole head artifact package, and the result JSON records them under
`measurement.omitted_brain_structures`. When the complete-head
gate passes, it writes a normalized axial head CT NIfTI
artifact, writes a canonical RAS 2 mm NIfTI artifact, writes a 1 mm
slice-spacing brain-mask geometry NIfTI artifact whose output plane is defined
by `total/brain.nii.gz` and whose in-plane midline is guided by
`brain_structures/septum_pellucidum.nii.gz` when available, emits a derived
axial CT DICOM series from that geometry volume using the configured
`derived_ct_transfer_syntax` while preserving source in-plane pixel spacing,
advancing 1 mm between images, and tagging 2 mm nominal slice thickness.
Slices are exported in spatial order so DICOM viewers detect a constant stack
interval; the brain-center slice is tagged in `ImageComments` without changing
stack order. The output field
of view is cropped from `total/skull.nii.gz` with a configurable margin so the
head opens at a practical display scale,
emits translated Secondary Capture DICOM artifacts for brain-structure volumes
and overlays, with the `brain_structures` overlay rendered on the
brain-geometry normalized CT grid without a text panel and with the overlay
color map included in the volume-table artifact. The only additional API-facing
head signal is the boolean
`measurement.cerebral_bleed.has_cerebral_bleed`, mirrored as
`measurement.cerebral_bleed.notification_bool` for downstream notification logic
when cerebral-bleed segmentation has run. The raw segmentation signal is exposed
as `measurement.cerebral_bleed.raw_has_cerebral_bleed`; notification is
suppressed when `measurement.cerebral_bleed.anatomic_support_qc` rejects
positive bleed voxels outside the `total/skull.nii.gz` plus
`total/brain.nii.gz` support mask.
The bleed overlay series is emitted only when the bleed mask has positive
voxels; it is rendered on the brain-geometry normalized CT grid as 5 mm slabs
with adjacent context slabs, no text panel, and a red transparent contour over
positive mask regions.

The automatic CT pipeline also supports pulmonary nodule screening through the
`lung_nodules` TotalSegmentator task. The task is selected after `total` only
when `artifacts/segmentation_inventory.json` reports at least one non-empty
lung lobe mask under `lungs.any_present`; complete bilateral lung coverage is
not required, so partial lung bases or apices can still be screened. External
submitters may also request the module explicitly with
`requested_metrics_modules=["lung_nodules"]`. The metrics result exposes
`measurement.has_pulmonary_nodule` and
`measurement.notification_bool`, both set from whether any `lung_nodules` mask
has positive voxels. Positive cases emit one Secondary Capture DICOM image per
connected nodule component in a single DICOM series; each component entry in
`measurement.components` records its `component_id`, NIfTI `slice_index`, the
approximate `probable_viewer_slice_index_one_based`, and the corresponding
overlay path. Negative cases write result JSON without a nodule overlay.

Pleural and pericardial effusion screening uses the TotalSegmentator
`pleural_pericard_effusion` task under the same `lungs.any_present` automatic
gate. External submitters may request it explicitly with
`requested_metrics_modules=["pleural_pericard_effusion"]`, but enabled automatic
profiles include it without requiring the consumer to name it. The module is
positive-only: `metrics.pleural_pericard_effusion` and its result JSON, PNG, and
DICOM artifacts are absent when both masks are empty. Positive results list only
the present findings, expose true `has_pleural_effusion` and/or
`has_pericardial_effusion` fields, and include volume and connected-component
details. Positive findings are exported as Secondary Capture images in one
DICOM series. Presentation images are 5 mm axial slab averages in a mediastinal
window and include every slab intersecting either positive mask; both findings
are overlaid together when present in the same slab.

If `series_selection_policy` is provided, Heimdallr deep-merges that object over
the active `config/series_selection.json` profile for the submitted job. The
supported rule groups include `required`, `hard_reject`, `phase_priority`,
`geometry_priority`, `follow_up_coverage`, `text_hints`, `window_hints`, and
`manufacturer_hints`. Text hints are accent-insensitive; window hints are
auxiliary preferences rather than hard-rejection criteria. The
selected series audit in `metadata/id.json` records `PolicySource` and
`ExternalPolicyName`.

If `artifact_dicom_policy` is provided, Heimdallr applies it to generated DICOM
artifacts for that submitted job only. Omitted fields fall back to the active
metrics profile. Supported `secondary_capture_transfer_syntax` values are
`original`, `deflated`, `jpeg_ls_lossless`, `jpeg_2000_lossless`, and
`rle_lossless`. Supported `secondary_capture_series_mode` values are
`separate` and `single_series`; the default is `separate`. In `single_series`
mode, only DICOM Secondary Capture artifacts are rewritten into one artifact
series for the case, including instruction documents when
`instruction_dicom_kind=secondary_capture`. Derived CT and Encapsulated PDF
DICOM artifacts remain in their own series. Secondary Captures retain
contiguous blocks by original artifact series and are ordered anatomically
within each block. Head CT jobs also support
`derived_ct_transfer_syntax` with the same values for generated derived CT
series. The repository default for generated Secondary Capture storage is
`jpeg_ls_lossless`; DICOM egress negotiates the peer's accepted presentation
context and transcodes only for transfer.

Example:

```bash
-F 'artifact_dicom_policy={"secondary_capture_transfer_syntax":"jpeg_ls_lossless","secondary_capture_series_mode":"single_series"}'
```

The options map to these DICOM transfer syntaxes:

| API value | DICOM transfer syntax | Compression |
| --- | --- | --- |
| `original` | Explicit VR Little Endian | none |
| `deflated` | Deflated Explicit VR Little Endian | lossless |
| `jpeg_ls_lossless` | JPEG-LS Lossless | lossless |
| `jpeg_2000_lossless` | JPEG 2000 Lossless Only | lossless |
| `rle_lossless` | RLE Lossless | lossless |

Use `jpeg_ls_lossless` as the preferred compressed option for OsiriX-facing
artifact storage. DICOM egress negotiates the target listener's accepted
presentation context and transcodes only for transfer when needed. `deflated`
remains supported, but observed OsiriX behavior can be less reliable with
Deflated Explicit VR Little Endian.

`GET /ops/queues` returns non-identifying operational capacity for external
feeders: queue status counts, oldest pending timestamps, segmentation
activity, independent `prepare_max_parallel_cases`,
`segmentation_max_parallel_cases`, and `metrics_max_parallel_cases` capacity,
and runtime disk usage. The legacy `max_parallel_cases` field remains an alias
for segmentation capacity. It does not return case IDs, study UIDs,
patient identifiers, or package paths.

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
