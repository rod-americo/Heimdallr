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
| Head normalization artifact | `artifacts/metrics/head_complete_qc/normalized_axial_head_ct.nii.gz` | NIfTI | Produced by `head_complete_qc` only after the automatic complete-head gate passes. The required mask is `total/brain.nii.gz`; `total/skull.nii.gz` is optional diagnostic/crop context and may be truncated. Required `cerebral_bleed` and `brain_structures` outputs must also be task-complete and geometry-compatible before derived artifacts are emitted. |
| Head RAS 2 mm artifact | `artifacts/metrics/head_complete_qc/normalized_ras_head_ct_2mm.nii.gz` | NIfTI | Canonical RAS isotropic 2 mm volume. Anatomical orbitomeatal and midline alignment is reported as `landmarks_required` until validated landmarks are available. |
| Head brain-geometry 2 mm artifact | `artifacts/metrics/head_complete_qc/normalized_brain_geometry_head_ct_2mm.nii.gz` | NIfTI | Volume resampled so the `total/brain.nii.gz` mask PCA axes define the output plane. It uses `brain_structures/septum_pellucidum.nii.gz` as an in-plane midline guide when available, preserves source in-plane spacing by default, and uses 1 mm slice spacing. Does not require the orbitomeatal line. |
| Head brain-geometry CT DICOM series | `artifacts/metrics/head_complete_qc/brain_geometry_ct_2mm_dicom/` | DICOM CT series | Derived axial CT series from the brain-geometry volume, encoded with the job's `derived_ct_transfer_syntax` while preserving source in-plane pixel spacing, using 1 mm spacing between images, and tagging 2 mm nominal slice thickness. `SeriesDate` and `SeriesTime` are preserved from the original selected source series when available, while `ContentDate`, `ContentTime`, and instance creation time describe artifact generation. Slices are exported in spatial order so DICOM viewers detect a constant stack interval; the brain-center slice is tagged in `ImageComments` without changing stack order. The output field of view uses `total/skull.nii.gz` with a configurable margin when available, otherwise it falls back to `total/brain.nii.gz`. Skull truncation is reported but does not block export. |
| Head volume table DICOM | `artifacts/metrics/head_complete_qc/volume_table_dicom/volume_table_0001.dcm` | DICOM Secondary Capture | Translated table containing total brain volume from `total/brain.nii.gz`, individual `brain_structures` volumes, and the color map used by the brain-structure overlay. |
| Head brain-structure overlay DICOM series | `artifacts/metrics/head_complete_qc/brain_structures_dicom/` | DICOM Secondary Capture series | Burned-in 3 mm slab overlays for available `brain_structures` masks. The CT base image is the brain-geometry normalized volume, and masks are nearest-neighbor resampled onto that same grid before rendering. No text panel is burned into the overlay images. |
| Head bleed overlay DICOM series | `artifacts/metrics/head_complete_qc/cerebral_bleed_dicom/` | DICOM Secondary Capture series | Conditional burned-in 5 mm slab overlays for positive bleed-mask slabs plus adjacent slabs. The CT base image is the brain-geometry normalized volume, and the bleed mask is nearest-neighbor resampled onto that same grid before rendering. No text panel is burned into the overlay images; positive mask regions are marked with a red transparent contour. Emitted only when bleed is present. |
| Pulmonary nodule result | `artifacts/metrics/lung_nodules/result.json` | JSON | Produced by the `lung_nodules` metrics job when the TotalSegmentator `lung_nodules` task is available. The API-facing signal is `measurement.has_pulmonary_nodule`. A connected component is eligible only when at least one voxel intersects the union of the final `total` lung-lobe masks. `measurement.anatomical_qc` preserves raw, eligible, and excluded component/voxel counts plus the per-component overlap decision. |
| Pulmonary nodule overlay DICOM series | `artifacts/metrics/lung_nodules/nodule_component_*.dcm` | DICOM Secondary Capture series | Positive-case axial lung-window overlays (`WL -600 HU`, `WW 1500 HU`), one DICOM image per anatomically eligible nodule component. Each component is linked by `component_id` in `measurement.components`, `artifacts.component_overlays`, and `dicom_exports`; the legacy `artifacts.overlay_sc_dcm` field points to the first component image for compatibility. DICOM image-plane position is copied from the nearest preserved source CT instance for position-based synchronization. No overlay is emitted when the raw nodule mask is empty or every component is outside the final lung masks. |
| Hepatic lesion result | `artifacts/metrics/liver_lesions/result.json` | JSON | Result from the TotalSegmentator `liver_lesions` task. The API-facing signal is `measurement.has_hepatic_lesion`; connected components include voxel counts, volumes, centroids, bounding boxes, and representative axial positions. A component is eligible only when at least one voxel intersects the final `total/liver.nii.gz` mask and its maximum boundary-to-boundary axial Feret diameter is at least 4 mm. Partial liver overlap retains the complete predicted component because the final liver segmentation can undersegment subcapsular tissue. `measurement.anatomical_qc` preserves raw, eligible, and excluded component/voxel counts, the axial diameter, and the per-component decision. The automatic CT planner requests the model only when `total/liver.nii.gz` is positive and the host profile enables both the task and job. |
| Hepatic lesion overlay DICOM series | `artifacts/metrics/liver_lesions/lesion_component_*.dcm` | DICOM Secondary Capture series | Positive-case axial soft-tissue overlays (`WL 60 HU`, `WW 400 HU`), one image per anatomically and size-eligible connected lesion component. The liver outline is cyan and the lesion is red; the QC diameter is not rendered. DICOM image-plane geometry is copied from the nearest preserved source CT instance for position-based synchronization. No overlay is emitted when no component passes both liver-overlap and minimum-size QC. |
| Pleural/pericardial effusion result | `artifacts/metrics/pleural_pericard_effusion/result.json` | JSON | Positive-only result from the TotalSegmentator `pleural_pericard_effusion` task. Pericardial effusion qualifies at a total volume of at least 50 mL; pleural effusion qualifies independently for each lateralized side at 50 mL or more. Subthreshold and indeterminate pleural volumes remain in `measurement.display_qc` for audit but do not make the public result positive. |
| Pleural/pericardial effusion overlay DICOM series | `artifacts/metrics/pleural_pericard_effusion/overlay_*.dcm` | DICOM Secondary Capture series | Positive axial 5 mm slab-average overlays in a mediastinal window (`WL 40 HU`, `WW 400 HU`). Only qualifying pericardial masks and qualifying pleural sides are displayed. DICOM image-plane position is copied from the nearest preserved source CT instance. No presentation artifact is emitted when every detected volume is below its QC threshold. |
| Metrics artifacts | `artifacts/` and `metadata/` | PNG/PDF/DICOM/JSON | Generated by enabled metrics jobs and artifact builders. |
| SQLite state | `database/dicom.db` | SQLite | Stores study metadata, queues, delivery state, and resource monitor samples. |
| DICOM egress items | remote SCP | DICOM C-STORE | Queue worker attempts configured artifact delivery. |

Every DICOM Secondary Capture anatomical overlay carries the exact image-plane
geometry of the nearest preserved source axial DICOM instance. This includes
single-panel axial overlays, axial slab series, and multipanel presentations;
the axial panel or analyzed axial level defines the position. The derived pixel
spacing uses one proportional scale factor when the rendered matrix size
differs, preserving pixel aspect ratio while keeping the source field of view
inside a physically centered canvas. Textual tables, instructions, and PDF artifacts are
non-spatial and do not receive an invented anatomic position.

Pulmonary nodule component overlays are exported in monotonic axial position
order rather than component-size order. When `secondary_capture_series_mode`
is `single_series`, Secondary Captures are first grouped into contiguous blocks
by their original artifact series, preserving the artifact export order. Within
each block, spatial images are renumbered by projected axial position and
non-spatial images follow in their original export order. This prevents distinct
overlay products at overlapping anatomy from being interleaved.
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
values can narrow the segmentation task set before metrics run. Automatic CT
profiles then filter the compatible job set with the `total` segmentation
inventory.
The `lung_nodules` module may be requested here; in automatic CT profiles it is
also selected automatically when `lungs.any_present` is true in the segmentation
inventory.
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
status counts, oldest pending timestamps, independent prepare, segmentation,
and metrics case capacities, segmentation active counts, and runtime disk
usage. The legacy `max_parallel_cases` field remains a segmentation-capacity
alias. It must not include `case_id`, study
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

### Worker Concurrency

Resident services own concurrency independently:

- `prepare_watchdog.max_parallel_cases` controls studies prepared concurrently;
  `series_conversion_workers` and `phase_detection.max_parallel` control work
  inside those studies.
- `segmentation_pipeline.execution.max_parallel_cases` controls studies being
  segmented concurrently.
- `metrics_pipeline.execution.max_parallel_cases` controls studies executing
  metrics concurrently, while `profiles.*.execution.max_parallel_jobs` controls
  independent jobs inside each study.

All case-concurrency defaults are `1`. API submission concurrency does not
override resident worker capacity.

## 7. Invariants

- Runtime state is mutable and must not be committed.
- Host-local operational JSON files must remain ignored.
- Host stack manifests are operational guardrails, not deployment guarantees.
They must not contain secrets, PHI, PACS credentials, callback tokens, or case
paths.
- `config/metrics_pipeline.example.json` must be updated when adding a new
production metrics module.
- The automatic CT segmentation workflow passes each task's configured
`extra_args` through to TotalSegmentator, including `total`. It writes
`artifacts/segmentation_inventory.json` after `total` and uses configured
`requires_inventory` requirements to select compatible metrics and downstream
segmentation tasks. It runs `tissue_types` only when `total/vertebrae_L3.nii.gz`
is present, geometry-compatible, non-empty, and complete along the scan axis.
Organ volumetry is compatible when at least one configured parenchymal organ
mask is present. Pulmonary nodule screening is compatible when at least one
configured lung lobe mask is present, even if the lung coverage is partial. The
complete-head workflow treats `total/brain.nii.gz` as the
required gate mask and requires it to be present, non-empty, geometry matched,
and not touching scan bounds. `total/skull.nii.gz` is optional crop and
diagnostic context; skull truncation is reported but does not block
`cerebral_bleed` or
`brain_structures` TotalSegmentator tasks. The
machine-readable bleed notification field is
`measurement.cerebral_bleed.has_cerebral_bleed` and mirrors
`measurement.cerebral_bleed.notification_bool` when bleed segmentation has run
and `measurement.cerebral_bleed.anatomic_support_qc` has not rejected the raw
bleed mask for extending outside the `total/skull.nii.gz` plus
`total/brain.nii.gz` support mask. The raw TotalSegmentator signal remains
available as `measurement.cerebral_bleed.raw_has_cerebral_bleed`. If required
head segmentation outputs are incomplete, `head_complete_qc` writes only
`result.json` with `measurement.job_status=incomplete_head_segmentation` and no
derived NIfTI or DICOM artifact references. `brain_structures` QC is
per-structure: missing, empty, geometry-incompatible, or truncated structure
masks are excluded from volume rows and overlays, recorded in
`measurement.omitted_brain_structures`, and do not block derived head CT or
overlays for the remaining complete structures.
- The pulmonary nodule workflow uses the TotalSegmentator `lung_nodules` task
and exposes `measurement.has_pulmonary_nodule` plus
`measurement.notification_bool`. Both fields are true when any nodule mask has
positive voxels. Positive cases split the mask into connected components and
emit a single Secondary Capture DICOM series with one image per component.
Component entries record the NIfTI center slice, approximate viewer slice, and
the DICOM path that corresponds to that `component_id`. Negative cases emit
  only result JSON.
- The pleural/pericardial effusion workflow uses the TotalSegmentator
  `pleural_pericard_effusion` task after the automatic lung inventory gate. The
  public result exists only for positive cases. `measurement.present_findings`
  lists `pleural_effusion` and/or `pericardial_effusion`; each present finding
  has a true `has_<finding>` field, total volume, voxel count, and
  connected-component records. Empty masks remain visible only in internal job
  audit state and remove any stale previously published result for the same
  case.
- Complete-head geometric normalization includes
`normalized_brain_geometry_head_ct_2mm.nii.gz`, which uses the `total/brain`
mask to define a reproducible output plane. This is the preferred head geometry
artifact when an exact orbitomeatal line is not required.
- Experimental metrics modules must not silently enter the default
production-facing profile.
- `StudyInstanceUID`, `case_id`, and `job_id` are not interchangeable.
- Series selection should prefer maximum measured anatomical coverage first and
the thinnest available reconstruction only among coverage-equivalent eligible
series. Within the same coverage tier and effective spacing, reconstruction
preference must precede residual exact-coverage differences so a one-slice
coverage difference does not override a soft-tissue reconstruction. If
geometric metadata is absent, the selector falls back to the legacy slice-count
ranking.
- Series-selection text matching is case-insensitive and accent-insensitive so
equivalent terms such as `pulmao` and `pulmão` follow the same rule. Hard
rejections remain explicit profile rules; generic `sharp` kernel text is a soft
penalty rather than a global rejection. After phase and geometry constraints,
the selector can combine positive/negative description, kernel, and protocol
hints with auxiliary window classification and matching manufacturer rules.
Window center/width must not be used as a hard rejection because presentation
windows do not change the stored HU values.
- Prepared studies preserve source DICOM instances grouped by series under the
case workspace. `AvailableSeries` and `DiscardedSeries` may include
`SourceDicomSeriesPath` and `SourceDicomInstanceCount` so later operators can
audit or reprocess from the same series set. The upload ZIP remains a transport
artifact and is deleted after successful prepare.
- New prepared-series entries also preserve `Manufacturer`,
`ManufacturerModelName`, `ProtocolName`, `WindowCenter`, `WindowWidth`, and
`ReconstructionAlgorithm` when present. Selection audit records the preference
score, auxiliary window class, and applied manufacturer hint names. Historical
entries without these fields remain eligible through the existing phase,
geometry, description, and kernel rules.
- Organ volumes should not be published when every candidate organ is missing,
empty, or incomplete. The parenchymal overlay may still be emitted as an
`attenuation_only` artifact when an incomplete liver provides at least 100 cm³
of segmented tissue over at least 30 mm of axial extent. Incomplete organ
volumes remain omitted. The metrics result JSON preserves the reason,
per-organ status, physical attenuation sample size, and sample QC audit.
- Parenchymal-organ overlays render the volume number in red when liver volume
is greater than 1,800 cm³, spleen volume is greater than 400 cm³, or either
kidney volume is less than 100 cm³. The liver row is followed by a steatosis
line when liver attenuation is available. Examinations outside the inclusive
115-125 kVp range are identified as outside range; within that range, liver
attenuation of at least 50 HU or a liver-to-spleen attenuation ratio greater
than 1 is reported as no steatosis, otherwise the displayed whole-number
percentage is `-0.58 × liver HU + 38.2`. The result JSON records the assessment
status, kVp, source attenuation values, liver-to-spleen ratio when available,
and the displayed estimated percentage.
- Hepatic attenuation from an incomplete mask is eligible only when its sample
contains at least 100 cm³ over 30 mm of axial extent. When liver attenuation is
below 50 HU, an incomplete spleen must independently contain at least 20 cm³
over 20 mm before the liver-to-spleen ratio or percentage is reported. Eligible
partial assessments are labeled `partial coverage`; insufficient liver samples
are rejected, and insufficient spleen samples make a low-liver-HU assessment
indeterminate. These are deterministic engineering QC thresholds and are not
a clinical-validation claim.
- The parenchymal-organ overlay series may include a complete
`total/vertebrae_L1.nii.gz` mask as an overlay-only structure. L1 must not be
reported as an organ volume or attenuation measurement by
`parenchymal_organ_volumetry`; its measurement role is only to make the L1 mask
visible in the generated 5 mm Secondary Capture series.
- L3 muscle area and VAT/SAT measurements use the `tissue_types` masks on the
selected L3 slice, then exclude tissue components that touch dilated near-slice
projections of upper appendicular `total` masks (`humerus`, `scapula`,
`clavicula`) when those masks are available. Result JSON preserves raw
pixel/area values, cleaned values, and an exclusion audit for traceability.
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
- Secondary Capture DICOM series grouping is configurable per metrics profile
through
`execution.artifact_dicom_policy.secondary_capture_series_mode` and per
external `/jobs` submission through
`artifact_dicom_policy.secondary_capture_series_mode`. The supported values are
`separate` and `single_series`, and the default is `separate`. In
`single_series`, generated DICOM Secondary Capture artifacts for the case,
including instruction documents emitted as Secondary Capture, are rewritten to
share one `SeriesInstanceUID`, `SeriesNumber`, and `SeriesDescription`, with
unique sequential `InstanceNumber` values. Original artifact series form
contiguous blocks in export order, with anatomic ordering applied within each
block. Derived CT and Encapsulated PDF DICOM artifacts are not grouped.
- The effective artifact DICOM policy is recorded in
`metadata/id.json` under `Pipeline.metrics_pipeline.artifact_dicom_policy`.
When `single_series` rewrites at least one Secondary Capture file, the same
audit object includes `secondary_capture_series_instance_uid`.
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
- The `bone_health_l1_hu` metric reports the primary sagittal L1 trabecular
ROI mean as the classified value. Its overlay and result JSON may also include
neutral volumetric L1 attenuation means for the total mask and 1-5 mm 3D mask
erosions; these supplemental values do not carry color-band classification.
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
