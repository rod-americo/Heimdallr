# Decisions

This file records lightweight architectural decisions that affect how Heimdallr should evolve.

## How to Add a Decision

Each decision should include:

- date
- context
- decision
- impact
- tradeoff
- alternatives rejected

## Decisions

### 2026-07-11 - Gate effusion overlays by minimum volume

**Context**

Small TotalSegmentator effusion masks can create visually prominent overlays
despite limited segmented volume.

**Decision**

Require at least 50 mL total volume for pericardial effusion display. Require
at least 50 mL independently for each lateralized pleural side, displaying only
the sides that meet the threshold. Preserve raw detected volumes and the QC
decision in the job payload audit.

**Impact**

- Subthreshold findings do not publish a positive result or produce overlays.
- A qualifying unilateral pleural effusion does not display a subthreshold
  contralateral component.
- The metrics worker must be restarted before resident processing uses the QC.

**Tradeoff**

The thresholds are deterministic presentation QC values and are not a clinical
validation guarantee. Pleural components without reliable lateralization are
excluded because the per-side threshold cannot be evaluated.

---

### 2026-07-11 - Keep artifact blocks contiguous in single-series egress

**Context**

Global anatomic sorting of all Secondary Capture images interleaved distinct
overlay products, such as parenchymal volumetry and pleural/pericardial
effusion, when they covered overlapping axial positions.

**Decision**

In `single_series` mode, preserve the first-export order of each original DICOM
artifact series as a contiguous block. Sort spatial images by projected axial
position only within their original artifact block, followed by any non-spatial
images from that block in export order.

**Impact**

- Distinct tables, overlays, and instruction products no longer alternate by
  anatomy after sharing the Heimdallr artifact series.
- Instance numbers remain unique and sequential across the combined series.
- The metrics worker must be restarted before new cases use this ordering.

**Tradeoff**

The combined series is no longer globally monotonic by axial position across
artifact boundaries; viewers encounter a new anatomic sequence at each block.

**Alternatives rejected**

- Continue global axial sorting, which mixes semantically distinct products.
- Force separate DICOM series, which removes the explicitly configured
  `single_series` workflow.

---

### 2026-07-11 - Use contextual soft-tissue hints for CT series selection

**Context**

A Canon Aquilion Lightning chest study offered geometrically equivalent
mediastinal and lung reconstructions. The mediastinal series carried kernel
`BODY_SHARP`, while the lung series was described in Portuguese as `PULMAO`
with kernel `FC30`. The former policy rejected every kernel containing `sharp`
but recognized only the English word `lung`, causing TotalSegmentator and the
derived overlays to use the lung reconstruction.

**Decision**

Normalize selection text case and accents, recognize Portuguese pulmonary
descriptions, and stop treating generic `sharp` text as a hard rejection.
Preserve manufacturer, model, protocol, reconstruction algorithm, and window
metadata for each prepared series. After phase and geometry constraints, rank
equivalent candidates with configurable positive/negative text hints,
manufacturer-specific hints, and an auxiliary soft-tissue/lung window class.
Window values never hard-reject a series.

**Impact**

- Existing prepared studies benefit from multilingual description handling and
  remain selectable when the new metadata fields are absent.
- Newly prepared studies expose enough context to audit vendor-specific kernel
  hints and window-based tie-breaking.
- `SelectionReason` and candidate audit include preference score, window class,
  and applied manufacturer hint rules.
- Deployments must restart prepare and segmentation workers after adopting the
  policy and code.

**Tradeoff**

Manufacturer hints remain operational heuristics rather than a universal CT
kernel ontology. Phase and anatomic coverage continue to outrank presentation
preferences, and new scanner families may need explicit audited hints.

**Alternatives rejected**

- Hard-reject every kernel containing `sharp`, which incorrectly excludes
  vendor body-reconstruction names.
- Globally reject `FC30` without manufacturer or protocol context.
- Select from display window alone, which is presentation metadata and does not
  alter stored HU values.

---

### 2026-07-10 - Gate partial-organ steatosis by physical sample size

**Context**

The parenchymal-organ job previously required a complete liver mask before it
could present hepatic steatosis in the volume overlay. A truncated mask can
still contain a physically substantial attenuation sample, while slice count
alone is not comparable across reconstructions with different spacing.

**Decision**

Keep incomplete organ volumes non-publishable, but permit an
`attenuation_only` parenchymal overlay when an incomplete liver contains at
least 100 cm³ of segmented tissue across at least 30 mm of axial extent. For a
liver mean below 50 HU, require an incomplete spleen to contain at least 20 cm³
across at least 20 mm before using the liver-to-spleen ratio or reporting the
formula-derived percentage. Label accepted results as partial coverage and
record the sample measurements and thresholds in result JSON.

**Impact**

- Substantial partial liver and spleen masks can support the deterministic
  attenuation rule without presenting their incomplete volumes as valid.
- Low-liver-HU cases with an inadequate spleen sample are explicitly
  indeterminate instead of silently falling back to the liver-only formula.
- Metrics workers must be restarted before resident processing uses the new
  sampling policy and localized overlay lines.

**Tradeoff**

The physical sample thresholds are engineering QC defaults rather than
clinically validated cutoffs. Clinical validation and outcome calibration
remain outside the automated repository checks.

**Alternatives rejected**

- Use only slice count, which changes meaning with reconstruction spacing.
- Publish incomplete organ volumes alongside the attenuation assessment.
- Calculate a percentage from low liver attenuation when the spleen sample is
  unavailable or too small to evaluate the requested ratio condition.

---

### 2026-05-30 - Scope head structure QC per segmented structure

**Context**

Head CT artifacts can be clinically useful when the brain mask is complete even
if one `brain_structures` mask is truncated at a scan boundary. The previous
`head_complete_qc` behavior treated any incomplete brain-structure mask as a
global failure and suppressed all derived head artifacts.

**Decision**

Keep `total/brain.nii.gz` as the complete-head gate, but evaluate
`brain_structures` QC per mask. Missing, empty, geometry-incompatible, or
truncated structure masks are omitted from the volume table and overlay, while
complete structure masks still generate artifacts. The result JSON records the
omitted masks in `measurement.omitted_brain_structures`.

**Impact**

Complete-brain head cases can deliver derived CT, volume-table, and
brain-structure overlay artifacts even when a subset of segmented structures is
not reliable. Downstream consumers can inspect the omitted-structure audit
instead of inferring that all structures were measured.

**Tradeoff**

The artifact package may be partial for structures while the job succeeds. This
requires consumers to read the omitted-structure audit before treating a missing
structure as absent anatomy.

**Alternatives rejected**

- Continue suppressing all head artifacts when one brain-structure mask is
  truncated.
- Report volumes for truncated structures, which would make quantitative output
  look more complete than the segmentation supports.

### 2026-05-01 - Preserve `heimdallr/` as the main package

**Context**

The starter kit recommends a clean package root and allows `src/` only when it is a conscious packaging requirement. Heimdallr already has a maintained `heimdallr/` package with many module entrypoints, tests, imports, docs, and runtime assumptions.

**Decision**

Keep production code under `heimdallr/` and do not migrate to `src/heimdallr/` during structural recovery.

**Impact**

- Avoids a broad import and packaging migration unrelated to current risk.
- Keeps existing tests, module entrypoints, and operational commands stable.

**Tradeoff**

- The package does not mirror every starter-kit layer name.
- Structural clarity must be documented through real module boundaries instead
of a cosmetic directory move.

**Alternatives rejected**

- Move all code to `src/heimdallr/` during recovery.
- Split workers into new repositories without a runtime migration plan.

---

### 2026-05-01 - Use `config/doctor.json` for structural doctor policy

**Context**

Heimdallr already uses `config/` for versioned examples and operational configuration. The starter-kit doctor policy is small, versioned, and not host-local.

**Decision**

Store doctor policy in `config/doctor.json`.

**Impact**

- Keeps governance validation discoverable beside other config contracts.
- Avoids adding a new top-level `governance/` directory for one file.

**Tradeoff**

- `config/` now contains both runtime examples and repository governance
policy.
- Docs must make clear that `config/doctor.json` is tracked and not a secret or
host-local override.

**Alternatives rejected**

- Add `governance/doctor.json`.
- Rely on hardcoded script behavior with no policy file.

---

### 2026-05-01 - Prepare local hook support without installing it

**Context**

The repository is existing, may have local workflows, and the user requested a one-off branch for this task. Forcing hook installation could disrupt the maintainer's environment.

**Decision**

Add `.githooks/pre-commit` and `scripts/install_git_hooks.sh`, but do not run the installer automatically.

**Impact**

- The gate can be enabled intentionally.
- This recovery round does not mutate local Git hook configuration.

**Tradeoff**

- Enforcement is available but not guaranteed on every local commit.
- CI or maintainer workflow must opt in if gate enforcement becomes mandatory.

**Alternatives rejected**

- Auto-install hooks during this task.
- Omit hook support entirely.

---

### 2026-05-01 - Document LLM-adjacent residue as out-of-scope debt

**Context**

The repository contains existing dependency and settings residue related to external model services, while the repository boundary states that LLM, prompting, NLP, and report intelligence belong to Asha.

**Decision**

Do not remove compatibility residue in this structural recovery round, but document it as a boundary hotspot that must not be expanded inside Heimdallr.

**Impact**

- Avoids breaking unknown local workflows during documentation recovery.
- Makes future additions easier to reject or redirect to Asha.

**Tradeoff**

- Some dependency/config names still appear broader than the desired domain.
- A later cleanup may be needed to remove unused LLM-adjacent dependencies after
runtime impact is confirmed.

**Alternatives rejected**

- Remove dependencies and settings immediately without impact analysis.
- Treat the residue as permission to add intelligence workflows here.

---

### 2026-05-08 - Remove unused LLM-adjacent runtime clients

**Context**

Heimdallr's maintained boundary excludes LLM, NLP, prompt engineering, OpenAI, Anthropic, MedGemma, and report intelligence workflows. An impact audit found no production imports of the `openai` or `anthropic` Python clients; they remained only as dependency residue.

**Decision**

Remove unused LLM-adjacent client dependencies from `requirements.txt` and keep the default control-plane title aligned with radiological image MLOps instead of AI/reporting terminology.

**Impact**

- Reduces installation surface that does not belong to Heimdallr's current
runtime.
- Makes the documented project boundary visible in dependency metadata and
operator-facing defaults.
- Keeps future intelligence-layer work directed to Asha or another explicit
consumer.

**Tradeoff**

- Any untracked local workflow that still imports those clients must install
them outside Heimdallr or move to the appropriate companion repository.

**Alternatives rejected**

- Keep unused clients to preserve historical compatibility.
- Replace the removed clients with optional extras before a real in-repository
use case exists.

---

### 2026-05-21 - Prefer measured coverage before slice thickness in CT series selection

**Context**

CT studies can contain multiple reconstructions where the thinnest series is
not necessarily the most anatomically complete. Selecting only by slice count
or thickness can choose a high-resolution partial reconstruction instead of a
series that covers the anatomy needed for deterministic segmentation and
metrics.

**Decision**

Preserve the existing modality, phase, hard-reject, and follow-up rules, but
enrich prepared `AvailableSeries` metadata with measured DICOM geometry. When
configured, segmentation series selection ranks eligible CT series by measured
coverage first and uses the thinnest available spacing only among
coverage-equivalent series. If measured geometry is unavailable, selection
falls back to the legacy ranking.

**Impact**

- Improves default segmentation input selection for studies with partial thin
reconstructions.
- Keeps older prepared studies processable because the selector does not
require geometry fields.
- Adds audit fields to `metadata/id.json` so operators can inspect coverage and
thickness decisions.

**Tradeoff**

- Series selection now depends on DICOM geometry tags when present.
- Host overrides of `config/series_selection.json` must opt in to the new
`geometry_priority` settings if they replace the tracked profile.

**Alternatives rejected**

- Always choose the thinnest reconstruction regardless of coverage.
- Continue using slice count as the main proxy for anatomical coverage.

---

### 2026-05-22 - Accept per-job series selection policy for external submissions

**Context**

External orchestrators can already decide which metrics and delivery outputs a
submitted study needs, but CT series selection still depended only on the host
global `config/series_selection.json`. Broad opportunistic CT ingestion needs a
caller-owned way to declare the selection rule for a job without rewriting the
host profile for every consumer.

**Decision**

Add optional `series_selection_policy` to `POST /jobs`. The API accepts a JSON
object, stores it in the external submission sidecar, copies it into
`ExternalDelivery`, and the segmentation worker deep-merges it over the active
series-selection profile for that job only. The selection audit records whether
the policy came from config or external delivery.

**Impact**

- Lets external orchestrators evolve CT series eligibility without duplicating
Heimdallr deployments or mutating host-local config.
- Preserves the existing default profile for ordinary uploads and submissions
that omit the field.
- Keeps the selected series auditable in `metadata/id.json`.

**Tradeoff**

- The control plane, prepare worker, and segmentation worker must be restarted
together before relying on the new field in production.

**Alternatives rejected**

- Create one host-local Heimdallr profile per external consumer.
- Make the external orchestrator preselect and send only one series without
recording the actual selection policy in Heimdallr metadata.

---

### 2026-05-23 - Expose non-identifying queue capacity for external feeders

**Context**

External orchestrators can submit broad CT workloads through `/jobs`, but they
need a safe way to avoid starving Heimdallr or flooding runtime storage. Direct
SQLite reads over SSH would couple callers to host-local persistence and could
expose case identifiers.

**Decision**

Expose `GET /ops/queues` from the control plane. The endpoint returns queue
status counts, oldest pending timestamps, segmentation concurrency, and runtime
disk usage without returning case IDs, study UIDs, patient identifiers, package
paths, or callback URLs.

**Impact**

- Feeders such as Orchestrum can apply backpressure before downloading and
submitting more studies.
- Queue capacity becomes an HTTP contract instead of an SSH/SQLite convention.
- The endpoint is operational metadata only and does not change `/jobs`
acceptance, segmentation, metrics, or delivery semantics.

**Tradeoff**

The control plane must be restarted when this route is deployed, and access
control still depends on the host/network boundary because built-in FastAPI
authentication is not implemented.

**Alternatives rejected**

- Let feeders query `database/dicom.db` directly over SSH.
- Encode capacity decisions into `/jobs` admission without first exposing a
read-only operational signal.

---

### 2026-05-24 - Preserve prepared source DICOM series in case workspaces

**Context**

The upload ZIP is a transport artifact. For later reprocessing, model
comparison, or auditing series selection, the useful persisted input is the
scanned DICOM series set that prepare already materializes before conversion.
Keeping only the canonical NIfTI is sufficient to reproduce the selected volume,
but not enough to revisit series choice, phase, reconstruction kernel, or DICOM
metadata.

**Decision**

During prepare, persist scanned DICOM instances grouped by series under
`runtime/studies/<case_id>/source/dicom/series/`. Keep deleting the upload ZIP
after successful prepare. Let `space_manager` reclaim the complete case
workspace, including source DICOM, derived NIfTI, segmentation, metrics, and
logs, when disk usage crosses the host-local threshold.

**Impact**

- Later operators can reprocess from the original prepared DICOM series without
needing the transport ZIP.
- `AvailableSeries` and `DiscardedSeries` can point to the retained source
DICOM series for audit.
- Runtime storage per case increases until the case workspace is purged.

**Tradeoff**

- Disk pressure will rise faster on busy hosts.
- The existing threshold-based `space_manager` remains the retention boundary,
so this is not indefinite archival.

**Alternatives rejected**

- Keep only the canonical NIfTI.
- Retain the original upload ZIP as the reprocessing artifact.
- Add a separate long-term archive outside the case workspace in this change.

---

### 2026-05-24 - Migrate the runtime contract to Python 3.14

**Context**

macOS MPS validation now needs a Python runtime that aligns with current local
tooling and available PyTorch wheels, while Heimdallr still needs one canonical
dependency contract for local development, CI, and the `thor` validation host.
The previous Python 3.12 baseline is no longer the target for new runtime
rebuilds.

**Decision**

Set the project runtime contract to Python `>=3.14,<3.15`, update CI and
operator bootstrap commands to Python 3.14, and pin PyTorch-family dependencies
to Python 3.14-compatible releases in `requirements.txt`.

**Impact**

- Local and host venvs must be rebuilt before runtime parity claims are made.
- CI compiles against Python 3.14.
- `thor` remains the CUDA POC host, but its current Python 3.12 baseline is
historical until a Python 3.14 venv is provisioned and audited.

**Tradeoff**

- Existing Python 3.12 venvs can still be useful for comparison, but they are no
longer authoritative for dependency drift or TotalSegmentator smoke.
- Some heavyweight dependencies may require newer wheels than the former pins.

**Alternatives rejected**

- Keep Python 3.12 as the canonical runtime and run macOS MPS experiments in an
untracked side environment.
- Support a broad Python version range without validating dependency parity.

---

### 2026-05-24 - Remove the experimental API container stack

**Context**

The checked-in Dockerfile, compose file, ignore file, and container-specific
documentation were created for an experiment. Heimdallr's maintained runtime
model is a set of host-supervised Python services sharing one venv, SQLite
database, runtime filesystem, and host-local JSON configuration.

**Decision**

Remove the experimental Docker assets and container-specific documentation from
the repository. Keep operational guidance centered on Python entrypoints and
host supervisors.

**Impact**

- The repository no longer advertises or maintains a Docker/compose API stack.
- Runtime setup, restart policy, and validation docs describe the canonical
host-supervised model only.
- Future container support would require a new explicit decision and matching
runtime validation.

**Tradeoff**

- Operators who built local experiments from the removed files must keep those
outside this repository or reintroduce them through a reviewed architecture
decision.

**Alternatives rejected**

- Keep stale Docker files as unsupported examples.
- Convert the whole service stack to containers as part of this change.

---

### 2026-05-24 - Keep per-host stack manifests as ignored guardrails

**Context**

Heimdallr now runs across hosts with different accelerator and concurrency
profiles: `thor` is the CUDA POC host, `odin` is the local macOS MPS host, and
`ms-heimdallr` is a conservative CPU POC host. Reusing one host-local
segmentation or metrics profile across those machines can silently switch
TotalSegmentator to the wrong device or overrun a smaller host.

**Decision**

Keep concrete host stack manifests under ignored `config/host_stack/*.json`
files, and provide a versioned validator that checks accelerator policy,
TotalSegmentator `--device` flags, segmentation concurrency, and metrics worker
limits against the active host-local pipeline JSON.

**Impact**

- The repository records the guardrail mechanism without versioning concrete
machine manifests.
- Operators can validate the current host before restarting segmentation or
metrics workers.
- Stored manifests for other hosts can be sanity-checked without comparing them
to the current machine.

**Tradeoff**

- The guardrail depends on operators keeping the local manifest current when a
host is rebuilt, renamed, or given new accelerator capacity.

**Alternatives rejected**

- Encode host-specific worker/device choices directly in tracked examples.
- Trust profile filenames alone to imply CPU, MPS, or CUDA behavior.

---

### 2026-05-24 - Make CT phase-detector device policy explicit

**Context**

`totalseg_get_phase` defaults to `gpu`, then falls back inside TotalSegmentator
when CUDA is unavailable. On the local macOS/MPS host, the `mps` phase path
crashed and the implicit CPU fallback could stall under default PyTorch/nnU-Net
threading. The same detector completed successfully on `thor` with CUDA and on
local macOS CPU when CPU thread pools were bounded.

**Decision**

Keep `totalseg_get_phase` device selection explicit through
`HEIMDALLR_TOTALSEG_GET_PHASE_DEVICE`. Heimdallr defaults the phase detector to
`cpu` on macOS when the host does not set a value, and applies
`HEIMDALLR_TOTALSEG_GET_PHASE_THREAD_LIMIT=1` by default for that CPU subprocess.
On Apple Silicon, Heimdallr uses bounded process-level parallelism through
`HEIMDALLR_TOTALSEG_GET_PHASE_MAX_PARALLEL=1` because concurrent
phase-detector subprocesses can still fan out into multiple PyTorch/nnU-Net
children even when internal CPU thread pools are bounded. CUDA hosts should set
the device to `gpu`; Linux CPU hosts should set it to `cpu`.

**Impact**

- Avoids relying on the upstream `gpu` default for hosts without CUDA.
- Keeps local macOS prepare runs from hanging in the phase detector.
- Lets multi-phase CT studies use multiple CPU cores through independent
  one-thread detector subprocesses.
- Preserves fast CUDA behavior on `thor` when the host environment selects
  `gpu`.

**Tradeoff**

- Host supervision must carry one more explicit TotalSegmentator setting for
  predictable performance.
- macOS phase detection runs on CPU even when MPS is available until the MPS path
  is validated upstream or in a controlled local smoke.

**Alternatives rejected**

- Keep relying on `totalseg_get_phase` default device fallback.
- Use `mps` for phase detection on macOS after a local segfault.

---

### 2026-05-25 - Keep the macOS desktop track in `desktop/`

**Context**

Heimdallr needs a local macOS distribution path that can present a menu bar app,
guide OsiriX/Horos DICOM setup, manage segmentation profile configuration,
supervise resident workers, and eventually package a private Python runtime.
The first implementation needs close coordination with existing engine
contracts, runtime paths, config examples, and worker entrypoints.

**Decision**

Create a `desktop/` track inside this repository for the initial Swift menu bar
app, Go daemon, runtime manifests, and macOS packaging work. Keep the Python
engine under `heimdallr/` as the authoritative implementation of DICOM intake,
prepare, segmentation, metrics, queues, and DICOM egress. Maintain the detailed
desktop execution plan in `docs/DESKTOP.md`.

**Impact**

- Desktop and engine contracts can evolve atomically during the proof of
  concept.
- Agents have a clear location for Swift, Go, packaging, and manifest work.
- The monorepo now carries a platform wrapper track, but clinical pipeline
  behavior remains owned by the existing `heimdallr/` modules.

**Tradeoff**

- The repository will include multiple toolchains when implementation starts.
- CI and validation must avoid treating desktop packaging artifacts as core
  engine runtime state.
- A later split to a separate repository may still be appropriate if desktop
  releases become independent.

**Alternatives rejected**

- Create a separate `heimdallr-desktop` repository immediately, which would add
  coordination overhead before the engine/daemon/app contracts are stable.
- Put Swift, Go, and packaging files as loose top-level directories without a
  single desktop boundary.

---

### 2026-07-11 - Keep resident worker concurrency independent

**Context**

Prepare, segmentation, and metrics are separate resident services with
different CPU, accelerator, memory, and internal parallelism characteristics.
The existing segmentation-only `MAX_PARALLEL_CASES` name looked global, while
prepare and metrics serialized cases with hardcoded worker counts.

**Decision**

Give each service an independent case limit. Prepare additionally owns its
series-conversion and phase-detector limits; metrics separately owns profile
job concurrency. Keep every case limit at `1` by default. Preserve
`HEIMDALLR_MAX_PARALLEL_CASES` only as a compatibility fallback for
segmentation and use service-qualified environment names for new deployments.
API ingestion does not override resident service capacity.

**Impact**

- Hosts may tune each stage without changing other services.
- Four accepted API jobs do not imply four concurrent cases at every stage.
- Prepare can scale one-thread phase-detector subprocesses between studies
  without enabling unstable internal detector multithreading.

**Tradeoff**

- Operators must reason about case concurrency and per-case concurrency as
  separate multiplicative limits.
- Host manifests and runbooks must record each service capacity explicitly.

**Alternatives rejected**

- One global pipeline worker count, which cannot represent heterogeneous
  resource constraints.
- Deriving worker capacity from API server worker count.
- Increasing hardcoded executor sizes per host.
