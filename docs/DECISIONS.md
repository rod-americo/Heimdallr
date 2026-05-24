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
