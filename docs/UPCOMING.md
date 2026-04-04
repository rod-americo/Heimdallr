# UPCOMING

**Planned operational capabilities for Heimdallr's radiological infrastructure**

This roadmap tracks features that are intentionally **not fully implemented yet** in the open-source operational layer. Modules classified as proprietary clinical intelligence, navigation, assistive reporting, or workflow orchestration are tracked in the **Asha** repository.

> **Scope boundary** — Heimdallr concentrates on ingestion, transport, interoperability, normalization, deterministic quantification, de-identification, observability, and operational surfaces. Anything requiring clinical semantic interpretation, prioritization heuristics, or proprietary intelligence belongs to Asha.

## Why This File Exists

- Align engineering priorities for the open-source infrastructure layer.
- Prevent scope creep from proprietary intelligence concerns.
- Keep the operational backlog explicit for contributors.
- Turn infrastructure strategy into auditable implementation steps.

## Working Definitions

- **Friction (1–10)**: implementation-in-production effort, combining integration complexity, data quality constraints, validation burden, and operational change management.
- **Impact (1–10)**: expected operational value once adopted in real workflows.

## Guardrails (Non-Negotiable)

1. **Deterministic outputs**: same input + same version = same result.
2. **Privacy by design**: de-identification and least-privilege are mandatory for any external call.
3. **Measured delivery**: every module must ship with KPIs, logs, and rollback.
4. **No clinical intelligence here**: if a feature interprets, prioritizes, recommends, or drafts — it belongs in Asha.
5. **Cloud-native**: no `.env` files, no hardcoded secrets. Configuration via `HEIMDALLR_*` env vars and JSON profiles.

## Prioritized Module Backlog

Ordered by pipeline viability and delivery sequence.

| Rank | Module | Pillar | Impact | Friction | Why now |
|---|---|---|---:|---:|---|
| 1 | HL7-triggered prefetch orchestration | Interoperability | 8 | 4 | Immediate throughput win with low risk |
| 2 | DICOMweb-native transport layer | Interoperability | 7 | 5 | Standards-based retrieval for modern viewers |
| 3 | De-identification governance hardening | Security | 10 | 5 | Extend existing gateway with stronger policy and audit |
| 4 | Deterministic pseudonymization + crosswalk | Security | 7 | 5 | Traceable privacy-preserving workflows |
| 5 | On-prem AI gateway enforcement | Security | 7 | 5 | Enterprise deployment maturity |
| 6 | Opportunistic coronary calcium (CAC-DRS) | Quantification | 7 | 6 | Preventive-care value from opportunistic CT |
| 7 | Liver steatosis opportunistic pipeline | Quantification | 7 | 6 | Population-health insight from existing scans |
| 8 | Bandwidth-aware transfer scheduling | Interoperability | 6 | 4 | Reliability for constrained network links |
| 9 | Worklist data aggregation (operational part) | Orchestration | 7 | 5 | Normalized feed for downstream consumers |
| 10 | SLA clock tracking (operational part) | Orchestration | 6 | 4 | Timer-based breach tracking and audit log |
| 11 | Fracture detection — morphometric module | Quantification | 7 | 6 | Deterministic fracture morphometry from segmentation |
| 12 | Kidney stone longitudinal quantification | Quantification | 6 | 5 | Time-series burden tracking (deterministic part only) |

## Current Implemented Baseline

The following modules are live in the repository:

- DICOM C-STORE intake listener (`heimdallr.intake`)
- Case preparation and queue worker (`heimdallr.prepare`)
- Segmentation worker with TotalSegmentator orchestration (`heimdallr.segmentation`)
- Post-segmentation metrics engine with job-based modules (`heimdallr.metrics`)
- FastAPI control plane with dashboard and patient API (`heimdallr.control_plane`)
- Operations TUI (`heimdallr.tui`)
- De-identification gateway (`services/deid_gateway.py`)
- Automated organ volumetry (liver, spleen, kidneys)
- L3 skeletal muscle area and sarcopenia metrics
- L1 trabecular HU-based and volumetric BMD estimation
- Vertebral fracture morphometric screening
- Opportunistic osteoporosis composite scoring
- Body fat abdominal volumes and L3-level composition
- Renal stone burden quantification

## Current Prototypes

- Retroactive cohort reprocessing toolkit: operational scripts for archived-case metric recalculation and backfills

---

## Pillar A: Interoperability and Transport

### A1. HL7-triggered prefetch orchestration
- Parse ADT/ORM messages for scheduled study events.
- Trigger prior retrieval with modality/body-region/time relevance.
- Ensure priors are staged before first open.

Exit criteria:
- >90% prior availability for eligible studies.
- <60s trigger-to-prefetch-start latency.

### A2. Bandwidth-aware transfer scheduling
- Off-peak transfer windows for constrained links.
- Retry policy with deterministic backoff.
- Route across local cache, VNA, and external repositories.

Exit criteria:
- >98% prefetch success with audited retry behavior.

### A3. DICOMweb-native transport layer
- QIDO-RS/WADO-RS/STOW-RS adapters where DIMSE is suboptimal.
- Range-request friendly retrieval for browser viewers.

Exit criteria:
- Feature parity on key retrieval paths across DIMSE and DICOMweb.

---

## Pillar B: De-identification and API Security

### B1. De-identification governance hardening
- **Implemented baseline (status: mvp-internal)**: DICOM metadata redaction + pixel-level burned-text removal.
- Expand OCR-assisted masking coverage for edge overlay/annotation patterns.
- Tamper-evident audit trail for redaction decisions.

### B2. Deterministic pseudonymization + secure crosswalk
- Salted tokenization for stable pseudonyms.
- Encrypted, access-controlled crosswalk separation.
- Key rotation and lifecycle management.

### B3. On-prem AI gateway enforcement
- **Implemented baseline (status: mvp-internal)**: external inference requests pass through de-identification checks.
- Hardening: tamper-evident logs, policy attestations, and stricter outbound controls.
- Rate limiting and circuit breaker for external model calls.

Exit criteria:
- No direct PHI in external LLM/VLM payloads.
- Auditable inference trail for compliance review.

---

## Pillar C: Opportunistic Quantification

Extends the existing deterministic measurement pipeline. All modules produce structured numerical outputs from segmentation masks — no clinical interpretation or recommendation generation.

### C1. Opportunistic coronary calcium (CAC-DRS)
- Opportunistic coronary calcium detection in eligible non-gated chest CT.
- CAC-DRS-oriented structured classification from segmentation output.
- Structured output block with category and confidence score.
- Governance note: this module targets CAC-DRS deterministic scoring, not Agatston pipelines.

Exit criteria:
- Deterministic repeated-run consistency.
- Structured outputs consumable by downstream consumers.

### C2. Liver steatosis quantification
- Extend existing liver HU/volume metrics with fat-fraction proxy estimation.
- Deterministic output from segmentation masks — no clinical interpretation.

### C3. Fracture detection — morphometric module
- Deterministic vertebral height ratio measurements from segmentation masks.
- Genant-grade-style morphometric output.
- Structured numerical output only — triage logic and urgency flagging belong to Asha.

### C4. Kidney stone burden longitudinal quantification
- Time-series stone burden tracking from serial segmentations.
- Deterministic volume/count comparisons across studies.
- Navigation and urology routing intelligence belong to Asha.

---

## Pillar D: Operational Orchestration (Heimdallr scope only)

These modules provide the **operational substrate** for intelligence consumers. Heimdallr provides the data feeds, queues, clocks, and audit trails. The assignment intelligence, routing logic, and escalation decisions are consumed/supplied by Asha.

### D1. Worklist data aggregation
- Normalize study metadata from PACS/RIS sources into a unified feed.
- Expose `GET /api/worklist/pending` with standardized schema.
- Generic capacity-based assignment queue mechanics.
- Fairness constraints (round-robin, capacity limits) as configurable policy.

### D2. SLA clock tracking
- SLA window data model with configurable contractual deadlines.
- Breach timestamp tracking and state machine (`active → warning → breached`).
- Timer-based alerts and audit log of SLA state transitions.
- Emit `sla.breach_imminent` events for downstream consumers.

### D3. Reminder dispatch infrastructure
- Timer-based reminder queue for pending follow-up tasks.
- Notification dispatch via configured channels (webhook, email).
- Audit logging of reminder state transitions.
- Intelligence for *who* gets escalated and *when* belongs to Asha.

### D4. Outcome state tracking
- Completion/adherence counters for follow-up tasks.
- Generic reporting data store with configurable state machine.
- Audit trail of state transitions.
- Quality framework interpretation and reimbursement logic belong to Asha.

---

## Delivery Horizons

### Horizon 1 — Foundation (Safety + Throughput)
- HL7-triggered prefetch orchestration
- Bandwidth-aware transfer scheduling
- DICOMweb-native transport layer
- De-identification governance hardening
- Deterministic pseudonymization + crosswalk

### Horizon 2 — Quantification Expansion
- Opportunistic coronary calcium (CAC-DRS)
- Liver steatosis quantification
- Osteoporosis screening hardening (already implemented)
- Fracture morphometric module
- Kidney stone longitudinal quantification

### Horizon 3 — Operational Substrate
- Worklist data aggregation
- SLA clock tracking
- Reminder dispatch infrastructure
- Outcome state tracking
- On-prem AI gateway enforcement hardening

---

## Boundary Contracts with Asha

Heimdallr's operational substrate exposes data, events, and APIs. Asha consumes them to apply intelligence. The boundary is:

| Heimdallr provides | Asha consumes and decides |
|---|---|
| Normalized worklist feed | Assignment intelligence, subspecialty routing |
| SLA clock state + breach events | Risk-weighted escalation, predictive breach actions |
| Reminder dispatch queue | Escalation intelligence, recipient selection |
| Outcome state counters | Quality framework interpretation, performance insights |
| Deterministic quantification outputs | Clinical interpretation, navigation routing, report drafting |
| De-identified payloads | LLM/VLM inference, semantic extraction |
| Morphometric fracture measurements | Triage urgency classification, auditable flags |
| Longitudinal stone burden deltas | Urology navigation routing |

> Asha capabilities tracked in Asha's own roadmap: [`~/Asha/docs/ROADMAP.md`](https://github.com/rod-americo/Asha)

---

## Top Risk Register

1. PACS/RIS/EHR integration heterogeneity and legacy constraints.
2. GPU capacity bottlenecks for segmentation-heavy modules.
3. Regulatory variance across LGPD/GDPR/HIPAA-like environments.
4. DICOMweb adoption variance across vendor implementations.
5. Scope creep: intelligence features migrating into the infrastructure layer.

## Platform Definition of Done

A module is production-ready only when it includes:
- Functional implementation with rollback path.
- Security/privacy controls aligned with data class.
- Monitoring dashboards, logs, and runbooks.
- Validation datasets and acceptance metrics documented.
- Deterministic output verification for the same input/version set.
