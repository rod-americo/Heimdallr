# UPCOMING

**Planned capabilities for Heimdallr's radiology preprocessing ecosystem**

This roadmap tracks features that are intentionally **not fully implemented yet**.
It is designed to protect delivery quality, clinical safety, and governance as the platform scales.

## Why This File Exists

- Align engineering, clinical, and governance priorities.
- Prevent rushed releases in high-risk clinical workflows.
- Keep scope explicit for contributors and partners.
- Turn strategy into auditable implementation steps.

## Working Definitions

- **Friction (1-10)**: implementation-in-production effort, combining integration complexity, data quality constraints, validation burden, compliance/governance controls, and operational change management.
- **Impact (1-10)**: expected clinical/operational value once adopted in real workflows.
- **Navigation**: active monitoring by a service team that owns each flagged patient until closure (`pending -> scheduled -> completed` or escalated).

## Guardrails (Non-Negotiable)

1. **Human-in-the-loop first**: AI prioritizes and drafts; clinicians decide.
2. **Privacy by design**: de-identification and least-privilege are mandatory for any external AI call.
3. **Workflow over isolated models**: optimize end-to-end throughput and follow-up completion.
4. **Measured delivery**: every module must ship with KPIs, logs, and rollback.
5. **Reproducibility**: deterministic outputs for the same input/version set.

## Prioritized Module Backlog

Ordered by strategic sequence (not only by model sophistication).

| Rank | Module | Pillar | Impact | Friction | Why now |
|---|---|---|---:|---:|---|
| 1 | HL7-triggered prefetch orchestration | Logistics | 9 | 4 | Immediate throughput win with low clinical risk |
| 2 | Unified worklist orchestration | Workflow | 9 | 5 | Reduces queue switching and unfair distribution |
| 3 | De-identification governance hardening | Security | 10 | 6 | Extend an implemented gateway with stronger policy and audit controls |
| 4 | Deterministic pseudonymization + crosswalk | Security | 10 | 5 | Enables traceable privacy-preserving workflows |
| 5 | Follow-up recommendation extraction (NLP) | Navigation | 8 | 5 | High value and operationally feasible |
| 6 | Urology navigation pathway | Navigation | 9 | 6 | Direct care-continuity impact with clear rule set |
| 7 | CAC-DRS coronary calcium classification | Quantification | 8 | 6 | High preventive-care value in routine CT workflows |
| 8 | AI-assisted urgency flagging | Workflow | 9 | 7 | Improves time-to-open for high-acuity cases |
| 9 | Liver steatosis opportunistic pipeline | Quantification | 7 | 6 | Population-health insight from existing scans |
| 10 | Osteoporosis opportunistic screening | Quantification | 7 | 6 | Scalable DXA-proxy value from routine CT |
| 11 | Structured report drafting copilot | Reporting | 8 | 7 | Reporting acceleration with strong guardrails |
| 12 | Emphysema quantification at scale | Quantification | 6 | 7 | Valuable but depends on segmentation throughput |
| 13 | On-prem AI gateway enforcement | Security | 9 | 7 | Critical for enterprise deployment maturity |
| 14 | Drift/hallucination control framework | Reporting | 9 | 8 | Needed before broad automation trust |
| 15 | Agentic workflow coordination | R&D | 8 | 9 | Long-horizon orchestration capability |
| 16 | Foundation model fine-tuning layer | R&D | 9 | 9 | Institution-adapted model performance with controlled governance |
| 17 | Temporal imaging intelligence (delta engine) | R&D | 9 | 8 | Longitudinal change tracking for follow-up-heavy workflows |
| 18 | Causal triage simulator | R&D | 8 | 8 | Safe policy simulation before live queue changes |
| 19 | Synthetic + federated validation sandbox | R&D | 8 | 8 | Privacy-preserving multi-site validation and benchmarking |
| 20 | Autonomous follow-up orchestrator (human-gated) | R&D | 9 | 8 | Strong continuity and financial recovery upside with clinical oversight |
| 21 | Prospective trial mode | R&D | 8 | 7 | Embedded publication-grade evidence generation |

## Pillar A: Logistics Automation and Smart Prefetch

### A1. HL7-triggered prefetch orchestration
- Trigger jobs from ADT/ORM events.
- Retrieve priors with modality/body-region/time relevance.
- Ensure priors are ready before first open.

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

## Pillar B: Workflow Orchestration and Clinical Triage

### B1. Unified worklist orchestration
- Single assignment engine across PACS/RIS sources.
- Rules: license scope, subspecialty, availability, fairness.
- "Go-to-next" mode with preloaded context.

### B2. AI-assisted urgency flagging
- Integrate emergency detectors for reprioritization.
- Confidence thresholds + full audit trail.
- Assistive only (never autonomous diagnosis).

### B3. SLA-aware policy engine
- Encode contractual windows and breach risk signals.
- Escalation paths for urgent pending/unassigned studies.

Exit criteria (B1-B3):
- Lower queue switching and lower workload variance.
- Reduced time-to-open for urgent studies.
- Near-zero untracked SLA breaches.

## Pillar C: Opportunistic Quantification and Precision Triage

### C1. Liver steatosis opportunistic pipeline
- 3D liver/spleen segmentation with HU analytics.
- Structured risk block for reporting templates.

### C2. Osteoporosis opportunistic screening
- L1-L4 segmentation with volumetric HU estimation.
- DXA-proxy fields + confidence and QC indicators.

### C3. Emphysema quantification at scale
- LAA%-based burden score.
- Optional lobe-level distribution and longitudinal trend.

### C4. Coronary calcium classification (CAC-DRS)
- CAC-DRS-oriented structured classification for eligible non-gated chest CT.
- Pre-report block with category, confidence, and recommendation text.
- Governance note: this module targets CAC-DRS workflows, not Agatston scoring pipelines.

Exit criteria (C1-C4):
- Deterministic repeated-run consistency.
- Structured outputs consumable by reporting and analytics.

## Pillar D: LLM/VLM Reporting Copilot

### D1. Structured report drafting
- Controlled template generation from findings + quant metrics.
- Radiologist style presets and macro-aware output.

### D2. Ambient assistance and hotkeys
- Low-latency text refinement in reporting environments.
- Prompt profiles by exam type.

### D3. Drift and hallucination controls
- Shadow-mode evaluation.
- Version pinning and regression packs.
- Red-team prompts + fail-safe fallback.

Exit criteria:
- Measured speed-up without hidden factual drift.
- Versioned prompt/model QA logs for traceability.

## Pillar E: De-identification and API Security

### E1. Multimodal PHI removal pipeline
- **Implemented baseline**: DICOM metadata redaction + pixel-level burned-text removal before external calls.
- Expand OCR-assisted masking coverage for edge overlay/annotation patterns.

### E2. Deterministic pseudonymization + secure crosswalk
- Salted tokenization for stable pseudonyms.
- Encrypted, access-controlled crosswalk separation.

### E3. On-prem AI gateway enforcement
- **Implemented baseline**: external inference requests pass through de-identification checks.
- Pending hardening: tamper-evident logs, policy attestations, and stricter outbound controls.

Exit criteria:
- No direct PHI in external LLM/VLM payloads.
- Auditable inference trail for compliance review.

## Pillar F: Patient Navigation and Closing the Loop

### F1. Follow-up recommendation extraction
- NLP extraction from finalized reports.
- Due date + modality + urgency structured tasking.

### F2. Reminder and escalation workflows
- Notifications to ordering teams and navigation staff.
- Dashboards for pending/overdue/completed queues.

### F3. Outcome tracking and quality reporting
- Completion/adherence metrics.
- Quality framework support and reimbursement reporting hooks.

### F4. Urology navigation pathway (pre-report + post-report)

Pre-report support:
- Detect and structure candidate findings for navigation handoff.
- Normalize measurement units and threshold logic.

Post-report routing:
- Open navigation tasks for the following findings:
  - Renal stones `>= 0.7 cm`
  - Simple renal cysts `> 10 cm`
  - Bosniak `III/IV` cysts
  - Solid renal nodules (any size)
  - Solid adrenal nodules `> 3 cm`
  - Solid ureteral nodules
  - Ureteropelvic junction stenosis
  - Ureteral stones
  - Solid or vegetative bladder lesions (any size)
  - Prostate findings: PI-RADS `3-5`, or volume `> 70 g`, or `> 40 g` with outlet obstruction signs
  - Solid testicular lesions (any size)
- Secondary routing rule: if neoplasm suspicion exists, also route to Oncology Navigation.

Exit criteria:
- Service ownership assigned for 100% of eligible navigable findings.
- Reduced lost-to-follow-up for urology-sensitive findings.

## Pillar G: Disruptive and Long-Horizon R&D

### G1. Agentic workflow coordination
- Multi-step agents for prep, triage, and handoff orchestration.

### G2. Foundation model fine-tuning layer
- Governance-controlled adaptation of VLM/LLM models to institutional data.
- Versioned evaluation packs, safety gates, and rollback paths.

### G3. Temporal imaging intelligence (longitudinal delta engine)
- Structured deltas across prior/current exams (volume, attenuation, growth/atrophy trajectories).
- Explicit confidence and stability flags for longitudinal interpretation support.

### G4. Causal triage simulator
- Offline simulation of triage/worklist policies before production rollout.
- Compare SLA, workload fairness, and critical-case latency under multiple assignment strategies.

### G5. Synthetic + federated validation sandbox
- Privacy-preserving validation across institutions without raw-image centralization.
- Fused module: combines federated development and synthetic-data benchmarking in a single validation pipeline.

### G6. Autonomous follow-up orchestrator (human-gated)
- AI proposes follow-up tasks, deadlines, and routing, but requires human approval for activation.
- Built-in escalation logic for high-risk overdue recommendations.

### G7. Prospective trial mode
- Native support for trial cohorts, intervention arms, and endpoint capture.
- Operational logs designed for publication-grade reproducibility.

## Suggested Delivery Horizons

Horizon legend:
- **Foundation**: baseline safety, interoperability, and throughput capabilities required before broader automation.
- **Acceleration**: modules that increase clinical/operational performance on top of a stable foundation.
- **Scaling**: expansion of validated workflows to higher volume, more services, and wider operational coverage.
- **Innovation**: long-horizon capabilities with higher uncertainty and strategic differentiation potential.

### Horizon 1 - Foundation (Safety + Throughput)
- A1, A2, B1, E1, E2

### Horizon 2 - Clinical Acceleration
- B2, B3, C1, C2, C4, D1

### Horizon 3 - Care Continuity at Scale
- F1, F2, F3, F4, C3, D2, E3

### Horizon 4 - Strategic Innovation
- D3, G1, G2, G3, G4, G5, G6, G7

## Top Risk Register

1. Regulatory variance across LGPD/GDPR/HIPAA-like environments.
2. PACS/RIS/EHR integration heterogeneity and legacy constraints.
3. Model/API drift and vendor-side behavior changes.
4. GPU capacity bottlenecks for segmentation-heavy modules.
5. Clinical adoption risk without explicit governance ownership.

## Platform Definition of Done

A module is production-ready only when it includes:
- Functional implementation with rollback path.
- Clinical safety boundaries and reviewer accountability.
- Security/privacy controls aligned with data class.
- Monitoring dashboards, logs, and runbooks.
- Validation datasets and acceptance metrics documented.
