# UPCOMING

**Planned capabilities for Heimdallr's radiology preprocessing ecosystem**

This document tracks features that are intentionally **not fully implemented yet**. It is organized by strategic pillars and delivery horizons so the team can phase rollout with clinical safety and governance controls.

## Guiding Principles

1. **Human-in-the-loop first**: AI can prioritize and draft, but clinicians own final decisions.
2. **Privacy by design**: de-identification and least-privilege access are mandatory for external AI APIs.
3. **Workflow over single models**: optimize end-to-end turnaround and follow-up closure, not isolated model benchmarks.
4. **Measurable operations**: every module must expose observable impact (TAT, SLA, escalation latency, follow-up completion).

## Pillar A: Logistics Automation and Smart Prefetch

### A1. HL7-triggered prefetch orchestration
- Trigger prefetch jobs from ADT/ORM events.
- Resolve prior exams with modality/body-region/time-window relevance scoring.
- Dispatch relevant priors to reading context before case open.

Acceptance targets:
- Prior availability at first-open for >90% eligible studies.
- Sub-minute trigger-to-prefetch-start latency.

### A2. Bandwidth-aware transfer scheduling
- Add off-peak transfer windows and retry policies for constrained links.
- Introduce routing rules for local cache, VNA, and external repositories.

Acceptance targets:
- Reduced network contention during peak clinical hours.
- Prefetch success rate >98% with deterministic retry behavior.

### A3. DICOMweb-native transport layer
- Add QIDO-RS/WADO-RS/STOW-RS adapters where DIMSE is not ideal.
- Support browser-friendly image fetch patterns and range-based retrieval.

Acceptance targets:
- Feature parity for key retrieval workflows across DIMSE and DICOMweb.
- Observable reduction in first-image latency in web viewers.

## Pillar B: Workflow Orchestration and Clinical Triage

### B1. Unified worklist orchestration
- Aggregate multi-source worklists into a single assignment engine.
- Assign studies by license scope, subspecialty, availability, and fairness constraints.
- Add "go-to-next" mode with preloaded case context.

Acceptance targets:
- Reduced manual queue switching.
- Improved fairness distribution and lower variance per-reader workload.

### B2. AI-assisted urgency flagging
- Integrate critical finding detectors for emergency reprioritization.
- Reorder worklists using confidence thresholds and audit trails.

Acceptance targets:
- Lower time-to-open for high-acuity studies.
- No autonomous final diagnosis; all flags remain assistive.

### B3. SLA-aware orchestration policy engine
- Encode contractual/SLA timing windows into queue policies.
- Real-time alerts for breach risk and unassigned critical cases.

Acceptance targets:
- Near-zero untracked SLA breaches.
- Explicit escalation paths for pending urgent studies.

## Pillar C: Opportunistic Quantification and Precision Triage

### C1. Liver steatosis opportunistic pipeline
- 3D liver/spleen segmentation and HU-based attenuation analytics.
- Structured steatosis risk stratification from eligible CT exams.

### C2. Osteoporosis opportunistic screening
- Vertebral segmentation (L1-L4) with volumetric HU estimation.
- DXA-proxy reporting fields and quality gates.

### C3. Emphysema quantification at scale
- LAA%-driven pulmonary burden scoring.
- Optional lobe-level distribution and longitudinal trend markers.

Cross-module acceptance targets:
- Deterministic reproducibility for repeated runs.
- Structured output fields suitable for analytics and downstream reporting.

## Pillar D: LLM/VLM Reporting Copilot

### D1. Structured report drafting
- Controlled template generation from findings + quantified metrics.
- Radiologist style presets and macro-aware post-editing.

### D2. Ambient assistance and hotkey workflows
- Low-latency text refinement in reporting environments.
- Context-aware prompt profiles per exam type.

### D3. Drift and hallucination controls
- Shadow mode validation, regression tests, and provider version pinning.
- Red-team prompts and fail-safe fallback behavior.

Acceptance targets:
- Assistive speed-up without hidden factual drift.
- Versioned prompts, model identifiers, and QA logs for traceability.

## Pillar E: De-identification and API Security

### E1. Multimodal PHI removal pipeline
- DICOM metadata redaction and pixel-level burned-text detection/removal.
- OCR-assisted masking for overlays and annotations.

### E2. Deterministic pseudonymization + secure crosswalk
- Salted tokenization for stable pseudonyms.
- Encrypted crosswalk store with strict access controls.

### E3. On-prem AI gateway enforcement
- Ensure external API traffic is de-identified by policy.
- Add tamper-evident audit logging for all outbound payloads.

Acceptance targets:
- No direct PHI in outbound LLM/VLM requests.
- Security audit trail for every external inference call.

## Pillar F: Patient Navigation and Closing the Loop

### F1. Follow-up recommendation extraction
- NLP extraction of follow-up recommendations from radiology reports.
- Structured tasks by due date, modality, and urgency.

### F2. Reminder and escalation workflows
- Notification pipelines for ordering clinicians and care navigation teams.
- Dashboard for pending, overdue, and completed follow-ups.

### F3. Outcome tracking and quality reporting
- Completion metrics and adherence rates.
- Support for quality frameworks and reimbursement reporting.

Acceptance targets:
- Improved follow-up completion rates.
- Reduced lost-to-follow-up incidents.

## Pillar G: Disruptive and Long-Horizon R&D

### G1. Agentic workflow coordination
- Multi-step agents for orchestration tasks (case prep, triage, follow-up handoff).
- Role-scoped execution with strict policy boundaries.

### G2. Dynamic digital twins for longitudinal care
- Integrate serial imaging and non-imaging biomarkers for trajectory modeling.

### G3. Federated learning and synthetic data
- Privacy-preserving model development across institutions.
- Synthetic data augmentation for rare conditions.

### G4. Sustainability-aligned hardware adaptation
- Support pipelines optimized for low-helium MRI and photon-counting CT outputs.

## Suggested Delivery Horizons

### Horizon 1 (Foundation)
- A1, B1, E1, E2
- Baseline governance, observability, and queue fairness controls

### Horizon 2 (Clinical Acceleration)
- B2, B3, C1, C2, D1
- Urgency-aware operations and structured draft reporting

### Horizon 3 (Care Continuity)
- F1, F2, F3, C3, D2, E3
- Closing-the-loop and enterprise-grade AI gateway hardening

### Horizon 4 (Strategic Innovation)
- D3, G1, G2, G3, G4
- Advanced agentic, longitudinal, and cross-institution capabilities

## Dependencies and Risks

- Regulatory variance by jurisdiction (LGPD/GDPR/HIPAA equivalents)
- PACS/RIS/EHR integration heterogeneity
- Model drift and vendor API changes
- GPU capacity planning for high-volume segmentation
- Change management for clinical adoption and governance

## Definition of Done (Platform Level)

A feature is considered production-ready only when it includes:
- Functional implementation with rollback path
- Clinical safety guardrails and human-review boundaries
- Security/privacy controls appropriate to data class
- Monitoring dashboards, logs, and operational runbooks
- Validation datasets and acceptance metrics documented
