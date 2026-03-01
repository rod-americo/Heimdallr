# Pipeline Card Implementation Guidelines

## Scope
- Source of truth: `docs/data/pipeline-modules.json`.
- Coverage in this document: future-facing cards with `repoState = not-started`.
- Prototype cards (`repoState = prototype`) are tracked in the strategy board but are intentionally excluded here because they require architecture hardening before standard implementation planning.
- Goal: provide initial implementation guidance per card, with a minimum kickoff standard.

## General Kickoff Rules
1. Open one issue per card with a clinical owner, technical owner, and operations owner.
2. Define baseline and primary metric before any implementation work.
3. Define the data contract (input, output, latency, auditability, and human-decision trace).
4. Run phased rollout: `shadow -> assistive -> monitored production`.
5. Define stop criteria (kill switch) and rollback plan per card.

## Minimum Criteria by Status
- `planned`: start with a 2-page design doc, 2-sprint backlog, and frozen validation dataset.
- `in-review`: close security/compliance/integration gaps before entering implementation.
- `exploratory`: 4-6 week timebox with explicit hypothesis, target metric, and discard criteria.

## Initial Delivery Template (per card)
- `Problem`: which delay/error/risk the card reduces.
- `Scope v1`: what is included and what is out of scope.
- `Data`: sources, minimum volume, sampling strategy, and QA.
- `Model/Logic`: initial approach and comparable baseline.
- `Human Loop`: mandatory review point and override rule.
- `SLO`: latency, availability, and acceptable error.
- `Go/No-Go`: minimum bar to move to the next phase.

## Initial Guidelines by Card (Ordered by Roadmap Rank)

| Rank | Card | Status | Initial implementation guideline |
|---|---|---|---|
| 1 | HL7-Triggered Smart Prefetch | planned | Start with 1 modality and 1 hospital; track prefetch hit rate, latency, and reduction in manual reopen/retrieval tasks. |
| 2 | Unified Worklist Orchestrator | planned | Start with deterministic rules (workload, subspecialty, fairness) before adaptive heuristics; audit reassignment decisions. |
| 4 | Deterministic Pseudonymization + Crosswalk | planned | Define stable pseudonym algorithm + segregated crosswalk vault; validate controlled reversibility and audit logging. |
| 5 | Follow-up Recommendation Extraction | planned | Start with rules-first extraction + human review; compare precision by specialty before advanced NLP scaling. |
| 6 | Urology Navigation Module | planned | Encode explicit clinical routing rules by finding; pilot at low volume with weekly multidisciplinary review. |
| 7 | Opportunistic Coronary Calcium (CAC-DRS) | planned | Start with ordinal classification and cardiothoracic review; measure agreement against manual reference reads. |
| 8 | Urgency Flagging and Reordering | planned | Build this as the shared queue-control layer first; consume detector outputs and policy rules before attempting broad live reprioritization. |
| 11 | Structured Report Copilot | planned | Start with 2 high-volume templates as a single-agent drafting layer, lock critical fields, and require human edit before sign-off. |
| 12 | Patient Follow-up Navigator | planned | Define owner assignment, due dates, and escalation SLA before adding more than 1 service line. |
| 13 | On-prem AI Policy Enforcement | planned | Apply default-deny outbound policy with allowlist destinations; log allow/block evidence per request. |
| 14 | Drift and Hallucination Control Framework | exploratory | Treat this as a cross-cutting control layer for reporting modules; define regression packs, drift detectors, and safe degradation policy before scale-up. |
| 15 | Agentic Workflow Coordinator | exploratory | Prototype one end-to-end workflow with human checkpoints; measure handoff correctness and execution failures. |
| 16 | Foundation Model Fine-Tuning Layer | exploratory | Build isolated sandbox with dataset/model versioning and rollback; advance only with reproducible local benchmark gains. |
| 17 | Temporal Imaging Intelligence (Delta Engine) | exploratory | Start with 1 organ and 1 delta type; validate prior-current matching robustness and false delta rates. |
| 18 | Causal Triage Simulator | exploratory | Simulate policies on frozen historical data; report SLA/fairness/latency impact before live testing. |
| 19 | Synthetic + Federated Validation Sandbox | exploratory | Define minimum benchmark suite and privacy metrics; run pilot with one partner before scaling. |
| 20 | Autonomous Follow-up Orchestrator (Human-Gated) | exploratory | Run in suggestion mode with explicit approval; measure follow-up closure rate and time-to-contact. |
| 21 | Prospective Trial Mode | exploratory | Start with 1 cohort and 2 objective endpoints; automate evidence capture to reduce operational bias. |
| 22 | General Surgery Navigation Module | planned | Start with simple routing rules and explicit exceptions; audit route deviations and response times. |
| 23 | Oncology High-Suspicion Navigation Router | planned | Define high-suspicion triggers with operational priority; start human-gated with aggressive monitored SLA. |
| 24 | Gynecology Navigation Module | planned | Map suspicion criteria by subtype and destination channel; validate communication language with care teams. |
| 25 | Fracture Detection and Triage Module | planned | Build this as a fracture-specific detector vertical that feeds the generic urgency layer; start with one anatomy subset and conservative thresholds. |
| 35 | Pre-classification + Priority Flag (CXR) | planned | Keep scope narrow to normal-vs-altered plus attention flagging for chest X-ray; do not overload it with full triage-agent responsibilities. |
| 37 | Lung Nodule Longitudinal Tracker | planned | Implement temporal matching with location/size tolerance; release alerts only after stability validation. |
| 38 | Aortic Aneurysm Surveillance Pipeline | exploratory | Start with standardized diameter measurement + guideline-based rules; review false positives in clinical board. |
| 39 | Kidney Stone Burden Longitudinal Module | planned | Define burden metric and cross-exam reconciliation; generate a single longitudinal trace per patient/episode. |
| 40 | Incidental Findings Closure Engine | planned | Prioritize top 3 findings with highest follow-up-loss risk; track documented closure rate and cycle time. |
| 41 | Prostate MRI Longitudinal PI-RADS Tracker | exploratory | Timebox harmonization across protocols and longitudinal PI-RADS consistency before pathway automation. |
| 42 | Bandwidth-Aware Transfer Scheduling | planned | Implement deterministic policies by time-window, network, and site; track delayed-transfer reduction without clinical impact. |
| 43 | DICOMweb-Native Transport Layer | planned | Start with QIDO/WADO in hybrid environment and DIMSE fallback; track compatibility by target PACS/viewer. |
| 44 | Hippocampal Volumetry Project | planned | Lock longitudinal standardization, protocol consistency, and scanner harmonization before trend usage. |
| 45 | Bone Lesion CT Pipeline | exploratory | Run HITL pilot on thoracolumbar spine with Spine-Mets bootstrap; measure assistive sensitivity and per-case review cost. |
| 46 | Radiology Triage Agent | exploratory | Treat this as the higher-order orchestration layer above detector-specific triage cards; prove multi-signal coordination value before autonomy claims. |
| 47 | Multi-Agent Report Orchestrator | exploratory | Scope this to multi-agent composition only; keep QA and recommendation steps separate so orchestration quality can be measured independently. |
| 48 | Agentic Report QA Gate | exploratory | Keep this strictly post-draft; add quality checks for image-text consistency and unsupported claims without blending it into draft generation. |
| 49 | Guideline-Cited Recommendation Agent | exploratory | Position this after drafting and QA; restrict output to citation-backed recommendations from approved guideline sources with explicit fallback when ungrounded. |
| 50 | Subtle-Finding Triage Safeguard | exploratory | Use this as a safety wrapper around triage modules, with sentinel rules and conservative fallback routing for failure-prone subtle findings. |
| 51 | Prospective Shadow-Mode Validator | exploratory | Keep this pre-production and silent-run only; compare radiologist-final outcomes, latency impact, and subgroup performance stability before operational influence. |
| 52 | Agent Drift and Bias Sentinel | exploratory | Reserve this for post-deployment continuous monitoring by modality, site, and subgroup, with alert thresholds and rollback criteria for sustained degradation. |

## Update Governance
- Update this file whenever a card changes status to `planned`, `in-review`, `exploratory`, `validation-ready`, `production-candidate`, or `implemented`.
- If a card moves to `repoState = prototype` or `repoState = implemented`, remove it from this table to keep focus on not-started roadmap work.
