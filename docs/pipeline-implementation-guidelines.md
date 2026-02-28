# Pipeline Card Implementation Guidelines

## Scope
- Source of truth: `docs/data/pipeline-modules.json`.
- Coverage in this document: future-facing cards with `status != poc` and `status != mvp-internal`.
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
| 8 | Urgency Flagging and Reordering | in-review | Finalize confidence thresholds and audit trail before rollout; start in shadow mode with no automatic reordering. |
| 11 | Structured Report Copilot | planned | Start with 2 high-volume templates, lock critical fields, and require human edit before sign-off. |
| 12 | Patient Follow-up Navigator | planned | Define owner assignment, due dates, and escalation SLA before adding more than 1 service line. |
| 13 | On-prem AI Policy Enforcement | planned | Apply default-deny outbound policy with allowlist destinations; log allow/block evidence per request. |
| 14 | Drift and Hallucination Control Framework | exploratory | Define input/output drift detectors and safe degradation policy; trigger automatic fallback by threshold. |
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
| 25 | Fracture Detection and Triage Module | in-review | Move to production only after subgroup anatomical validation and threshold calibration to minimize undertriage. |
| 35 | Pre-classification + Priority Flag (CXR) | planned | Start in shadow mode and prove faster case-open times without clinically relevant error increase. |
| 37 | Lung Nodule Longitudinal Tracker | planned | Implement temporal matching with location/size tolerance; release alerts only after stability validation. |
| 38 | Aortic Aneurysm Surveillance Pipeline | exploratory | Start with standardized diameter measurement + guideline-based rules; review false positives in clinical board. |
| 39 | Kidney Stone Burden Longitudinal Module | planned | Define burden metric and cross-exam reconciliation; generate a single longitudinal trace per patient/episode. |
| 40 | Incidental Findings Closure Engine | planned | Prioritize top 3 findings with highest follow-up-loss risk; track documented closure rate and cycle time. |
| 41 | Prostate MRI Longitudinal PI-RADS Tracker | exploratory | Timebox harmonization across protocols and longitudinal PI-RADS consistency before pathway automation. |
| 42 | Bandwidth-Aware Transfer Scheduling | planned | Implement deterministic policies by time-window, network, and site; track delayed-transfer reduction without clinical impact. |
| 43 | DICOMweb-Native Transport Layer | planned | Start with QIDO/WADO in hybrid environment and DIMSE fallback; track compatibility by target PACS/viewer. |
| 44 | Hippocampal Volumetry Project | planned | Lock longitudinal standardization, protocol consistency, and scanner harmonization before trend usage. |
| 45 | Bone Lesion CT Pipeline | exploratory | Run HITL pilot on thoracolumbar spine with Spine-Mets bootstrap; measure assistive sensitivity and per-case review cost. |
| 46 | Radiology Triage Agent | exploratory | Start with emergency-only cohorts and deterministic escalation rules; prove time-to-read gains without clinically relevant undertriage increase. |
| 47 | Multi-Agent Report Orchestrator | exploratory | Pilot one modality with fixed agent roles and mandatory human sign-off; measure draft quality, handoff failures, and edit burden. |
| 48 | Agentic Report QA Gate | exploratory | Add post-draft quality checks for image-text consistency and unsupported claims; block release on low-confidence or contradiction triggers. |
| 49 | Guideline-Cited Recommendation Agent | exploratory | Restrict output to citation-backed recommendations from approved guideline sources; require explicit fallback when no grounded citation exists. |
| 50 | Subtle-Finding Triage Safeguard | exploratory | Define subtle-finding sentinel rules and conservative fallback routing; monitor undertriage-related misses by subgroup before scale-up. |
| 51 | Prospective Shadow-Mode Validator | exploratory | Run silent prospective validation across sites and shifts; compare radiologist-final outcomes, latency impact, and subgroup performance stability. |
| 52 | Agent Drift and Bias Sentinel | exploratory | Deploy continuous monitoring by modality, site, and subgroup with alert thresholds and automatic rollback criteria for sustained degradation. |

## Update Governance
- Update this file whenever a card changes status to `planned`, `in-review`, `exploratory`, `validation-ready`, `production-candidate`, or `implemented`.
- If a card moves to `poc` or `mvp-internal`, remove it from this table to keep focus on post-POC implementation.
