# Module Classification: **HEIMDALLR** vs **ASHA**

This document defines the authoritative split between the open-source operational substrate (**Heimdallr**) and the proprietary intelligence layer (**Asha**).

**Rule of Thumb:**
- If it moves, converts, or calculates deterministic physical properties (vols, densities, ratios) from raw data -> **Heimdallr**.
- If it interprets, prioritizes, recommends, or assists clinical decision-making -> **Asha**.

---

## 1. Operational Substrate (HEIMDALLR)

| ID | Module | Rationale |
|---|---|---|
| 1 | DICOM C-STORE Intake | Pure connectivity and ingestion. |
| 2 | Case Prep & Queue Worker | Operational backbone for any imaging pipeline. |
| 3 | Dashboard & API Surface | Generic operational visibility and integration layer. |
| 4 | HL7-Triggered Smart Prefetch | Orchestration and logistics between clinical systems. |
| 5 | Unified Worklist Orchestrator | Infrastructure for equitable work distribution. |
| 6 | SLA Policy Engine | Operational monitoring and escalation logic. |
| 8 | TotalSegmentator Core | Scientific substrate; provides the masks for all subsequent analysis. |
| 9 | L3 Muscle / SMI | Deterministic physical calculation from segmentation masks. |
| 10 | Automated Organ Volumetry | Deterministic physical calculation of volume. |
| 14 | Opportunistic Liver Quant | Deterministic HU-based calculation from segmentation. |
| 15 | Opportunistic Bone Quant | Deterministic HU-based calculation from segmentation. |
| 16 | CAC-DRS Deterministic Quant | Physical quantification of calcium burden (substrate for triage). |
| 18 | Opportunistic Emphysema Quant | Deterministic LAA% calculation from segmentation. |
| 22 | Causal Triage Simulator | Operational workflow simulation and performance modeling. |
| 23 | Synthetic/Federated Sandbox | Infrastructure for platform-wide validation and benchmarking. |
| 25 | Prospective Trial Mode | Governance infrastructure for evidence capture. |
| 26 | De-identification Gateway | Security and compliance infrastructure. |
| 38 | Bandwidth-Aware Transfer | Logistics and network orchestration. |
| 39 | Pseudonymization & Crosswalk | Security and identity governance infrastructure. |
| 40 | DICOMweb-Native Transport | Interoperability and connectivity standard. |
| 42 | On-prem AI Policy Enforcement | Enterprise deployment governance and safety controls. |
| 51 | Prospective Shadow Validator | Operational rollout governance and monitoring. |
| 52 | Agent Drift & Bias Sentinel | Cross-cutting observability and quality governance. |
| 53 | CTR Extraction (ICT) | Deterministic ratio calculation from 2D coordinates. |
| 54 | Renal Stone Burden Quant | Deterministic physical quantification of stone properties. |
| 55 | Segmentation Service API | Infrastructure for remote execution of scientific models. |
| 56 | Cohort Reprocessing Toolkit | Operational utility for validation and data management. |

---

## 2. Intelligence & Clinical Layer (ASHA)

| ID | Module | Rationale |
|---|---|---|
| 7 | AP Chest X-ray Assist APIs | Assistive interpretation and clinical drafting aid. |
| 11 | Intracranial Hemorrhage Triage | High-value clinical interpretative signal for triage. |
| 12 | Structured Report Copilot | Generative AI layer for clinical report synthesis. |
| 13 | Urgency Flagging & Reordering | Intelligence-driven reprioritization of the worklist. |
| 17 | Hippocampal Longitudinal | Specialty neuro-intelligence and trend interpretation. |
| 19 | Agentic Workflow Coordinator | Intelligent orchestration of clinical handoffs and tasks. |
| 20 | Foundation Model Fine-Tuning | Proprietary competitive advantage in reasoning depth. |
| 21 | Delta Engine (Longitudinal) | Semantic change detection and clinical trend analysis. |
| 24 | Autonomous Follow-up Orch | Intelligent decision-making for care closure. |
| 27 | Patient Follow-up Navigator | Clinical pathway management and navigation logic. |
| 28 | Urology Navigation | Clinical vertical for finding-to-action routing. |
| 29 | Lung Nodule Tracker | Strategic clinical surveillance and growth interpretation. |
| 30 | Aortic Aneurysm Surveillance | Guideline-aligned clinical interpretation and escalations. |
| 31 | Renal Stone Longitudinal | Intelligence on stone growth and clinical progression. |
| 32 | Incidental Findings Closure | High-value clinical workflow intelligence for closure. |
| 33 | Prostate PI-RADS Tracker | Specialized clinical classification and trend monitoring. |
| 34 | General Surgery Navigation | Clinical vertical for surgical finding management. |
| 35 | Oncology High-Suspicion Router | High-stakes clinical triage and intelligence routing. |
| 36 | Gynecology Navigation | Clinical vertical for specialized female imaging findings. |
| 37 | Fracture Detection & Triage | Interpretative signal for urgent clinical escalation. |
| 41 | Follow-up Recommendation Ext | NLP-driven semantic extraction from clinical text. |
| 43 | Pre-classification Flag (CXR) | Interpretative "normal vs abnormal" triage logic. |
| 44 | Drift & Hallucination Control | Safety and quality intelligence for LLM-based products. |
| 45 | Bone Lesion CT Pipeline | Interpretative signal for metastatic/neoplastic triage. |
| 46 | Radiology Triage Agent | Multi-signal coordination and clinical reasoning over cases. |
| 47 | Multi-Agent Report Orch | Orchestration of diverse intelligence for clinical synthesis. |
| 48 | Agentic Report QA Gate | Intelligent auditing of clinical report claims. |
| 49 | Guideline-Cited Rec Agent | Evidence-based reasoning and clinical recommendations. |
| 50 | Subtle-Finding Triage Safe | Specialized safety intelligence for edge-case detection. |
