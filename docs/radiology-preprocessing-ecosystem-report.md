# Architecture and Implementation of a Radiology Preprocessing Ecosystem
## From Logistics Automation to Agentic AI and Patient Navigation

## Executive Motivation

Contemporary radiology faces a structural paradox: image acquisition technology has reached unprecedented levels of speed and resolution, while human workflow remains constrained by administrative overhead, data fragmentation, and specialist shortages.[1] In 2025-2026, the answer is no longer isolated detection models, but a holistic preprocessing pipeline that orchestrates logistics, clinical triage, reporting assistance, and patient navigation.[3]

This document translates and consolidates the research rationale behind Heimdallr into an implementation-oriented architecture. The intent is practical and strategic: build a clinically safe, technically rigorous, and collaboration-ready foundation for an AI-enabled radiology operating layer.

## 1. Logistics Automation and Data Acquisition Autonomy

The first stage of an effective preprocessing pipeline is eliminating data-availability friction. Radiologists still lose time manually searching prior studies and reconciling data across PACS, RIS, and EHR systems.[5]

### 1.1 Trigger Mechanisms and Prefetch Logic

Prefetch efficiency depends on precise trigger logic based on HL7 messages or DICOM headers.[7] ADT and ORM flows allow the system to anticipate imaging needs before the patient enters the scanner room.[7] Once an ORM arrives, a prefetch engine can query VNA/PACS via DICOM C-FIND to retrieve relevant prior studies by date, modality, and anatomy.[7]

A practical relevance model can be expressed as metadata similarity weighting (recency, modality match, anatomical region match), with coefficients tuned to prioritize same-region and recent studies.[7]

Solutions such as Candelis Unifier/ImageGrid enable automatic routing of priors and historical reports to reading workstations, reducing cognitive and operational latency at interpretation time.[6] In low-bandwidth regions, transfer jobs can be scheduled for off-peak windows to optimize network usage.[7]

### 1.2 Cloud-Native Evolution and DICOMweb

Migrating from on-prem client/server architectures to cloud-native platforms increases scalability.[9] DICOMweb (WADO-RS, QIDO-RS, STOW-RS) simplifies API-based AI integration and lightweight viewer streaming compared with classic DIMSE-only workflows.[11] Object storage plus caching and range requests improves viewer responsiveness and can significantly reduce archival cost.[11]

## 2. Workflow Orchestration and Intelligent Clinical Triage

Radiology triage has evolved from FIFO queues to dynamic orchestration by clinical urgency, subspecialty, and workload balancing.[13] The target outcome is straightforward: protect specialist time for high-judgment tasks while automation handles administrative flow and urgency routing.[2]

### 2.1 Fair Distribution and Anti Cherry-Picking

Large radiology groups often suffer from selective case picking, overloading peers with complex studies. Modern orchestrators (e.g., Merge Workflow Orchestrator) distribute work fairly and consolidate multiple lists into a unified interface.[13]

Reported operational gains include:[13][14]
- +34% fairer workload distribution
- up to -50% manual workflow steps
- +23% to +35% physician throughput
- full SLA compliance in orchestrated environments
- sub-second first-image access in cloud-first setups

Assignment logic should account for license scope, real-time availability, and subspecialty fit.[14] A "go-to-next" mode can pre-load context (history, labs, prior findings) through HL7/FHIR integrations.[11]

### 2.2 AI Urgency Flagging

Early-detection algorithms for critical findings (e.g., intracranial hemorrhage, LVO, suspicious pulmonary nodules) can automatically reorder worklists.[16] Platforms such as Viz.ai and Aidoc have shown major treatment-time reductions in stroke pathways, including reports of up to 66 minutes in selected scenarios.[16]

This is not autonomous diagnosis. It is queue intelligence: ensuring the most critical case is opened next.[2]

## 3. Opportunistic Precision Triage Through Anatomical Segmentation

One of the highest-value preprocessing capabilities is opportunistic screening: extracting population-health biomarkers from studies acquired for unrelated indications.[4]

### 3.1 Liver Steatosis (HU-based)

NAFLD impacts up to one-third of the global population and is tightly linked to cardiometabolic risk.[20] A practical pipeline segments liver and spleen volumes in 3D and computes attenuation in HU.[20] Open methods such as ALARM can automate central/peripheral ROI extraction.[20]

Low-dose chest CT (LDCT) liver-in-volume analysis has shown strong agreement with standard abdominal CT in selected studies.[24]

### 3.2 Opportunistic Osteoporosis

Routine CT-based osteoporosis screening can materially increase diagnosis rates and downstream prevention value.[18] Deep models segment L1-L4 vertebrae and calculate volumetric HU as a DXA proxy.[21] Volumetric methods outperform single-slice manual measurements in heterogeneous bone patterns.[21][25]

### 3.3 Emphysema Quantification

AI-driven emphysema analysis can use LAA% (low attenuation area percentage) and lobe-level segmentation. TotalSegmentator supports robust multi-structure segmentation for this class of workflow.[26][27]

## 4. LLM/VLM Integration for Reporting Assistance

LLMs and VLMs are becoming radiology copilots, shifting from transcription support to structured report drafting and synthesis.[4][29]

### 4.1 AI Reporting Assistants and Macros

Tools such as Rad AI Reporting and BlabbyAI can transform dictated findings into personalized impressions, reducing fatigue and report turnaround time.[2][30]

Ambient AI/scribe systems are also advancing, with workflow integrations across EHR/PACS interfaces and hotkey-driven modality contexts.[31][33]

### 4.2 Precision, Drift, and Shadow AI Risks

Core risks include hallucinations, performance drift after provider updates, and ungoverned "Shadow AI" usage by clinicians.[3][29] Mitigations include domain-specific tuning, continuous validation, red-teaming, and strict institutional governance.[29]

## 5. De-Identification and External API Security

If external LLM APIs are used, robust de-identification is mandatory for LGPD/GDPR-aligned handling of healthcare data.[35]

A production-safe de-identification pipeline should include:[35][37]
- PHI detection in metadata and image overlays
- metadata redaction and burned-in pixel text removal
- deterministic pseudonymization with salted cryptographic tokens
- encrypted crosswalk for controlled re-identification
- on-prem de-identification gateway before outbound API calls

Configuration errors in real-world healthcare AI deployments have already led to expensive remediation events, reinforcing the need for continuous audits and tamper-evident logging.[35]

## 6. Patient Navigation and Closing the Loop

Preprocessing should not stop at report generation. A robust system must close the loop by ensuring follow-up recommendations become clinical action.[41][42]

NLP pipelines (e.g., transformer variants such as ClinicalBERT-class approaches) can identify follow-up recommendations and track completion with high precision in published studies.[43][44]

Navigation platforms (e.g., Radloop, RADNAV, Nursenav) can automate reminders, escalation, and navigator dashboards, improving adherence and reducing lost follow-up risk.[41][45][46]

## 7. Disruptive Frontier Capabilities

Emerging capabilities expected to reshape radiology operations include:[2][9][47][48][49]
- agentic AI for multi-step workflow orchestration
- dynamic digital twins for longitudinal oncology adaptation
- federated learning and synthetic data for privacy-preserving model development
- sustainability-aware imaging workflows (low-helium MRI, photon-counting CT, AI reconstruction)

## Strategic Synthesis

For 2026 and beyond, success in radiology AI will be measured less by isolated reading speed and more by measurable clinical impact, workflow reliability, and follow-up completion.

A next-generation preprocessing ecosystem should operate as an "AI operating layer" where logistics, de-identification, triage, reporting assistance, and care navigation are integrated under clear governance.

## Positioning and Collaboration Intent

This initiative is intentionally positioned at the intersection of software engineering, imaging informatics, and clinical operations. The long-term intent is threefold:

1. **Technical leadership**: establish a practical reference architecture for AI-enabled radiology operations.
2. **Collaborative growth**: attract contributors across engineering, radiology, product, and compliance.
3. **Academic and economic outcomes**: produce publishable implementation evidence, reusable tooling, and sustainable value creation.

## References

1. The Effect of AI on the Radiologist Workforce: A Task-Based Analysis (medRxiv). Accessed February 7, 2026. https://www.medrxiv.org/content/10.64898/2025.12.20.25342714v1.full.pdf?utm_source=xrayinterpreter.com
2. 6 Bold Predictions for RSNA 2025 — What Radiologists Will Actually Be Talking About in Chicago. Accessed February 7, 2026. https://www.radai.com/blogs/6-bold-predictions-for-rsna-2025-what-radiologists-will-actually-be-talking-about-in-chicago
3. Radiology AI in 2026: Governance, Workflow, Quality (Vesta Teleradiology). Accessed February 7, 2026. https://vestarad.com/radiology-ai-in-2026-from-cool-tools-to-governance-workflow-quality/
4. RSNA Trends Set to Redefine Radiology in 2026 (Rad AI). Accessed February 7, 2026. https://www.radai.com/blogs/5-rsna-trends-set-to-redefine-radiology-in-2026
5. Reducing Radiologist Workload with Smart PACS Systems (RamSoft). Accessed February 7, 2026. https://www.ramsoft.com/blog/smart-pacs-systems-reduce-radiologist-workload
6. Medical Image Routing & Prefetching (Candelis). Accessed February 7, 2026. https://www.candelis.com/solutions/image-routing-and-prefetching
7. Using Automated DICOM Prefetch for Legacy Applications with UltraPREFETCH. Accessed February 7, 2026. https://www.ultraradcorp.com/post/using-automated-dicom-prefetch-for-legacy-applications-with-ultraprefetch
8. Prefetch Relevant Priors (Dicom Systems). Accessed February 7, 2026. https://dcmsys.com/solutions/relevant-priors/
9. deepcOS - The Radiology AI Operating System. Accessed February 7, 2026. https://www.deepc.ai/
10. Top 2026 Radiology Trends, RT Students Dip, and Imaging Affordability (The Imaging Wire). Accessed February 7, 2026. https://theimagingwire.com/newsletter/top-2026-radiology-trends/
11. How to Build Medical Imaging Workflows Using Cloud and AI. Accessed February 7, 2026. https://www.spiralcompute.co.nz/how-to-build-medical-imaging-workflows-using-cloud-and-ai/
12. open-dicom/awesome-dicom. Accessed February 7, 2026. https://github.com/open-dicom/awesome-dicom
13. Workflow orchestration to streamline radiology (Merge by Merative). Accessed February 7, 2026. https://www.merative.com/merge-imaging/workflow-orchestration
14. Radiology Workflow | AI-Powered Solutions 2026 (RAD365). Accessed February 7, 2026. https://www.rad365.com/radiology-workflow
15. Workflow orchestration streamlines imaging workflows in radiology and beyond (Merative). Accessed February 7, 2026. https://www.merative.com/blog/workflow-orchestration-streamlines-imaging-workflows-in-radiology-and-beyond
16. AI in Radiology: 2025 Trends, FDA Approvals & Adoption (IntuitionLabs). Accessed February 7, 2026. https://intuitionlabs.ai/articles/ai-radiology-trends-2025
17. Selected Abstracts from CAIMI 2025 (SIIM). Accessed February 7, 2026. https://pmc.ncbi.nlm.nih.gov/articles/PMC12705894/
18. AI-Based Analysis of CT Scans Taken for Many Reasons May Also Reveal Weakened Bones (NYU Langone). Accessed February 7, 2026. https://nyulangone.org/news/ai-based-analysis-ct-scans-taken-many-reasons-may-also-reveal-weakened-bones
19. US Healthcare AI Market in 2025: Growth Drivers & Clinical Impact. Accessed February 7, 2026. https://www.corelinesoft.com/en/blog/Insight/us-healthcare-ai-market-2025
20. Fully automatic liver attenuation estimation combining CNN segmentation and morphological operations. Accessed February 7, 2026. https://pmc.ncbi.nlm.nih.gov/articles/PMC6692233/
21. Automated Volumetric Assessment of Hounsfield Units Using a Deep-Reasoning and Learning Model: Correlations with DXA Metrics (ResearchGate). Accessed February 7, 2026. https://www.researchgate.net/publication/392850629_Automated_Volumetric_Assessment_of_Hounsfield_Units_Using_a_Deep-Reasoning_and_Learning_Model_Correlations_with_DXA_Metrics
22. Top 2026 Radiology Trends (The Imaging Wire). Accessed February 7, 2026. https://theimagingwire.com/2026/01/07/the-top-trends-shaping-radiology-in-2026/
23. Quantification of Liver Fat Content with CT and MRI: State of the Art. Accessed February 7, 2026. https://pmc.ncbi.nlm.nih.gov/articles/PMC8574059/
24. Clinical feasibility of fully automated 3D liver segmentation in LDCT for hepatic steatosis. Accessed February 7, 2026. https://snu.elsevierpure.com/en/publications/clinical-feasibility-of-fully-automated-three-dimensional-liver-s/
25. Automated Volumetric Assessment of Hounsfield Units Using a Deep-Reasoning and Learning Model: Correlations with DXA Metrics (MDPI). Accessed February 7, 2026. https://www.mdpi.com/2077-0383/14/12/4373
26. wasserth/TotalSegmentator. Accessed February 7, 2026. https://github.com/wasserth/TotalSegmentator
27. TotalSegmentator paper (ResearchGate). Accessed February 7, 2026. https://www.researchgate.net/publication/362643652_TotalSegmentator_robust_segmentation_of_104_anatomical_structures_in_CT_images
28. Chatbots and Large Language Models in Radiology: A Practical Primer. Accessed February 7, 2026. https://pubs.rsna.org/doi/abs/10.1148/radiol.232756
29. Clinical Applications, Challenges & Pitfalls (PMC). Accessed February 7, 2026. https://pmc.ncbi.nlm.nih.gov/articles/PMC12531660/
30. AI Radiology Dictation Software (BlabbyAI). Accessed February 7, 2026. https://www.blabby.ai/radiology-dictation-software
31. DAX Copilot Alternative | AI Medical Scribe for Clinicians (RevMaxx). Accessed February 7, 2026. https://www.revmaxx.co/dax-copilot/
32. Top 50 Voice Dictation & Recognition Tools for Clinicians (2025). Accessed February 7, 2026. https://acmso.org/medical-scribing/top-50-voice-recognition-amp-dictation-software-for-clinicians-and-scribes-2025-buyers-guide
33. 9 Best AI Scribes for Clinicians [2026]. Accessed February 7, 2026. https://www.getfreed.ai/resources/best-ai-scribes
34. Delivering Regulatory-Grade, Automated, Multimodal Medical Data De-Identification (John Snow Labs). Accessed February 7, 2026. https://www.johnsnowlabs.com/delivering-regulatory-grade-automated-multimodal-medical-data-de-identification/
35. The De-Identification Pipeline No One Shows You — Processing PHI Through LLMs. Accessed February 7, 2026. https://pub.towardsai.net/the-builders-notes-the-de-identification-pipeline-no-one-shows-you-processing-phi-through-llms-23c803f14b08
36. Research on De-identification Applications of LLMs in Medical Records. Accessed February 7, 2026. http://ijns.jalaxy.com.tw/contents/ijns-v27-n1/ijns-2025-v27-n1-p213-222.pdf
37. PixelGuard: AI-driven de-identification for medical imaging research (AWS). Accessed February 7, 2026. https://aws.amazon.com/blogs/publicsector/pixelguard-advancing-healthcare-data-privacy-through-ai-driven-de-identification-system-for-medical-imaging-research/
38. JohnSnowLabs/dicom-deid-dataset. Accessed February 7, 2026. https://github.com/JohnSnowLabs/dicom-deid-dataset
39. A Two-Stage De-Identification Process for Privacy-Preserving Medical Image Analysis. Accessed February 7, 2026. https://www.mdpi.com/2227-9032/10/5/755
40. TIO-IKIM/medical_image_deidentification. Accessed February 7, 2026. https://github.com/TIO-IKIM/medical_image_deidentification
41. Radloop | AI Tools for Radiology Follow-Up and MIPS Reporting. Accessed February 7, 2026. https://radloop.net/
42. Using Automation and AI for Better Patient Follow-Up: What Really Works? Accessed February 7, 2026. https://inflohealth.com/blog/using-automation-and-ai-for-better-patient-follow-up-what-really-works/
43. NLP Model for Identifying Critical Findings — Multi-Institutional Study. Accessed February 7, 2026. https://pmc.ncbi.nlm.nih.gov/articles/PMC9984612/
44. Automated Tracking of Follow-Up Imaging Recommendations (AJR). Accessed February 7, 2026. https://ajronline.org/doi/10.2214/AJR.18.20586
45. Patient Follow-Up Solution, RADNAV. Accessed February 7, 2026. https://imagineteam.com/landing/radnav/
46. CONNECT: Patient Navigation Software (Nursenav). Accessed February 7, 2026. https://nursenav.com/
47. Agentic AI Revolutionizes Radiology Workflow (Oatmeal Health). Accessed February 7, 2026. https://oatmealhealth.com/agentic-ai-revolutionizes-radiology-workflow/
48. Dynamic Digital Twins for Adaptive Oncology (Frontiers). Accessed February 7, 2026. https://www.frontiersin.org/journals/artificial-intelligence/articles/10.3389/frai.2025.1624877/full
49. AI Digital Twins and Synthetic Data: Application to Clinical Trials (MRCT Center). Accessed February 7, 2026. https://mrctcenter.org/resource/ai-digital-twins-and-synthetic-data-application-to-clinical-trials/
50. AI for CT Image Quality and Radiation Protection: Systematic Review (JMIR). Accessed February 7, 2026. https://www.jmir.org/2025/1/e66622/
51. AI-Driven Advances in Low-Dose Imaging and Enhancement (MDPI). Accessed February 7, 2026. https://www.mdpi.com/2075-4418/15/6/689
