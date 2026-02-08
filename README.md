# Heimdallr

**Radiology Preprocessing Ecosystem for Operational Automation, Clinical Triage, and AI-Assisted Reporting**

Heimdallr is a production-oriented platform that turns raw imaging intake into structured, actionable clinical intelligence. It connects DICOM ingestion, preprocessing, AI analysis, and report-assist workflows so radiologists can spend less time on logistics and more time on decision-making.

Named after the all-seeing Norse guardian, Heimdallr watches over every scan and every report, standing at the threshold between raw data and clinical action with relentless vigilance.

The project follows a systems approach: instead of a single detection model, Heimdallr is designed as an end-to-end radiology pipeline where each stage contributes to safety, throughput, and continuity of care.

## Core Scope

Heimdallr currently focuses on three practical layers:

1. **Operational intake and routing**
   - DICOM listener for PACS/modality integration
   - Study grouping, idle detection, and automated upload
   - Queue-based processing for reliable throughput

2. **Imaging preprocessing and quantitative analytics**
   - DICOM to NIfTI conversion and series selection
   - Segmentation-driven metrics (volume, density, sarcopenia, hemorrhage)
   - Structured outputs for downstream clinical and data workflows

3. **Radiologist assistance interfaces**
   - FastAPI endpoints and interactive web dashboard
   - Chest X-ray assistance flow (Anthropic + MedGemma with structured prompting)
   - Downloadable artifacts for auditability and review

Future-facing capabilities (workflow orchestration, clinical urgency triage, de-identification pipelines, patient navigation, and agentic AI) are tracked in [`UPCOMING.md`](UPCOMING.md).

## What Is Implemented Today

### Ingestion and Preparation
- `dicom_listener.py`: DICOM C-STORE SCP (`HEIMDALLR`, port `11112`)
- `uploader.py`: manual/CLI uploads (ZIP or folder)
- `server.py`: upload API, dashboard API, static web serving
- `prepare.py`: DICOM parsing, series selection, NIfTI conversion via `dcm2niix`

### Processing and Metrics
- `run.py`: queue worker with parallel case processing
- `metrics.py`: quantitative extraction and derived metrics
- Segmentation and tissue maps through TotalSegmentator pipeline integration

### Reporting Assistance
- `server.py`: report-assist endpoints, including Anthropic chest X-ray flow (`/api/anthropic/ap-thorax-xray`)
- `medgemma_api.py`: dedicated microservice for AP chest X-ray assistant flow
- `medgemma_prompts.py`: prompt templates and structured output helpers
- `anthropic_report_builder.py`: report-building utilities for narrative output

### Data Layer and Outputs
- SQLite schema in `database/schema.sql`
- Per-case output directory with:
  - `id.json` (study/case metadata)
  - `resultados.json` (quantitative outputs)
  - overlays and segmentation artifacts (`.png`, `.nii.gz`)

## Architecture

```text
PACS / Modality (DICOM C-STORE)
            |
            v
   dicom_listener.py  --->  HTTP /upload  --->  server.py
                                              |
                                              v
                                        prepare.py
                                  (DICOM selection + NIfTI)
                                              |
                                              v
                                           input/
                                              |
                                              v
                                            run.py
                                   (segmentation + metrics)
                                              |
                                              v
                                           output/
                                              |
                                              +--> Dashboard/API (server.py)
                                              +--> Report assist endpoints

medgemma_api.py runs as an isolated service,
while Anthropic-backed report flows are orchestrated by server endpoints.
```

## Quick Start

### Prerequisites
- Python `3.10+`
- `dcm2niix`
- NVIDIA GPU (recommended for segmentation workloads)

### Install

```bash
git clone <repository-url>
cd Heimdallr
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Run (separate terminals)

```bash
# 1) API + Dashboard
source venv/bin/activate
python server.py

# 2) Processing worker
source venv/bin/activate
python run.py

# 3) Optional: DICOM listener for PACS integration
source venv/bin/activate
python dicom_listener.py
```

### Access
- Dashboard: `http://localhost:8001`
- API docs: `http://localhost:8001/docs`

## Operational Notes

- Configuration is centralized in `config.py` and can be overridden with `HEIMDALLR_*` environment variables.
- For deployment, run `server.py`, `run.py`, and `dicom_listener.py` as independent services (systemd examples were previously used and can be reintroduced as infrastructure docs if needed).
- This repository contains active experimentation and production-facing utilities; validate feature toggles before rolling out in clinical environments.

## Licensing and Third-Party Compliance

- Heimdallr is distributed under Apache License 2.0.
- This project uses **TotalSegmentator**. Commercial usage may require a separate TotalSegmentator license.
- Each institution and deployer is responsible for validating third-party licensing and regulatory compliance before production use.
- For attributions and notices, see [`NOTICE`](NOTICE).

## Production Baseline

Minimum production topology:

1. `server.py` (API + dashboard)
2. `run.py` (processing worker)
3. `dicom_listener.py` (DICOM intake)

Recommended baseline checks:

1. `http://localhost:8001/docs` responds.
2. Listener port `11112` is reachable from PACS.
3. Queue flow `upload -> input/ -> output/` completes for a known study.
4. GPU capacity is validated for segmentation workloads.

For deployment units, observability, and incident handling runbooks, see [`docs/OPERATIONS.md`](docs/OPERATIONS.md).

## PACS Integration Quick Check

Expected defaults:

- AE Title: `HEIMDALLR`
- Port: `11112`
- Protocol: DICOM C-STORE

Connectivity smoke test with DCMTK:

```bash
dcmsend localhost 11112 -aec HEIMDALLR test.dcm
```

If tests fail, verify listener process state, firewall rules, and PACS destination configuration.

## API Quick Contracts

Anthropic chest X-ray flow:

```bash
curl -X POST http://localhost:8001/api/anthropic/ap-thorax-xray \
  -F "file=@/path/to/image.dcm" \
  -F "age=45 year old" \
  -F "identificador=case_123"
```

MedGemma chest X-ray flow:

```bash
curl -X POST http://localhost:8001/api/medgemma/ap-thorax-xray \
  -F "file=@/path/to/image.png" \
  -F "age=45 year old"
```

For endpoint coverage and payload conventions, see [`docs/API.md`](docs/API.md).

## Documentation Map

- Strategic roadmap and future architecture: [`UPCOMING.md`](UPCOMING.md)
- Operations and deployment runbook: [`docs/OPERATIONS.md`](docs/OPERATIONS.md)
- API contracts and examples: [`docs/API.md`](docs/API.md)
- Strategic visual board: [`static/pipeline-strategy.html`](static/pipeline-strategy.html)
- Public docs landing page (GitHub Pages): [https://rod-americo.github.io/Heimdallr/](https://rod-americo.github.io/Heimdallr/)
- Public strategy board (GitHub Pages): [https://rod-americo.github.io/Heimdallr/pipeline-strategy.html](https://rod-americo.github.io/Heimdallr/pipeline-strategy.html)

## GitHub Pages Publishing

To publish the HTML strategy board as a rendered page:

1. Open repository settings on GitHub.
2. Go to `Pages`.
3. Set source to `Deploy from a branch`.
4. Select branch `main` and folder `/docs`.
5. Save and wait for the deployment to complete.

## Strategy and Roadmap

- Strategic backlog and upcoming modules: [`UPCOMING.md`](UPCOMING.md)
- Dark-mode strategic planning page (based on your concept): [`static/pipeline-strategy.html`](static/pipeline-strategy.html)

### Upcoming Prioritization (Ordered)

1. HL7-triggered smart prefetch orchestration
2. Unified worklist orchestration and fair assignment
3. De-identification gateway (metadata + pixel PHI controls)
4. AI urgency flagging with auditable reprioritization
5. Structured LLM report drafting with style-safe templates
6. Opportunistic liver and bone quantification at scale
7. Follow-up recommendation extraction and navigation workflows
8. SLA-aware orchestration and escalation policy engine
9. Enterprise audit hardening for external model gateways
10. Agentic workflow coordinator for multi-step radiology operations

## Safety, Compliance, and Clinical Use

Heimdallr is intended as **clinical decision support** infrastructure, not an autonomous diagnostic authority. Any AI-generated text, prioritization signal, or quantitative metric must be reviewed by qualified professionals.

### Test Data Handling

- Test datasets are fully anonymized before use.
- In the image-conversion test flow, DICOM metadata is intentionally not carried forward, reducing PHI exposure risk in derived files.
- Test radiographs used in this repository do not contain burned-in PHI overlays.

For planned governance controls (de-identification, auditability hardening, model drift controls, and shadow-AI mitigation), see [`UPCOMING.md`](UPCOMING.md).

## License

Apache License 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).

### Third-Party Dependencies

This software uses **TotalSegmentator**, which may require a separate commercial license depending on usage context. Users are responsible for independent license compliance.
