# Heimdallr

**Radiology Preprocessing Ecosystem for Operational Automation, Clinical Triage, and AI-Assisted Reporting**

Heimdallr is a production-oriented platform that turns raw imaging intake into structured, actionable clinical intelligence. It connects DICOM ingestion, preprocessing, AI analysis, and report-assist workflows so radiologists can spend less time on logistics and more time on decision-making.

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
   - Chest X-ray assistance flow (MedGemma + structured prompting)
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

medgemma_api.py runs as an isolated service and is consumed by server endpoints.
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

## Strategy and Roadmap

- Strategic backlog and upcoming modules: [`UPCOMING.md`](UPCOMING.md)
- Dark-mode strategic planning page (based on your concept): [`static/pipeline-strategy.html`](static/pipeline-strategy.html)

## Safety, Compliance, and Clinical Use

Heimdallr is intended as **clinical decision support** infrastructure, not an autonomous diagnostic authority. Any AI-generated text, prioritization signal, or quantitative metric must be reviewed by qualified professionals.

For planned governance controls (de-identification, auditability hardening, model drift controls, and shadow-AI mitigation), see [`UPCOMING.md`](UPCOMING.md).

## License

Apache License 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
