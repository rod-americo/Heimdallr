# Operations Runbook

This document provides a baseline for operating Heimdallr in production-like environments.

## Service Topology

Run as independent services:

1. `server.py` (API, dashboard, upload endpoints)
2. `run.py` (processing worker)
3. `dicom_listener.py` (DICOM C-STORE intake)

## Baseline Startup

```bash
# API + Dashboard
source venv/bin/activate
python server.py

# Processing worker
source venv/bin/activate
python run.py

# DICOM listener
source venv/bin/activate
python dicom_listener.py
```

## Environment and Config

Configuration is centralized in `config.py` and can be overridden via `HEIMDALLR_*` environment variables.

Common examples:

```bash
export HEIMDALLR_AE_TITLE="HEIMDALLR"
export HEIMDALLR_DICOM_PORT="11112"
export HEIMDALLR_IDLE_SECONDS="30"
```

## PACS Connectivity Checks

Expected defaults:

- AE Title: `HEIMDALLR`
- Port: `11112`
- Protocol: DICOM C-STORE

Quick smoke test using DCMTK:

```bash
dcmsend localhost 11112 -aec HEIMDALLR test.dcm
```

## Health and Monitoring Checks

1. `http://localhost:8001/docs` responds.
2. Listener accepts inbound C-STORE on port `11112`.
3. Queue path `upload -> input/ -> output/` completes for a known study.
4. GPU capacity is available for segmentation processing.

## Incident Triage Shortlist

1. Validate service process state and restart order (`server -> run -> listener`).
2. Check PACS destination configuration and network reachability.
3. Inspect `input/`, `output/`, and `errors/` for stuck or failed cases.
4. Verify model/API credentials and quota for report-assist flows.
5. Confirm data storage permissions for intake and output paths.

## Safety Reminder

Heimdallr is clinical decision support infrastructure. Automated outputs must be reviewed by qualified professionals before clinical action.
