# Sarcopenia Analysis Pipeline

Automated pipeline for L3 sarcopenia assessment from CT scans.

## Architecture

1.  **Ingestion Service** (`ingest.py`): FastAPI service that accepts ZIP uploads containing DICOM files.
2.  **Preparation** (`prepare.py`): Extracts DICOMs, selects the best series, and converts to NIfTI (`input/` folder).
3.  **Analysis** (`run.py`): Batch processes NIfTI files from `input/`, generates segmentations + metrics in `output/`, and archives inputs to `nii/`.

## Setup

1.  Create and activate environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt # (ensure you have dependencies installed)
    ```

2.  External Tools:
    - Ensure `dcm2niix` is installed (`sudo apt install dcm2niix` or download binary).

## Usage

### 1. Start Ingestion Service
```bash
./venv/bin/uvicorn ingest:app --host 0.0.0.0 --port 8000
```

### 2. Upload Data
Send a ZIP file containing DICOMs:
```bash
curl -X 'POST' \
  'http://localhost:8000/upload' \
  -H 'accept: application/json' \
  -H 'Content-Type: multipart/form-data' \
  -F 'file=@/path/to/exam.zip'
```
The file will be processed and placed in `input/` folder as `PatientName_AccessionNumber.nii.gz`.

### 3. Run Analysis
Process all pending files in `input/`:
```bash
./venv/bin/python run.py
```
Results will be in `output/{PatientName_AccessionNumber}/resultados.json`.
Processed inputs are moved to `nii/`.

## Output Structure

- `output/`
    - `PatientName_AccessionNumber/`
        - `resultados.json`: Sarcopenia metrics.
        - `total/`: Organ segmentations.
        - `tissue_types/`: Muscle/Fat segmentations.
