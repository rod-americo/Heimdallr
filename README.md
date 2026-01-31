# Sarcopenia Analysis Pipeline

Automated pipeline for L3 sarcopenia assessment and organ volumetry from CT scans.

## Architecture

1.  **Ingestion Service** (`ingest.py`): FastAPI service that accepts ZIP uploads containing DICOM files. It prepares the data and places it in `input/` queue.
2.  **Execution Daemon** (`run.py`): Continuous monitoring service. Watch `input/` for new files, processes them (TotalSegmentator + Metrics), and moves result to `output/`.
3.  **Metrics Module** (`metrics.py`): Encapsulates logic for Muscle Area, HU, and Organ Volumes.
4.  **Uploader Client** (`uploader.py`): CLI tool for easy uploading of folders/files.

## Setup

1.  Create and activate environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

2.  External Tools:
    - Ensure `dcm2niix` is installed (`sudo apt install dcm2niix` or download binary).

## Usage

You must run **two separate terminals** to keep the pipeline active.

### Terminal 1: Ingestion Service
Starts the web server to receive files.
```bash
source venv/bin/activate
python ingest.py
```
*Running on http://0.0.0.0:8001*

### Terminal 2: Execution Daemon
Starts the processor that monitors the `input/` folder.
```bash
source venv/bin/activate
python run.py
```

### Terminal 3: Upload Client
Send data to the pipeline.
```bash
source venv/bin/activate
# Upload a single zip
python uploader.py path/to/exam.zip

# Upload a folder (auto-zips)
python uploader.py path/to/dicom_folder/
```

## Output Structure

- `output/`
    - `PatientName_AccessionNumber/`
        - `resultados.json`: Sarcopenia metrics + Organ Volumes.
        - `total/`: Organ segmentations (nii.gz).
        - `tissue_types/`: Muscle/Fat segmentations (nii.gz).

### Metrics in JSON
- `SMA_cm2`: Skeletal Muscle Area at L3.
- `muscle_HU_mean`: Mean attenuation of muscle.
- `liver_vol_cm3`: Total liver volume.
- `liver_HU_mean`: Mean attenuation of liver.
- `spleen_vol_cm3`: Splen volume.
- `kidney_right_vol_cm3`, `kidney_left_vol_cm3`: Kidney volumes.
