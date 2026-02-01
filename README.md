# Heimdallr

**Radiology AI Pipeline for Patient Safety & Clinical Decision Support**

Heimdallr is an automated medical imaging analysis platform that extracts clinically relevant metrics from CT and MR scans. Named after the all-seeing Norse guardian, Heimdallr watches over every scan to provide radiologists with actionable insights that enhance patient safety and diagnostic confidence.

---

## Vision

The goal of Heimdallr is to become a comprehensive **AI co-pilot for radiologists**, automatically analyzing every incoming exam to:

- **Detect critical findings** (hemorrhage, masses, incidental findings)
- **Quantify organ health** (volumes, densities, anatomical variations)
- **Assess patient frailty** (sarcopenia, body composition)
- **Flag contrast phase issues** (ensuring proper exam quality)
- **Identify body regions covered** (head, thorax, abdomen, pelvis, extremities)

Every metric calculated serves one purpose: **improve patient outcomes through earlier detection and better data**.

---

## Current Capabilities

### Supported Modalities
- **CT** (Computed Tomography) — Full feature set
- **MR** (Magnetic Resonance) — Segmentation and volumetry

### Analysis Modules

| Module | Description | Output |
|--------|-------------|--------|
| **Organ Segmentation** | Automated segmentation of 100+ anatomical structures using TotalSegmentator | NIfTI masks in `total/` |
| **Tissue Composition** | Skeletal muscle, subcutaneous/visceral fat segmentation | NIfTI masks in `tissue_types/` |
| **Cerebral Hemorrhage Detection** | Automatic detection and quantification of intracranial bleeding | Volume + overlay images |
| **Sarcopenia Analysis (L3)** | Skeletal Muscle Area (SMA) and density at L3 vertebra level | SMA (cm²), muscle HU |
| **Organ Volumetry** | Liver, spleen, kidney volumes | Volume (cm³) |
| **Organ Density** | Mean and standard deviation of Hounsfield Units (CT only) | HU mean ± std |
| **Body Region Detection** | Automatic identification of scanned body regions | List of regions |
| **Contrast Phase Detection** | Classification of CT phase (native, arterial, venous, delayed) | Phase + probability |

---

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   uploader.py   │───▶│   ingest.py     │───▶│   prepare.py    │
│  (CLI Client)   │    │  (FastAPI)      │    │ (DICOM→NIfTI)   │
└─────────────────┘    └─────────────────┘    └────────┬────────┘
                                                       │
                                                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    output/      │◀───│    run.py       │◀───│    input/       │
│   (Results)     │    │  (Processing)   │    │  (Queue)        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Components

1. **Uploader Client** (`uploader.py`)
   - CLI tool for sending exams to the pipeline
   - Supports ZIP files and folders (auto-zipped)
   - Progress bar with transfer speed

2. **Ingestion Service** (`ingest.py`)
   - FastAPI server receiving uploads on port 8001
   - Triggers preparation asynchronously

3. **Preparation Module** (`prepare.py`)
   - Extracts and scans DICOM files
   - Intelligent series selection (CT: phase + kernel priority, MR: slice count)
   - Converts to NIfTI using `dcm2niix`
   - Generates clinical naming: `FirstNameInitials_YYYYMMDD_AccessionNumber`
   - Stores metadata in SQLite database

4. **Processing Daemon** (`run.py`)
   - Monitors `input/` for new cases
   - Runs TotalSegmentator for segmentation
   - Executes conditional analyses (e.g., hemorrhage if brain detected)
   - Calculates all metrics via `metrics.py`
   - Parallel processing (up to 3 cases simultaneously)

5. **Metrics Module** (`metrics.py`)
   - Organ volume calculation
   - HU density analysis
   - L3 sarcopenia metrics
   - Hemorrhage quantification
   - Overlay image generation

---

## Output Structure

```
output/
└── PatientInitials_YYYYMMDD_AccessionNumber/
    ├── id.json                    # Patient & study metadata
    ├── resultados.json            # All calculated metrics
    ├── L3_overlay.png             # Sarcopenia visualization (if L3 found)
    ├── bleed_overlay_*.png        # Hemorrhage visualizations (if detected)
    ├── total/                     # Organ segmentation masks (.nii.gz)
    ├── tissue_types/              # Tissue segmentation masks (.nii.gz)
    └── bleed/                     # Hemorrhage segmentation (if detected)
```

### Metrics JSON (`resultados.json`)

```json
{
  "case_id": "PatientInitials_YYYYMMDD_AccessionNumber",
  "modality": "CT",
  "body_regions": ["head", "thorax", "abdomen"],
  
  "liver_vol_cm3": 1523.45,
  "liver_hu_mean": 58.2,
  "liver_hu_std": 12.1,
  
  "spleen_vol_cm3": 189.32,
  "spleen_hu_mean": 52.8,
  "spleen_hu_std": 8.4,
  
  "kidney_right_vol_cm3": 156.78,
  "kidney_right_hu_mean": 35.2,
  "kidney_right_hu_std": 10.5,
  
  "kidney_left_vol_cm3": 162.34,
  "kidney_left_hu_mean": 34.8,
  "kidney_left_hu_std": 11.2,
  
  "slice_L3": 127,
  "SMA_cm2": 142.56,
  "muscle_HU_mean": 38.4,
  "muscle_HU_std": 15.2,
  
  "hemorrhage_vol_cm3": 45.23,
  "hemorrhage_analysis_slices": {
    "inferior_15": 83,
    "center_50": 124,
    "superior_85": 165
  }
}
```

### Patient Metadata (`id.json`)

```json
{
  "PatientName": "FULL PATIENT NAME",
  "AccessionNumber": "123456",
  "StudyInstanceUID": "1.2.3...",
  "Modality": "CT",
  "StudyDate": "20260201",
  "CaseID": "PatientInitials_20260201_123456",
  "ClinicalName": "PatientInitials_20260201_123456",
  "Pipeline": {
    "start_time": "2026-02-01T10:00:00",
    "end_time": "2026-02-01T10:02:30",
    "elapsed_time": "0:02:30"
  },
  "SelectedSeries": {
    "SeriesNumber": "4",
    "ContrastPhaseData": {
      "phase": "native",
      "probability": 1.0
    }
  }
}
```

---

## Setup

### Prerequisites

- Python 3.10+
- NVIDIA GPU with CUDA (recommended for TotalSegmentator)
- `dcm2niix` installed (`sudo apt install dcm2niix`)

### Installation

```bash
# Clone repository
git clone <repository-url>
cd Heimdallr

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

## Usage

### Starting the Pipeline

Run these in **separate terminals**:

```bash
# Terminal 1: Ingestion Service (receives uploads)
source venv/bin/activate
python ingest.py
# Running on http://0.0.0.0:8001

# Terminal 2: Processing Daemon (processes queue)
source venv/bin/activate
python run.py
# Monitoring input/ folder...
```

### Uploading Exams

```bash
source venv/bin/activate

# Upload a ZIP file
python uploader.py /path/to/exam.zip

# Upload a folder (auto-zipped)
python uploader.py /path/to/dicom_folder/

# Specify custom server
python uploader.py /path/to/exam.zip --server http://192.168.1.100:8001/upload
```

---

## Roadmap

Future enhancements planned for Heimdallr:

- [ ] **Lung nodule detection** — Automated CAD for pulmonary nodules
- [ ] **Coronary calcium scoring** — Agatston score calculation
- [ ] **Liver steatosis quantification** — Fat fraction estimation
- [ ] **Aortic measurements** — Diameter and aneurysm detection
- [ ] **Bone density analysis** — Opportunistic osteoporosis screening
- [ ] **Incidental findings detection** — AI-powered anomaly detection
- [ ] **PACS integration** — Direct DICOM receive/send
- [ ] **Web dashboard** — Real-time monitoring and results visualization
- [ ] **HL7/FHIR integration** — EMR interoperability

---

## License

Proprietary. TotalSegmentator requires a valid license for commercial use.

---

## Contributing

Contributions are welcome. Please open an issue to discuss proposed changes before submitting a pull request.
