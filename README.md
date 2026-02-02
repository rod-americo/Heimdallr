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
┌─────────────────┐    ┌─────────────────┐     ┌─────────────────┐
│ Modality/PACS   │───▶│dicom_listener.py│────▶│   server.py     │
│ (C-STORE)       │    │  (Port 11112)   │     │ (FastAPI Server)│
└─────────────────┘    │  Auto-upload    │     │  Port 8001      │
                       └─────────────────┘     └────────┬────────┘
                                                        │
┌─────────────────┐                                     │
│   uploader.py   │────────────────────────────────────▶│
│  (CLI Client)   │    HTTP POST /upload                │
└─────────────────┘                                     │
                                                        ▼
                                               ┌─────────────────┐
                                               │   prepare.py    │
                                               │ (DICOM→NIfTI)   │
                                               └────────┬────────┘
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │    input/       │
                                               │  (Queue)        │
                                               └────────┬────────┘
                                                        │
                                                        ▼
┌─────────────────┐                            ┌─────────────────┐
│  Web Dashboard  │◀───────────────────────────│    run.py       │
│  (Browser UI)   │    API Endpoints           │  (Processing)   │
└─────────────────┘                            └────────┬────────┘
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │    output/      │
                                               │   (Results)     │
                                               └─────────────────┘
```

### Components

#### Ingestion Methods

**Option 1: DICOM Listener** (`dicom_listener.py`) — **Recommended for PACS Integration**
- DICOM C-STORE SCP (Service Class Provider) on port 11112
- Receives images directly from modalities/PACS
- Groups images by StudyInstanceUID
- Auto-detects study completion (30s idle timeout)
- Automatically zips and uploads to server
- Production-ready with retry logic and error handling

**Option 2: CLI Uploader** (`uploader.py`) — **Manual Upload**
- Command-line tool for manual exam submission
- Supports ZIP files and folders (auto-zipped)
- Progress bar with transfer speed
- Useful for batch processing or testing

#### Core Pipeline

1. **Unified Server** (`server.py`)
   - FastAPI server on port 8001
   - **Upload API**: Receives DICOM ZIP files and triggers preparation
   - **Dashboard API**: RESTful endpoints for patient data, results, and downloads
   - **Web UI**: Serves interactive dashboard from `static/` directory
   - **File Downloads**: NIfTI files, segmentation folders (ZIP), overlay images

2. **Preparation Module** (`prepare.py`)
   - Extracts and scans DICOM files
   - Intelligent series selection (CT: phase + kernel priority, MR: slice count)
   - Converts to NIfTI using `dcm2niix`
   - Generates clinical naming: `FirstNameInitials_YYYYMMDD_AccessionNumber`
   - Stores metadata in SQLite database

3. **Processing Daemon** (`run.py`)
   - Monitors `input/` for new cases
   - Runs TotalSegmentator for segmentation
   - Executes conditional analyses (e.g., hemorrhage if brain detected)
   - Calculates all metrics via `metrics.py`
   - Parallel processing (up to 3 cases simultaneously)

4. **Metrics Module** (`metrics.py`)
   - Organ volume calculation
   - HU density analysis
   - L3 sarcopenia metrics
   - Hemorrhage quantification
   - Overlay image generation

6. **Web Dashboard** (`static/`)
   - **Real-time patient list** with auto-refresh (30s intervals)
   - **Quick search** for filtering patients by name
   - **Interactive results modal** displaying metrics and overlay images
   - **Download capabilities**: NIfTI files, segmentation folders
   - **Responsive design** with modern UI/UX

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
# Terminal 1: Unified Server (web dashboard + upload API)
source venv/bin/activate
python server.py
# Running on http://0.0.0.0:8001
# Dashboard: http://localhost:8001
# API Docs: http://localhost:8001/docs

# Terminal 2: Processing Daemon (processes queue)
source venv/bin/activate
python run.py
# Monitoring input/ folder...
```

### Accessing the Dashboard

Open your browser and navigate to:
- **Dashboard**: `http://localhost:8001`
- **API Documentation**: `http://localhost:8001/docs`

The dashboard provides:
- Real-time patient list with auto-refresh
- Quick search functionality
- Detailed results viewer with overlay images
- Direct download of NIfTI files and segmentation masks

### Uploading Exams

#### Option 1: DICOM Listener (PACS Integration)

For production environments with PACS/modalities:

```bash
# Terminal 3: DICOM Listener (receives from PACS)
source venv/bin/activate
python dicom_listener.py
# Heimdallr DICOM Listener started
#   AE Title: HEIMDALLR
#   Port: 11112
#   Upload URL: http://127.0.0.1:8001/upload
#   Idle timeout: 30s
# Waiting for DICOM connections...
```

**Configuration** (via `config.py` or environment variables):
```bash
# Override defaults
export HEIMDALLR_AE_TITLE="MY_AE_TITLE"
export HEIMDALLR_DICOM_PORT="11113"
export HEIMDALLR_IDLE_SECONDS="60"

python dicom_listener.py
```

**PACS Configuration:**
- Configure your PACS to send studies to Heimdallr
- Destination AE Title: `HEIMDALLR`
- Destination IP: Heimdallr server IP
- Destination Port: `11112`
- Protocol: DICOM C-STORE

**How it works:**
1. Receives DICOM images via C-STORE protocol
2. Groups images by StudyInstanceUID
3. Waits 30 seconds after last image (idle timeout)
4. Automatically zips completed study
5. Uploads to server via HTTP POST
6. Archives successful uploads, logs failures

**Testing with DCMTK:**
```bash
# Send single file
dcmsend localhost 11112 -aec HEIMDALLR test.dcm

# Send entire directory
dcmsend localhost 11112 -aec HEIMDALLR +sd /path/to/dicom/folder
```

#### Option 2: Manual Upload (CLI)

For manual uploads or batch processing:

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

## Production Deployment

### Systemd Services

For production environments, run services as systemd units:

**DICOM Listener Service** (`/etc/systemd/system/heimdallr-dicom.service`):
```ini
[Unit]
Description=Heimdallr DICOM Listener
After=network.target

[Service]
Type=simple
User=heimdallr
WorkingDirectory=/opt/heimdallr
ExecStart=/opt/heimdallr/venv/bin/python dicom_listener.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Server Service** (`/etc/systemd/system/heimdallr-server.service`):
```ini
[Unit]
Description=Heimdallr FastAPI Server
After=network.target

[Service]
Type=simple
User=heimdallr
WorkingDirectory=/opt/heimdallr
ExecStart=/opt/heimdallr/venv/bin/python server.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Processing Daemon** (`/etc/systemd/system/heimdallr-processor.service`):
```ini
[Unit]
Description=Heimdallr Processing Daemon
After=network.target

[Service]
Type=simple
User=heimdallr
WorkingDirectory=/opt/heimdallr
ExecStart=/opt/heimdallr/venv/bin/python run.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Enable and start services:**
```bash
sudo systemctl enable heimdallr-dicom heimdallr-server heimdallr-processor
sudo systemctl start heimdallr-dicom heimdallr-server heimdallr-processor
sudo systemctl status heimdallr-*
```

### Configuration Management

All configuration is centralized in `config.py` and can be overridden via environment variables:

```bash
# View current configuration
python config.py

# Override via environment (add to systemd service files)
Environment="HEIMDALLR_DICOM_PORT=11113"
Environment="HEIMDALLR_MAX_PARALLEL_CASES=5"
Environment="HEIMDALLR_UPLOAD_URL=http://production-server:8001/upload"
```

### Troubleshooting

**DICOM Listener Issues:**
```bash
# Check if port is in use
sudo lsof -i :11112

# Kill existing process
sudo fuser -k 11112/tcp

# Check listener logs
journalctl -u heimdallr-dicom -f

# Test DICOM connectivity
dcmsend localhost 11112 -aec HEIMDALLR test.dcm
```

**Upload Failures:**
- Check `data/failed/` directory for failed ZIPs
- Verify server.py is running: `systemctl status heimdallr-server`
- Check network connectivity between listener and server
- Review logs: `journalctl -u heimdallr-dicom -n 100`

**Processing Issues:**
- Check GPU availability: `nvidia-smi`
- Monitor processing: `journalctl -u heimdallr-processor -f`
- Check input queue: `ls -lh input/`
- Review error directory: `ls -lh errors/`

---

## Roadmap

Future enhancements planned for Heimdallr:

- [ ] **Chest X-ray PA analysis & automated preliminary reporting** — AI-powered analysis and pre-reports for radiologist review
- [ ] **Lung nodule detection** — Automated CAD for pulmonary nodules
- [ ] **Coronary calcium scoring** — Agatston score calculation
- [ ] **Liver steatosis quantification** — Fat fraction estimation
- [ ] **Aortic measurements** — Diameter and aneurysm detection
- [ ] **Bone density analysis** — Opportunistic osteoporosis screening
- [ ] **Incidental findings detection** — AI-powered anomaly detection
- [x] **PACS integration** — Direct DICOM receive/send ✅
- [x] **Web dashboard** — Real-time monitoring and results visualization ✅
- [ ] **HL7/FHIR integration** — EMR interoperability

---

## License

Heimdallr is licensed under the **Apache License, Version 2.0**.

You may obtain a copy of the License at:
- [LICENSE](LICENSE) file in this repository
- http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

### Third-Party Dependencies

This software uses **TotalSegmentator**, which requires a valid license for commercial use. Users must ensure compliance with TotalSegmentator's licensing requirements independently.

For complete third-party notices and attributions, see the [NOTICE](NOTICE) file.

---

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

By contributing to Heimdallr, you agree that your contributions will be licensed under the Apache License 2.0.
