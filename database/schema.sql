-- Heimdallr Database Schema
-- SQLite 3.x
-- Version: 1.0
-- Last Updated: 2026-02-01

-- ============================================================
-- Main Table: DICOM Metadata and Calculation Results
-- ============================================================

CREATE TABLE IF NOT EXISTS dicom_metadata (
    -- Primary Key
    StudyInstanceUID TEXT PRIMARY KEY,
    
    -- Patient Information
    PatientName TEXT,
    PatientID TEXT,
    PatientBirthDate TEXT,
    
    -- Clinical Naming (FirstNameInitials_YYYYMMDD_AccessionNumber)
    ClinicalName TEXT,
    
    -- Study Information
    AccessionNumber TEXT,
    StudyDate TEXT,
    Modality TEXT,
    CallingAET TEXT,
    RemoteIP TEXT,
    
    -- Metadata Storage (JSON)
    IdJson TEXT,                -- Complete id.json from output directory
    JsonDump TEXT,              -- Basic metadata from heimdallr.prepare (legacy)
    DicomMetadata TEXT,         -- Full DICOM tags from selected series
    CalculationResults TEXT,    -- Computed metrics from heimdallr.metrics
    PatientSex TEXT,
    Weight REAL,
    Height REAL,
    SMI REAL,
    SegmentationSeriesInstanceUID TEXT,
    SegmentationSliceCount INTEGER,
    SegmentationProfile TEXT,
    SegmentationTasks TEXT,
    SegmentationCompletedAt TIMESTAMP,
    
    -- Timestamps
    ProcessedAt TIMESTAMP
);

-- ============================================================
-- Segmentation Queue: Immediate Dispatch Signaling
-- ============================================================

CREATE TABLE IF NOT EXISTS segmentation_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id TEXT NOT NULL UNIQUE,
    input_path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP,
    claimed_at TIMESTAMP,
    finished_at TIMESTAMP,
    error TEXT
);

-- ============================================================
-- Metrics Queue: Post-segmentation derived measurements
-- ============================================================

CREATE TABLE IF NOT EXISTS metrics_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id TEXT NOT NULL UNIQUE,
    input_path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP,
    claimed_at TIMESTAMP,
    finished_at TIMESTAMP,
    error TEXT
);

-- ============================================================
-- DICOM Egress Queue: Outbound artifact delivery
-- ============================================================

CREATE TABLE IF NOT EXISTS dicom_egress_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id TEXT NOT NULL,
    study_uid TEXT,
    artifact_path TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    destination_name TEXT NOT NULL,
    destination_host TEXT NOT NULL,
    destination_port INTEGER NOT NULL,
    destination_called_aet TEXT NOT NULL,
    source_calling_aet TEXT,
    source_remote_ip TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP,
    claimed_at TIMESTAMP,
    finished_at TIMESTAMP,
    next_attempt_at TIMESTAMP,
    error TEXT,
    UNIQUE(case_id, artifact_path, destination_name)
);

-- ============================================================
-- Integration Dispatch Queue: outbound webhook/API event delivery
-- ============================================================

CREATE TABLE IF NOT EXISTS integration_dispatch_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL DEFAULT 1,
    event_key TEXT NOT NULL,
    case_id TEXT,
    study_uid TEXT,
    destination_name TEXT NOT NULL,
    destination_url TEXT NOT NULL,
    http_method TEXT NOT NULL DEFAULT 'POST',
    timeout_seconds INTEGER NOT NULL DEFAULT 10,
    request_headers TEXT,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP,
    claimed_at TIMESTAMP,
    finished_at TIMESTAMP,
    next_attempt_at TIMESTAMP,
    response_status INTEGER,
    error TEXT,
    UNIQUE(event_key, destination_name)
);

-- ============================================================
-- Integration Delivery Queue: outbound final package delivery
-- ============================================================

CREATE TABLE IF NOT EXISTS integration_delivery_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL DEFAULT 1,
    case_id TEXT NOT NULL,
    study_uid TEXT,
    client_case_id TEXT,
    source_system TEXT,
    callback_url TEXT NOT NULL,
    http_method TEXT NOT NULL DEFAULT 'POST',
    timeout_seconds INTEGER NOT NULL DEFAULT 120,
    requested_outputs_json TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP,
    claimed_at TIMESTAMP,
    finished_at TIMESTAMP,
    next_attempt_at TIMESTAMP,
    response_status INTEGER,
    error TEXT,
    UNIQUE(job_id, callback_url)
);

-- ============================================================
-- Resource Monitor Samples: resident memory telemetry snapshots
-- ============================================================

CREATE TABLE IF NOT EXISTS resource_monitor_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sampled_at TIMESTAMP NOT NULL,
    service_slug TEXT NOT NULL,
    service_unit TEXT NOT NULL,
    stage TEXT,
    main_pid INTEGER,
    subtree_pids_json TEXT,
    active_case_ids_json TEXT,
    rss_mb REAL,
    peak_rss_mb REAL,
    subtree_rss_mb REAL,
    subtree_peak_rss_mb REAL,
    subtree_pss_mb REAL,
    major_faults INTEGER,
    cgroup_memory_current_mb REAL,
    cgroup_memory_peak_mb REAL,
    host_mem_total_mb REAL,
    host_mem_available_mb REAL,
    host_swap_used_mb REAL,
    host_mem_used_percent REAL,
    notes_json TEXT
);

CREATE TABLE IF NOT EXISTS resource_monitor_case_peaks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    first_sampled_at TIMESTAMP NOT NULL,
    last_sampled_at TIMESTAMP NOT NULL,
    sample_count INTEGER NOT NULL DEFAULT 0,
    max_main_rss_mb REAL,
    max_peak_rss_mb REAL,
    max_subtree_pss_mb REAL,
    max_cgroup_memory_current_mb REAL,
    min_host_mem_available_mb REAL,
    max_host_swap_used_mb REAL,
    max_major_faults INTEGER,
    UNIQUE(case_id, stage)
);

-- ============================================================
-- Indexes for Performance
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_clinical_name ON dicom_metadata(ClinicalName);
CREATE INDEX IF NOT EXISTS idx_accession ON dicom_metadata(AccessionNumber);
CREATE INDEX IF NOT EXISTS idx_study_date ON dicom_metadata(StudyDate);
CREATE INDEX IF NOT EXISTS idx_modality ON dicom_metadata(Modality);
CREATE INDEX IF NOT EXISTS idx_processed_at ON dicom_metadata(ProcessedAt);
CREATE INDEX IF NOT EXISTS idx_segmentation_queue_status_created ON segmentation_queue(status, created_at);
CREATE INDEX IF NOT EXISTS idx_metrics_queue_status_created ON metrics_queue(status, created_at);
CREATE INDEX IF NOT EXISTS idx_dicom_egress_queue_status_next_attempt ON dicom_egress_queue(status, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_integration_dispatch_queue_status_next_attempt ON integration_dispatch_queue(status, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_integration_delivery_queue_status_next_attempt ON integration_delivery_queue(status, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_resource_monitor_samples_service_time ON resource_monitor_samples(service_slug, sampled_at);
CREATE INDEX IF NOT EXISTS idx_resource_monitor_samples_time ON resource_monitor_samples(sampled_at);
CREATE INDEX IF NOT EXISTS idx_resource_monitor_case_peaks_case_stage ON resource_monitor_case_peaks(case_id, stage);

-- ============================================================
-- Schema Notes
-- ============================================================

-- StudyInstanceUID: Unique DICOM identifier (1.2.840.xxx...)
-- PatientName: Full patient name from DICOM
-- PatientID: DICOM PatientID
-- PatientBirthDate: DICOM PatientBirthDate (YYYYMMDD when present)
-- ClinicalName: Standardized filename format for easy identification
-- AccessionNumber: Hospital/PACS accession number
-- StudyDate: YYYYMMDD format
-- Modality: CT, MR, etc.
-- CallingAET: DICOM Calling AE Title of the upstream sender captured at intake
-- RemoteIP: Source IP address observed on the DICOM association
-- IdJson: Complete id.json from output directory (includes Pipeline info, SelectedSeries)
-- JsonDump: Basic study metadata (PatientName, AccessionNumber, etc.) - legacy
-- DicomMetadata: Complete DICOM tags from selected series (all standard tags)
-- CalculationResults: JSON with volumes, densities, sarcopenia metrics, etc.
-- ProcessedAt: Timestamp when study was first processed

-- ============================================================
-- JSON Structure Examples
-- ============================================================

-- IdJson example (complete id.json from output directory):
-- {
--   "PatientName": "John Doe",
--   "PatientID": "1234567",
--   "PatientBirthDate": "19800115",
--   "AccessionNumber": "123456",
--   "StudyInstanceUID": "1.2.840...",
--   "Modality": "CT",
--   "StudyDate": "20260201",
--   "CaseID": "JohnD_20260201_123456",
--   "ClinicalName": "JohnD_20260201_123456",
--   "Pipeline": {
--     "start_time": "2026-02-01T10:30:00",
--     "end_time": "2026-02-01T10:35:00",
--     "elapsed_time": "0:05:00"
--   },
--   "SelectedSeries": {
--     "SeriesNumber": "4",
--     "ContrastPhaseData": {
--       "phase": "native",
--       "probability": 0.95
--     }
--   }
-- }

-- JsonDump example (legacy, basic metadata):
-- {
--   "PatientName": "John Doe",
--   "PatientID": "1234567",
--   "PatientBirthDate": "19800115",
--   "AccessionNumber": "123456",
--   "StudyInstanceUID": "1.2.840...",
--   "Modality": "CT",
--   "StudyDate": "20260201",
--   "CaseID": "JohnD_20260201_123456",
--   "ClinicalName": "JohnD_20260201_123456"
-- }

-- DicomMetadata example:
-- {
--   "PatientName": "John Doe",
--   "PatientID": "1234567",
    --   "PatientAge": "045Y",
--   "Modality": "CT",
--   "SliceThickness": "1.0",
--   "KVP": "120",
--   "ConvolutionKernel": "FC07",
--   "_PipelineSelectedPhase": "native",
--   "_PipelineSelectedKernel": "fc07",
--   ... (all DICOM tags)
-- }

-- CalculationResults example:
-- {
--   "volumes": {
--     "liver": 1234.5,
--     "spleen": 234.5,
--     ...
--   },
--   "densities": {
--     "liver_hu_mean": 55.2,
--     "liver_hu_std": 12.3,
--     ...
--   },
--   "sarcopenia": {
--     "l3_sma_cm2": 145.2,
--     "l3_muscle_hu": 42.1,
--     ...
--   },
--   "hemorrhage": {
--     "total_volume_ml": 12.5,
--     ...
--   },
--   "body_regions": ["head", "thorax", "abdomen"]
-- }
