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
    
    -- Clinical Naming (FirstNameInitials_YYYYMMDD_AccessionNumber)
    ClinicalName TEXT,
    
    -- Study Information
    AccessionNumber TEXT,
    StudyDate TEXT,
    Modality TEXT,
    
    -- Metadata Storage (JSON)
    IdJson TEXT,                -- Complete id.json from output directory
    JsonDump TEXT,              -- Basic metadata from prepare.py (legacy)
    DicomMetadata TEXT,         -- Full DICOM tags from selected series
    CalculationResults TEXT,    -- Computed metrics from run.py/metrics.py
    
    -- Timestamps
    ProcessedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- Indexes for Performance
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_clinical_name ON dicom_metadata(ClinicalName);
CREATE INDEX IF NOT EXISTS idx_accession ON dicom_metadata(AccessionNumber);
CREATE INDEX IF NOT EXISTS idx_study_date ON dicom_metadata(StudyDate);
CREATE INDEX IF NOT EXISTS idx_modality ON dicom_metadata(Modality);
CREATE INDEX IF NOT EXISTS idx_processed_at ON dicom_metadata(ProcessedAt);

-- ============================================================
-- Schema Notes
-- ============================================================

-- StudyInstanceUID: Unique DICOM identifier (1.2.840.xxx...)
-- PatientName: Full patient name from DICOM
-- ClinicalName: Standardized filename format for easy identification
-- AccessionNumber: Hospital/PACS accession number
-- StudyDate: YYYYMMDD format
-- Modality: CT, MR, etc.
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
