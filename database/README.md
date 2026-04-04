# Database Documentation

## Schema

The database schema is defined in [`schema.sql`](schema.sql).

## Structure

### Main Table: `dicom_metadata`

Stores DICOM metadata and calculation results for all processed studies.

| Column | Type | Description |
|--------|------|-------------|
| `StudyInstanceUID` | TEXT (PK) | Unique DICOM study identifier |
| `PatientName` | TEXT | Full patient name from DICOM |
| `ClinicalName` | TEXT | Standardized format: `FirstNameInitials_YYYYMMDD_AccessionNumber` |
| `AccessionNumber` | TEXT | Hospital/PACS accession number |
| `StudyDate` | TEXT | Study date in YYYYMMDD format |  
| `Modality` | TEXT | Imaging modality (CT, MR, etc.) |
| `CallingAET` | TEXT | DICOM Calling AE Title captured by intake |
| `RemoteIP` | TEXT | Sender IP captured by intake |
| `IdJson` | TEXT | Complete id.json from output directory (includes Pipeline info, SelectedSeries) |
| `JsonDump` | TEXT | Basic metadata JSON from prepare.py |
| `DicomMetadata` | TEXT | Complete DICOM tags JSON from selected series |
| `CalculationResults` | TEXT | Computed metrics JSON (volumes, densities, etc.) |
| `ProcessedAt` | TIMESTAMP | When study was first processed (America/Sao_Paulo) |

### Queue Tables

Operational dispatch is tracked in three queue tables:

| Table | Purpose |
|-------|---------|
| `processing_queue` | Prepared studies waiting for segmentation/processing |
| `metrics_queue` | Processed studies waiting for post-segmentation metrics |
| `dicom_egress_queue` | Generated DICOM artifacts waiting for outbound C-STORE delivery |

### Indexes

Performance indexes on commonly queried fields:
- `idx_clinical_name` - For quick lookup by clinical name
- `idx_accession` - For accession number searches
- `idx_study_date` - For date range queries
- `idx_modality` - For filtering by modality
- `idx_processed_at` - For chronological queries

## Initialization

The database is automatically created by `prepare.py` on first run. The schema includes:

1. Table creation with `CREATE TABLE IF NOT EXISTS`
2. Automatic migration for new columns using `ALTER TABLE` (if table exists)
3. Index creation for performance

## Data Flow

```
prepare.py
  ↓
  Inserts: StudyInstanceUID, PatientName, ClinicalName, 
           AccessionNumber, StudyDate, Modality, JsonDump
  ↓
  Updates: DicomMetadata (full DICOM tags from selected series)

heimdallr.intake
  ↓
  Upserts: StudyInstanceUID, CallingAET, RemoteIP

heimdallr.metrics
  ↓
  Updates: CalculationResults (after metrics worker completes)
  ↓
  Enqueues: dicom_egress_queue entries for generated DICOM artifacts

heimdallr.dicom_egress
  ↓
  Claims: pending outbound artifacts and performs DICOM C-STORE delivery
```

## Querying Examples

### Get all studies
```sql
SELECT StudyInstanceUID, ClinicalName, Modality, ProcessedAt 
FROM dicom_metadata 
ORDER BY ProcessedAt DESC;
```

### Get studies with calculation results
```sql
SELECT ClinicalName, CalculationResults 
FROM dicom_metadata 
WHERE CalculationResults IS NOT NULL;
```

### Get CT studies from specific date
```sql
SELECT * FROM dicom_metadata 
WHERE Modality = 'CT' 
AND StudyDate = '20260201';
```

### Search by patient name
```sql
SELECT * FROM dicom_metadata 
WHERE PatientName LIKE '%Silva%';
```

## Backup

To backup the database:
```bash
# Create backup
cp database/dicom.db database/dicom_backup_$(date +%Y%m%d).db

# Or use SQLite dump
sqlite3 database/dicom.db .dump > database/backup.sql
```

## Restore

To restore from backup:
```bash
# From file copy
cp database/dicom_backup_20260201.db database/dicom.db

# From SQL dump
sqlite3 database/dicom.db < database/backup.sql
```

## Maintenance

### Vacuum (optimize database)
```bash
sqlite3 database/dicom.db "VACUUM;"
```

### Check integrity
```bash
sqlite3 database/dicom.db "PRAGMA integrity_check;"
```

## Migration History

### Version 1.0 (2026-02-01)
- Initial schema with `dicom_metadata` table
- Added `DicomMetadata` column for full DICOM tags
- Added `CalculationResults` column for computed metrics
- Created performance indexes
- Implemented automatic migration in `prepare.py`
