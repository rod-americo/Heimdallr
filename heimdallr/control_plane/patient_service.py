import json
import logging
import sqlite3

from heimdallr.shared import store
from heimdallr.shared import settings
from heimdallr.shared.patient_names import normalize_patient_name_display
from heimdallr.shared.paths import study_derived_dir, study_dir, study_results_json

logger = logging.getLogger(__name__)


def parse_elapsed_seconds(elapsed_str):
    if not elapsed_str or ":" not in elapsed_str:
        return 0
    try:
        h, m, s = elapsed_str.split(':')
        return int(int(h) * 3600 + int(m) * 60 + float(s))
    except Exception:
        return 0

class PatientService:
    @staticmethod
    def get_all_patients(db: sqlite3.Connection):
        """
        Fetch all patients from the database.
        Calculates elapsed seconds, hemorrhage status, etc., natively instead of hitting the filesystem.
        """
        patients = []
        try:
            rows = store.list_patient_rows(db)
            
            for row in rows:
                metadata = {}
                results = {}
                
                # Safely parse JSON blocks
                try:
                    metadata = json.loads(row["IdJson"]) if row["IdJson"] else {}
                except Exception as e:
                    logger.warning(f"Failed to parse IdJson for {row['StudyInstanceUID']}: {e}")

                try:
                    results = json.loads(row["CalculationResults"]) if row["CalculationResults"] else {}
                except Exception as e:
                    logger.warning(f"Failed to parse CalculationResults for {row['StudyInstanceUID']}: {e}")

                # Case ID
                # Format: FirstNameInitials_YYYYMMDD_AccessionNumber
                # Using the CaseID if it exists, otherwise constructing it (which might not be perfect here)
                # But since the pipeline creates the CaseID and it should match the folder structure
                # We can try to extract from IdJson or use a fallback
                case_id = metadata.get("CaseID", f"{row['PatientName'][:3]}_{row['StudyDate']}_{row['AccessionNumber']}")
                filename = f"{case_id}.nii.gz"
                case_folder = study_dir(case_id)

                # Some legacy or interrupted runs wrote resultados.json to disk but did not
                # persist CalculationResults back into SQLite. The dashboard should still
                # expose those cases as having results.
                if not results:
                    results_path = study_results_json(case_id)
                    if results_path.exists():
                        try:
                            with open(results_path, "r") as f:
                                results = json.load(f)
                        except Exception as e:
                            logger.warning(f"Failed to read resultados.json for {case_id}: {e}")
                
                # File size (We might still need to hit the disk for this if not in DB, 
                # but it's a single stat() call instead of multiple file reads). 
                # To be purely DB driven, file size should ideally live in DB, but for now we fallback to disk.
                nii_path = study_derived_dir(case_id) / filename
                file_size = nii_path.stat().st_size if nii_path.exists() else 0
                
                # Elapsed time
                pipeline = metadata.get("Pipeline", {})
                elapsed_seconds = parse_elapsed_seconds(
                    pipeline.get("segmentation_elapsed_time")
                    or pipeline.get("processing_elapsed_time")
                    or pipeline.get("elapsed_time", "")
                )
                prepare_elapsed_seconds = parse_elapsed_seconds(pipeline.get("prepare_elapsed_time", ""))

                hemorrhage_vol = results.get("hemorrhage_vol_cm3")
                has_hemorrhage = isinstance(hemorrhage_vol, (int, float)) and hemorrhage_vol > 0.1
                
                patients.append({
                    "case_id": case_id,
                    "filename": filename,
                    "file_size_bytes": file_size,
                    "file_size_mb": round(file_size / (1024 * 1024), 2) if file_size else 0.0,
                    "patient_name": normalize_patient_name_display(
                        row["PatientName"] or metadata.get("PatientName", "Unknown"),
                        settings.PATIENT_NAME_PROFILE,
                    ),
                    "patient_id": row["PatientID"] or metadata.get("PatientID", ""),
                    "patient_birth_date": row["PatientBirthDate"] or metadata.get("PatientBirthDate", ""),
                    "study_date": row["StudyDate"] or metadata.get("StudyDate", ""),
                    "accession": row["AccessionNumber"] or metadata.get("AccessionNumber", ""),
                    "modality": row["Modality"] or metadata.get("Modality", ""),
                    "prepare_elapsed_seconds": prepare_elapsed_seconds,
                    "elapsed_seconds": elapsed_seconds,
                    "has_results": bool(results),
                    "body_regions": results.get("body_regions", []),
                    "has_hemorrhage": has_hemorrhage,
                    "artifacts_purged": bool(row["ArtifactsPurged"]),
                    "artifacts_purged_at": row["ArtifactsPurgedAt"],
                })
                
            # Sort alphabetically by the displayed name (first part of case_id)
            patients.sort(key=lambda x: x["case_id"].split('_')[0].lower())
            
            return patients
        except Exception as e:
            logger.error(f"Error fetching patients from DB: {e}")
            return []

    @staticmethod
    def get_patient_metadata(db: sqlite3.Connection, case_id: str):
        # The CaseID is theoretically inside IdJson. 
        # A Better way is searching by CaseID if we had it as a column.
        # For now, we fetch all and filter or use JSON functions if SQLite version allows
        row = store.find_case_row_by_case_id(db, case_id)
        if not row:
            return None
        metadata = json.loads(row["IdJson"]) if row["IdJson"] else {}
        if row["Weight"] is not None:
            metadata["Weight"] = row["Weight"]
        if row["Height"] is not None:
            metadata["Height"] = row["Height"]
        if row["PatientSex"] is not None:
            metadata["Sex"] = row["PatientSex"]
        if row["PatientID"] is not None:
            metadata["PatientID"] = row["PatientID"]
        if row["PatientBirthDate"] is not None:
            metadata["PatientBirthDate"] = row["PatientBirthDate"]
        if row["SMI"] is not None:
            metadata["SMI"] = row["SMI"]
        metadata["ArtifactsPurged"] = bool(row["ArtifactsPurged"])
        metadata["ArtifactsPurgedAt"] = row["ArtifactsPurgedAt"]
        return metadata

    @staticmethod
    def update_biometrics(db: sqlite3.Connection, case_id: str, weight: float, height: float):
        # Find StudyInstanceUID
        meta = PatientService.get_patient_metadata(db, case_id)
        if not meta:
            return None
        
        study_uid = meta.get("StudyInstanceUID")
        if not study_uid:
            return None
            
        meta["Weight"] = weight
        meta["Height"] = height
        
        store.update_id_json(db, study_uid, meta)
        return meta

    @staticmethod
    def update_smi(db: sqlite3.Connection, case_id: str, smi: float):
        row = store.find_case_row_by_case_id(db, case_id)
        if not row:
            return None
        results = json.loads(row["CalculationResults"]) if row["CalculationResults"] else {}
        results["SMI_cm2_m2"] = round(smi, 2)
        db.execute(
            "UPDATE dicom_metadata SET SMI = ?, CalculationResults = ? WHERE StudyInstanceUID = ?",
            (smi, json.dumps(results), row["StudyInstanceUID"]),
        )
        db.commit()
        return results
