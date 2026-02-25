import sqlite3
import json
from sqlite3 import Connection
from typing import List, Dict, Any, Optional
import logging
import config
from core.database import get_db
from config import NII_DIR

logger = logging.getLogger(__name__)

class PatientService:
    @staticmethod
    def get_all_patients(db: sqlite3.Connection):
        """
        Fetch all patients from the database.
        Calculates elapsed seconds, hemorrhage status, etc., natively instead of hitting the filesystem.
        """
        patients = []
        try:
            # Query all metadata, ordered by StudyDate descending
            cursor = db.cursor()
            cursor.execute("SELECT * FROM dicom_metadata ORDER BY StudyDate DESC")
            rows = cursor.fetchall()
            
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
                
                # File size (We might still need to hit the disk for this if not in DB, 
                # but it's a single stat() call instead of multiple file reads). 
                # To be purely DB driven, file size should ideally live in DB, but for now we fallback to disk.
                nii_path = NII_DIR / filename
                file_size = nii_path.stat().st_size if nii_path.exists() else 0
                
                # Elapsed time
                pipeline = metadata.get("Pipeline", {})
                elapsed_str = pipeline.get("elapsed_time", "")
                elapsed_seconds = 0
                if elapsed_str and ":" in elapsed_str:
                    try:
                        h, m, s = elapsed_str.split(':')
                        elapsed_seconds = int(int(h) * 3600 + int(m) * 60 + float(s))
                    except Exception:
                        pass
                
                patients.append({
                    "case_id": case_id,
                    "filename": filename,
                    "file_size_bytes": file_size,
                    "file_size_mb": round(file_size / (1024 * 1024), 2) if file_size else 0.0,
                    "patient_name": row["PatientName"] or metadata.get("PatientName", "Unknown"),
                    "study_date": row["StudyDate"] or metadata.get("StudyDate", ""),
                    "accession": row["AccessionNumber"] or metadata.get("AccessionNumber", ""),
                    "modality": row["Modality"] or metadata.get("Modality", ""),
                    "elapsed_seconds": elapsed_seconds,
                    "has_results": bool(results),
                    "body_regions": results.get("body_regions", []),
                    "has_hemorrhage": results.get("hemorrhage_vol_cm3", 0.0) > 0.1
                })
                
            return patients
        except Exception as e:
            logger.error(f"Error fetching patients from DB: {e}")
            return []

    @staticmethod
    def get_patient_metadata(db: sqlite3.Connection, case_id: str):
        # The CaseID is theoretically inside IdJson. 
        # A Better way is searching by CaseID if we had it as a column.
        # For now, we fetch all and filter or use JSON functions if SQLite version allows
        cursor = db.cursor()
        # Fallback to json_extract if available, but for wider compatibility we might have to scan or 
        # just assume `CaseID` is consistently queryable.
        # However, looking at dicom_metadata schema, we only have StudyInstanceUID as primary key.
        # We need to find the study using the case_id (which is embedded in IdJson)
        # Using SQLite JSON1 extension:
        try:
            cursor.execute("SELECT IdJson, Weight, Height, PatientSex FROM dicom_metadata WHERE json_extract(IdJson, '$.CaseID') = ?", (case_id,))
            row = cursor.fetchone()
            if row:
                metadata = json.loads(row["IdJson"]) if row["IdJson"] else {}
                if row["Weight"] is not None:
                    metadata["Weight"] = row["Weight"]
                if row["Height"] is not None:
                    metadata["Height"] = row["Height"]
                if row["PatientSex"] is not None:
                    metadata["Sex"] = row["PatientSex"]
                return metadata
        except Exception as e:
            logger.warning(f"Error extracting JSON with json_extract (maybe not supported): {e}")

        # Fallback manual scan if json_extract fails
        cursor.execute("SELECT IdJson, Weight, Height, PatientSex FROM dicom_metadata")
        for row in cursor.fetchall():
            meta = json.loads(row["IdJson"]) if row["IdJson"] else {}
            if meta.get("CaseID") == case_id:
                if row["Weight"] is not None:
                    meta["Weight"] = row["Weight"]
                if row["Height"] is not None:
                    meta["Height"] = row["Height"]
                if row["PatientSex"] is not None:
                    meta["Sex"] = row["PatientSex"]
                return meta
        return None

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
        
        cursor = db.cursor()
        cursor.execute(
            "UPDATE dicom_metadata SET Weight = ?, Height = ?, IdJson = ? WHERE StudyInstanceUID = ?",
            (weight, height, json.dumps(meta), study_uid)
        )
        db.commit()
        return meta

    @staticmethod
    def update_smi(db: sqlite3.Connection, case_id: str, smi: float):
        # Fallback scan for CalculationResults update
        cursor = db.cursor()
        cursor.execute("SELECT StudyInstanceUID, IdJson, CalculationResults FROM dicom_metadata")
        for row in cursor.fetchall():
            meta = json.loads(row["IdJson"]) if row["IdJson"] else {}
            if meta.get("CaseID") == case_id:
                results = json.loads(row["CalculationResults"]) if row["CalculationResults"] else {}
                results["SMI_cm2_m2"] = round(smi, 2)
                
                # Update DB
                cursor.execute(
                    "UPDATE dicom_metadata SET SMI = ?, CalculationResults = ? WHERE StudyInstanceUID = ?",
                    (smi, json.dumps(results), row["StudyInstanceUID"])
                )
                db.commit()
                return results
        return None
