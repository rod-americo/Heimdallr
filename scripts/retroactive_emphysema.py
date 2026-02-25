#!/ env python3
import json
import sqlite3
from pathlib import Path
import sys
import os

# Add project root to sys.path
sys.path.append("/home/rodrigo/Heimdallr")

from metrics import calculate_all_metrics
import config

def run_retroactive():
    output_dir = Path(config.OUTPUT_DIR)
    nii_archive = Path(config.NII_DIR)
    db_path = Path(config.DB_PATH)
    
    cases = [d for d in output_dir.iterdir() if d.is_dir()]
    total_cases = len(cases)
    
    print(f"Starting retroactive processing for {total_cases} cases...")
    
    for idx, case_folder in enumerate(cases):
        case_id = case_folder.name
        print(f"[{idx+1}/{total_cases}] Processing {case_id}...", end="\r")
        
        # Try to find original NIfTI in archive (nii/)
        nifti_path = nii_archive / f"{case_id}.nii.gz"
        
        # If not there, try reading ClinicalName from id.json
        if not nifti_path.exists():
            id_json = case_folder / "id.json"
            if id_json.exists():
                with open(id_json, "r") as f:
                    meta = json.load(f)
                    clinical_name = meta.get("ClinicalName")
                    if clinical_name and clinical_name != "Unknown":
                        nifti_path = nii_archive / f"{clinical_name}.nii.gz"
        
        if not nifti_path.exists():
            # Search for any nifti with the case name in it as a fallback
            candidate = list(nii_archive.glob(f"*{case_id}*.nii.gz"))
            if candidate:
                nifti_path = candidate[0]

        if not nifti_path.exists():
            print(f"\n[Error] NIfTI not found for {case_id}. Skipping.")
            continue
            
        try:
            # Recalculate metrics
            results = calculate_all_metrics(case_id, nifti_path, case_folder)
            
            # Save resultados.json
            with open(case_folder / "resultados.json", "w") as f:
                json.dump(results, f, indent=2)
                
            # Update database
            if db_path.exists():
                conn = sqlite3.connect(str(db_path))
                c = conn.cursor()
                
                # Update CalculationResults
                c.execute(
                    "UPDATE dicom_metadata SET CalculationResults = ? WHERE IdJson LIKE ?",
                    (json.dumps(results), f'%{case_id}%')
                )
                conn.commit()
                conn.close()
                
        except Exception as e:
            print(f"\n[Error] Failed to process {case_id}: {e}")
            
    print(f"\nRetroactive processing complete!")

if __name__ == "__main__":
    run_retroactive()
