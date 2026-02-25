#!/usr/bin/env python3
# Copyright (c) 2026 Rodrigo Americo
import os
import json
import sqlite3
from pathlib import Path

BASE_DIR = Path("/home/rodrigo/Heimdallr")
OUTPUT_DIR = BASE_DIR / "output"
DB_PATH = BASE_DIR / "database" / "dicom.db"

def main():
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    updated_cases = 0

    for case_dir in OUTPUT_DIR.iterdir():
        if not case_dir.is_dir():
            continue

        id_json_path = case_dir / "id.json"
        metrics_json_path = case_dir / "resultados.json"

        if not id_json_path.exists():
            continue

        with open(id_json_path, "r") as f:
            id_data = json.load(f)

        study_uid = id_data.get("StudyInstanceUID")
        if not study_uid:
            continue

        # Fetch KVP from Database
        c.execute("SELECT DicomMetadata, JsonDump FROM dicom_metadata WHERE StudyInstanceUID = ?", (study_uid,))
        row = c.fetchone()
        
        kvp_val = "Unknown"
        if row:
            dicom_meta_str, json_dump_str = row
            # Try DicomMetadata first
            if dicom_meta_str:
                try:
                    meta_dict = json.loads(dicom_meta_str)
                    kvp_val = meta_dict.get("KVP", "Unknown")
                except:
                    pass
            # Fallback to JsonDump
            if kvp_val == "Unknown" and json_dump_str:
                try:
                    dump_dict = json.loads(json_dump_str)
                    kvp_val = dump_dict.get("KVP", "Unknown")
                except:
                    pass

        # If we couldn't find KVP, try reading from other standard DICOM files if they exist somewhere?
        # Actually, if we have KVP, let's update id.json
        if "KVP" not in id_data or id_data["KVP"] != kvp_val:
            id_data["KVP"] = str(kvp_val)
            with open(id_json_path, "w") as f:
                json.dump(id_data, f, indent=2)
                
        # Now update metrics.json
        if metrics_json_path.exists():
            try:
                with open(metrics_json_path, "r") as f:
                    metrics_data = json.load(f)
                    
                # Clean up HU for missing organs and handle PDFF
                for organ in ["liver", "spleen", "kidney_right", "kidney_left"]:
                    vol_key = f"{organ}_vol_cm3"
                    hu_mean_key = f"{organ}_hu_mean"
                    hu_std_key = f"{organ}_hu_std"
                    
                    if vol_key in metrics_data:
                        vol = metrics_data[vol_key]
                        if vol <= 0.1:
                            # Set HU to None if organ is not present
                            metrics_data[hu_mean_key] = None
                            metrics_data[hu_std_key] = None
                            
                            # Specifically for liver, remove PDFF if it exists
                            if organ == "liver":
                                metrics_data.pop("liver_pdff_percent", None)
                                metrics_data.pop("liver_pdff_kvp", None)
                        else:
                            # Calculate PDFF for liver if volume is valid
                            if organ == "liver" and hu_mean_key in metrics_data:
                                hu_mean = metrics_data[hu_mean_key]
                                if hu_mean is not None:
                                    pdff = -0.58 * hu_mean + 38.2
                                    pdff = max(0.0, min(100.0, pdff))
                                    metrics_data["liver_pdff_percent"] = round(pdff, 2)
                                    metrics_data["liver_pdff_kvp"] = str(kvp_val)
                
                with open(metrics_json_path, "w") as f:
                    json.dump(metrics_data, f, indent=2)
                                
            except Exception as e:
                print(f"Error updating metrics for {case_dir.name}: {e}")

        print(f"Processed {case_dir.name} - KVP: {kvp_val}")
        updated_cases += 1

    conn.close()
    print(f"\nFinished processing {updated_cases} cases.")

if __name__ == "__main__":
    main()
