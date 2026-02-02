#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) 2026 Rodrigo Americo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import sys
import shutil
import zipfile
import subprocess
import argparse
import tempfile
import json
import re
import numpy as np
import datetime
from pathlib import Path
import pydicom
import sqlite3
import concurrent.futures # Adicionado para multithreading

# Ensure venv/bin is in PATH/totalseg_get_phase
os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]

# ============================================================
# CONFIGURATIONS
# ============================================================

BASE = Path(__file__).resolve().parent
INPUT_DIR = BASE / "input"
OUTPUT_BASE_DIR = BASE / "output"
DB_PATH = BASE / "database" / "dicom.db"

INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_BASE_DIR.mkdir(exist_ok=True)
DB_PATH.parent.mkdir(exist_ok=True) 

def clean_filename(s):
    s = str(s).strip()
    return re.sub(r'[^a-zA-Z0-9_-]', '', s)

def generate_clinical_name(patient_name, study_date_str, accession_number):
    """
    Generates ClinicalFileName: [FirstName][Initials]_[YYYYMMDD]_[AccessionNumber]
    Example: RodrigoACS_20260131_5531196
    """
    if not patient_name or patient_name == "Unknown": return "Unknown"
    
    # Normalize name
    parts = patient_name.upper().split()
    if not parts: return "Unknown"
    
    # Filter particles (<= 3 chars)
    # Exception: First name is always kept regardless of length
    first = parts[0]
    rest = parts[1:]
    
    kept_rest = [p for p in rest if len(p) > 3]
    
    # Format
    # First name: Capitalized fully (Rodrigo)
    final_first = first.capitalize()
    
    # Initials: First char of each remaining part
    final_initials = "".join([p[0] for p in kept_rest])
    
    # Date
    if not study_date_str or len(study_date_str) < 8:
        study_date_str = "00000000"

    # Accession
    acc = str(accession_number).strip()
    if not acc: acc = "000000"
    # Remove non-alphanumeric from accession just to be safe
    acc = re.sub(r'[^a-zA-Z0-9]', '', acc)
        
    return f"{final_first}{final_initials}_{study_date_str}_{acc}"

def init_and_insert_db(metadata):
    """
    Inserts DICOM metadata into SQLite DB.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Create Table (Added DicomMetadata, CalculationResults, and IdJson)
        c.execute('''
            CREATE TABLE IF NOT EXISTS dicom_metadata (
                StudyInstanceUID TEXT PRIMARY KEY,
                PatientName TEXT,
                ClinicalName TEXT,
                AccessionNumber TEXT,
                StudyDate TEXT,
                Modality TEXT,
                IdJson TEXT,
                JsonDump TEXT,
                DicomMetadata TEXT,
                CalculationResults TEXT,
                ProcessedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Ensure new columns exist if table already exists (Migration Hack)
        try: c.execute("ALTER TABLE dicom_metadata ADD COLUMN IdJson TEXT")
        except: pass
        try: c.execute("ALTER TABLE dicom_metadata ADD COLUMN CalculationResults TEXT")
        except: pass

        # Upsert (Only initial fields)
        c.execute('''
            INSERT OR REPLACE INTO dicom_metadata 
            (StudyInstanceUID, PatientName, ClinicalName, AccessionNumber, StudyDate, Modality, JsonDump)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            metadata["StudyInstanceUID"],
            metadata["PatientName"],
            metadata.get("ClinicalName", ""),
            metadata["AccessionNumber"],
            metadata.get("StudyDate", ""),
            metadata["Modality"],
            json.dumps(metadata)
        ))
        
        conn.commit()
        conn.close()
        print(f"  [DB] Metadata saved for {metadata['StudyInstanceUID']}")
        
    except Exception as e:
        print(f"  [Error] DB Insert failed: {e}")

def extract_full_dicom_metadata(ds):
    """
    Extracts all standard DICOM tags into a dictionary.
    Excludes Pixel Data and long binary fields.
    """
    meta = {}
    for elem in ds:
        if elem.tag.group == 0x7FE0: continue # Skip Pixel Data
        keyword = elem.keyword
        if not keyword: continue
        
        val = elem.value
        # Handle types
        if isinstance(val, (pydicom.multival.MultiValue, list, tuple)):
            val = [str(x) for x in val]
        elif isinstance(val, (bytes, bytearray)):
             val = "<binary>"
        else:
             val = str(val)
             
        meta[keyword] = val
    return meta

def update_db_full_metadata(study_uid, full_meta):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("UPDATE dicom_metadata SET DicomMetadata = ? WHERE StudyInstanceUID = ?", 
                  (json.dumps(full_meta), study_uid))
        conn.commit()
        conn.close()
        print(f"  [DB] Full DICOM Metadata updated for {study_uid}")
    except Exception as e:
        print(f"  [Error] DB Update Full Metadata failed: {e}")

def get_tag_value(ds, tag, default=None):
    return getattr(ds, tag, default)

def process_ct_series_concurrency(uid, s_data, case_output_dir, temp_dir):
    """
    Helper function to process a single CT series in a thread.
    Returns candidate dict or None if failed.
    """
    try:
        s_num = s_data["SeriesNumber"]
        files = s_data["files"]
        
        if len(files) < 2: return None
        
        prefix = "ct" if s_data["Modality"] == "CT" else "ot"
        nii_filename = f"{prefix}_{s_num}.nii.gz"
        nii_path = case_output_dir / nii_filename
        
        # Convert
        if not convert_series(s_num, files, nii_path, temp_dir):
            return None
            
        # Phase Detection
        phase = "unknown"
        if s_data["Modality"] == "CT":
            json_path = case_output_dir / f"contrast_phase_{s_num}.json"
            phase_data = run_totalseg_phase(nii_path, json_path)
            if phase_data:
                phase = phase_data.get("phase", "unknown")
        
        # Return result dict
        return {
            "uid": uid,
            "series_number": s_num,
            "path": nii_path,
            "num_slices": len(files),
            "kernel": s_data["ConvolutionKernel"].lower(),
            "phase": phase
        }
    except Exception as e:
        print(f"  [Error] Failed to process series {s_data.get('SeriesNumber')}: {e}")
        return None

def run_totalseg_phase(input_nifti, output_json):
    """Runs totalseg_get_phase to detect CT contrast phase."""
    try:
        cmd = [
            "totalseg_get_phase",
            "-i", str(input_nifti),
            "-o", str(output_json),
            "-q"
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if output_json.exists():
            with open(output_json, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"  Warning: Phase detection failed for {input_nifti.name}: {e}")
    return None

def is_4d_series(files_list):
    """
    Check if series is 4D (Time resolved) by checking for duplicate ImagePositions.
    """
    positions = set()
    for f in files_list:
        try:
            ds = pydicom.dcmread(str(f), stop_before_pixels=True)
            if hasattr(ds, "ImagePositionPatient"):
                pos = tuple(ds.ImagePositionPatient)
                if pos in positions:
                    return True # Duplicate position found -> 4D
                positions.add(pos)
        except:
            pass
    return False

def convert_series(series_id, files_list, output_nii_path, temp_dir):
    """
    Converts a specific list of DICOM files to NIfTI.
    """
    dcm_in = temp_dir / f"dcm_{series_id}"
    dcm_in.mkdir(exist_ok=True)
    for f in files_list:
        shutil.copy(f, dcm_in)
        
    dcm_out = temp_dir / f"nii_{series_id}"
    dcm_out.mkdir(exist_ok=True)
    
    subprocess.run([
        "dcm2niix", "-z", "y", "-f", "converted", "-o", str(dcm_out), str(dcm_in)
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    generated = list(dcm_out.glob("*.nii.gz"))
    if not generated:
        return False
        
    target_nii = max(generated, key=lambda p: p.stat().st_size)
    shutil.move(str(target_nii), str(output_nii_path))
    return True

def process_zip(zip_path):
    start_time_str = datetime.datetime.now().isoformat()
    zip_path = Path(zip_path)
    if not zip_path.exists():
        print(f"Error: File {zip_path} not found.")
        sys.exit(1)

    print(f"Processing {zip_path.name}...")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        extract_dir = temp_dir / "extracted"
        extract_dir.mkdir()
        
        # 1. Extract ZIP
        try:
            print("  Extracting...")
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(extract_dir)
        except zipfile.BadZipFile:
             print("Error: Invalid ZIP file.")
             sys.exit(1)

        # 2. Scanning DICOMs
        print("  Scanning DICOM files...")
        series_map = {}
        
        global_meta = {
            "PatientName": "Unknown",
            "AccessionNumber": "000000",
            "StudyInstanceUID": "",
            "StudyDate": "", # Added
            "Modality": "" # Overall Modality match
        }
        
        found_ct = False
        
        for root, _, files in os.walk(extract_dir):
            for f in files:
                fpath = Path(root) / f
                try:
                    ds = pydicom.dcmread(str(fpath), stop_before_pixels=True)
                    if hasattr(ds, "SeriesInstanceUID"):
                        uid = ds.SeriesInstanceUID
                        modality = get_tag_value(ds, "Modality", "OT")
                        
                        if uid not in series_map:
                            series_map[uid] = {
                                "SeriesInstanceUID": uid,
                                "SeriesNumber": str(get_tag_value(ds, "SeriesNumber", "0")),
                                "Modality": modality,
                                "SliceThickness": float(get_tag_value(ds, "SliceThickness", 0.0) or 0.0),
                                "ConvolutionKernel": str(get_tag_value(ds, "ConvolutionKernel", "")),
                                "SeriesDescription": str(get_tag_value(ds, "SeriesDescription", "")).lower(),
                                "files": []
                            }
                            # Global Metadata (first encounter)
                            if global_meta["PatientName"] == "Unknown":
                                name_val = get_tag_value(ds, "PatientName", "")
                                if name_val:
                                    global_meta["PatientName"] = str(name_val).replace('^', ' ').strip()
                                global_meta["AccessionNumber"] = str(get_tag_value(ds, "AccessionNumber", "000000"))
                                global_meta["StudyInstanceUID"] = str(get_tag_value(ds, "StudyInstanceUID", ""))
                                global_meta["StudyDate"] = str(get_tag_value(ds, "StudyDate", ""))
                                global_meta["Modality"] = modality

                        series_map[uid]["files"].append(fpath)
                        if modality == "CT": found_ct = True
                        
                except Exception:
                    pass
        
        if not series_map:
            print("Error: No valid DICOM series found.")
            # sys.exit(1) # Don't exit, let's see what happens

        # Determine Global Modality
        if found_ct: 
            global_meta["Modality"] = "CT"
        
        exam_modality = global_meta["Modality"]
        print(f"  Exam Modality Detected: {exam_modality}")

        # ... (Output Dir Setup skipped in this chunk) ...

        # 4. Processing Logic Split
        final_selected_nii = None
        # 3. Setup Output Dir
        # --- NEW: Get Date/Accession for Clinical Name/CaseID ---
        study_date = global_meta.get("StudyDate", "00000000")
        acc_num = global_meta.get("AccessionNumber", "000000")
        
        clinical_name = generate_clinical_name(global_meta["PatientName"], str(study_date), str(acc_num))
        print(f"  Clinical Name (CaseID): {clinical_name}")
        
        # Override CaseID with ClinicalName
        case_id = clinical_name
        
        case_output_dir = OUTPUT_BASE_DIR / case_id
        case_output_dir.mkdir(parents=True, exist_ok=True)
        
        id_data = {
            "PatientName": global_meta["PatientName"],
            "AccessionNumber": global_meta["AccessionNumber"],
            "StudyInstanceUID": global_meta["StudyInstanceUID"],
            "Modality": exam_modality,
            "StudyDate": str(study_date),
            "CaseID": case_id,
            "ClinicalName": clinical_name
        }

        # Insert into DB immediately
        init_and_insert_db(id_data)
        
        with open(case_output_dir / "id.json", "w") as f:
            json.dump(id_data, f, indent=2)

        # 4. Processing Logic Split
        final_selected_nii = None
        
        if exam_modality == "MR":
            # --- MR LOGIC: SELECT FIRST, THEN CONVERT ---
            print("  [MR Mode] Analyzing series for selection BEFORE conversion...")
            mr_candidates = []
            
            for uid, s_data in series_map.items():
                # Filter out obvious non-MR or small stuff if deemed necessary
                # if s_data["Modality"] != "MR": continue 

                # 4D Check
                # Just assuming 'files' list order is somewhat valid, or random access is fine
                is_4d = is_4d_series(s_data["files"])
                
                score = len(s_data["files"])
                if is_4d: score = -5000 # Penalize heavily but don't crash
                if len(s_data["files"]) < 2: score = -9000
                
                mr_candidates.append({
                    "uid": uid,
                    "s_data": s_data,
                    "score": score,
                    "is_4d": is_4d
                })
            
            # Sort best first
            mr_candidates.sort(key=lambda x: x["score"], reverse=True)
            
            if not mr_candidates:
                print("Error: No valid MR candidates.")
                sys.exit(1)
                
            winner = mr_candidates[0]
            print(f"  Selected MR Series: {winner['s_data']['SeriesNumber']} (Slices: {len(winner['s_data']['files'])})")
            
            # Convert ONLY the winner
            s_num = winner['s_data']['SeriesNumber']
            nii_filename = f"mr_{s_num}.nii.gz"
            nii_path = case_output_dir / nii_filename
            
            print(f"    Converting Series {s_num}...")
            success = convert_series(s_num, winner['s_data']['files'], nii_path, temp_dir)
            
            if success:
                final_selected_nii = nii_path
            else:
                print("Error: Conversion of selected MR series failed.")
                sys.exit(1)

        else:
            # --- CT LOGIC: CONVERT ALL, PHASE DETECT, THEN SELECT ---
            print("  [CT Mode] Converting all series and running phase detection (Parallel - 3 Threads)...")
            candidates = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                futures = []
                for uid, s_data in series_map.items():
                    futures.append(
                        executor.submit(process_ct_series_concurrency, uid, s_data, case_output_dir, temp_dir)
                    )
                
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result:
                        candidates.append(result)
                        print(f"    Series {result['series_number']}: {result['num_slices']} slices, Phase: {result['phase']}, Kernel: {result['kernel']}")
            
            # Select Best CT
            if not candidates:
                 print("Error: No valid CT series converted.")
                 sys.exit(1)
                 
            def score_ct(c):
                score = c["num_slices"]
                # Phase Priority
                if c["phase"] == "native": score += 10000
                elif c["phase"] == "unknown": score += 0
                else: score += 500
                
                # Kernel
                k = c["kernel"]
                desc = c.get("description", "")
                
                # Bad Check (Kernel + Description)
                bad_keywords = ["lung", "b60", "b70", "b80", "bone", "sharp", "edge", 
                                "h60", "h70", "h80", "h90", "detail", "f8", "f9"]
                
                is_bad = any(x in k for x in bad_keywords) or any(x in desc for x in bad_keywords)
                
                # Good Check
                good_keywords = ["soft", "standard", "h30", "h40", "j30", "j40", "fc08", "fc10", "fc12", "brain"]
                is_good = any(x in k for x in good_keywords) or any(x in desc for x in good_keywords)
                
                if is_bad: score -= 2000
                elif is_good: score += 500
                else: score += 100
                
                return score
            
            # Enrich candidates with Description for the sort function
            for c in candidates:
                # Need to find desc from series_map using uid or s_data... 
                # Wait, candidates['uid'] is key.
                # Actually, I didn't pass desc to candidate dict in the loop above.
                # I need to fix the loop above first or look it up here.
                # Easier to look up here if I have access to series_map? Yes, it's in scope.
                c["description"] = series_map[c["uid"]]["SeriesDescription"]
                
            candidates.sort(key=score_ct, reverse=True)
            
            # --- DEBUG LOG ---
            print("\n  [DEBUG] CT Candidates Ranking:")
            for i, c in enumerate(candidates):
                ks = score_ct(c)
                print(f"    #{i+1}: Series {c['series_number']} | Slices: {c['num_slices']} | Phase: {c['phase']} | Kernel: {c['kernel']} | Desc: {c['description']} | Score: {ks}")
            # -----------------

            winner = candidates[0]
            print(f"  Selected CT Series: {winner['series_number']} (Phase: {winner['phase']})")
            final_selected_nii = winner['path']
            
            # --- NEW: Extract Full Metadata from Winner & Update DB ---
            try:
                # Need to find the original DICOM file. 
                # candidates dict has 'uid'. series_map has 'files' by uid.
                # series_map is available in this scope.
                w_uid = winner['uid']
                w_files = series_map[w_uid]['files']
                if w_files:
                    first_dcm = w_files[0]
                    ds_full = pydicom.dcmread(str(first_dcm), stop_before_pixels=True)
                    full_meta = extract_full_dicom_metadata(ds_full)
                    
                    # Add our calculated info
                    full_meta["_PipelineSelectedPhase"] = winner['phase']
                    full_meta["_PipelineSelectedKernel"] = winner['kernel']
                    full_meta["_PipelineSelectedSeriesDescription"] = winner.get('description', '')
                    
                    update_db_full_metadata(global_meta["StudyInstanceUID"], full_meta)
            except Exception as e:
                print(f"  [Error] Failed to extract/update full metadata: {e}")

        # 5. Final Handover & Metadata Enrichment
        if final_selected_nii and final_selected_nii.exists():
            dest_input = INPUT_DIR / f"{case_id}.nii.gz"
            shutil.copy(str(final_selected_nii), str(dest_input))
            print(f"\n  Ready: {dest_input}")
            print(str(dest_input))

            # Enrichment: Add Selection Info to id.json
            output_meta = id_data.copy()
            output_meta["Pipeline"] = {
                "start_time": start_time_str
            }
            
            selected_info = {
                "SeriesNumber": "",
                "ContrastPhaseData": {}
            }
            
            if exam_modality == "MR":
                selected_info["SeriesNumber"] = winner['s_data']['SeriesNumber']
            else:
                selected_info["SeriesNumber"] = winner['series_number']
                # Read specific phase JSON if exists
                phase_json_path = case_output_dir / f"contrast_phase_{winner['series_number']}.json"
                if phase_json_path.exists():
                    try:
                        with open(phase_json_path, 'r') as pf:
                            selected_info["ContrastPhaseData"] = json.load(pf)
                    except: pass

            output_meta["SelectedSeries"] = selected_info
            
            # Save updated id.json
            with open(case_output_dir / "id.json", "w") as f:
                json.dump(output_meta, f, indent=2)

            # CLEANUP: Delete generated .nii.gz and phase .jsons in output folder
            print("  Cleaning up intermediate files...")
            for f in case_output_dir.glob("*.nii.gz"):
                try: f.unlink()
                except: pass
                
            for f in case_output_dir.glob("contrast_phase_*.json"):
                try: f.unlink()
                except: pass

        else:
             print("Error: Selection failed.")
             sys.exit(1)

    # Clean up input ZIP
    if zip_path.exists():
        try:
            zip_path.unlink()
            print(f"  Deleted input ZIP: {zip_path}")
        except Exception as e:
            print(f"  Warning: Could not delete input ZIP: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("zip_path", help="Path to DICOM ZIP")
    args = parser.parse_args()
    process_zip(args.zip_path)