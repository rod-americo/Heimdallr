#!/usr/bin/env python3
"""
Heimdallr Processing Daemon (run.py)

Monitors the input/ directory for new NIfTI files and processes them through:
1. TotalSegmentator organ and tissue segmentation (parallel)
2. Conditional specialized analysis (e.g., cerebral hemorrhage if brain detected)
3. Metrics calculation (volumes, densities, sarcopenia)
4. Results archival

Supports parallel processing of up to 3 cases simultaneously.
"""

import os
import json
import shutil
import subprocess
import threading
import sys
import time
import datetime
import concurrent.futures  # For parallel case processing
from pathlib import Path

# Import metrics calculation module
from metrics import calculate_all_metrics

# Import centralized configuration
import config

# Ensure virtual environment binaries (TotalSegmentator, dcm2niix) are in PATH
os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]

# ============================================================
# CONFIGURATION
# ============================================================

# Use centralized configuration
LICENSE = config.TOTALSEGMENTATOR_LICENSE
BASE_DIR = config.BASE_DIR
INPUT_DIR = config.INPUT_DIR
OUTPUT_DIR = config.OUTPUT_DIR
ARCHIVE_DIR = config.NII_DIR
NII_DIR = ARCHIVE_DIR  # Alias for compatibility
ERROR_DIR = config.ERROR_DIR

# Create directories if they don't exist
config.ensure_directories()


def run_task(task_name, input_file, output_folder, extra_args=None):
    """
    Execute a TotalSegmentator task.
    
    Args:
        task_name: TotalSegmentator task (e.g., 'total', 'tissue_types', 'cerebral_bleed')
        input_file: Path to input NIfTI file
        output_folder: Directory for output segmentation masks
        extra_args: Additional command-line arguments (e.g., ['--fast'])
    
    Raises:
        CalledProcessError: If TotalSegmentator exits with non-zero status
    """
    if extra_args is None:
        extra_args = []
    print(f"[{task_name}] Starting...")
    
    # Build TotalSegmentator command
    cmd = [
        "TotalSegmentator",
        "-l", LICENSE,
        "-i", str(input_file),
        "-o", str(output_folder),
        "--task", task_name
    ] + extra_args
    
    # Run with Popen to capture and filter output
    # This allows us to suppress citation messages while preserving important logs
    process = subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT,
        text=True, 
        bufsize=1  # Line-buffered output
    )
    
    # Stream output line by line
    for line in process.stdout:
        # Optionally filter unwanted lines (citation currently enabled)
        # if "If you use this tool please cite" in line:
        #     continue
        print(line, end="")  # Full logging enabled
    
    # Wait for process completion
    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, cmd)
        
    print(f"[{task_name}] Finished.")


def process_case(nifti_path):
    """
    Process a single patient case through the complete pipeline.
    
    Steps:
    1. Parallel segmentation (organs + tissues)
    2. Conditional specialized analysis (e.g., hemorrhage if brain found)
    3. Metrics calculation and JSON output
    4. Update processing timestamps
    5. Archive NIfTI file
    
    Args:
        nifti_path: Path to NIfTI file in input/ directory
    
    Returns:
        bool: True if successful, False on error
    """
    # Extract case ID from filename (e.g., "PatientRACS_20260201_5531196.nii.gz" -> "PatientRACS_20260201_5531196")
    case_id = nifti_path.name.replace("".join(nifti_path.suffixes), "")
    case_output = OUTPUT_DIR / case_id
    
    # Create output directory structure
    if not case_output.exists():
        case_output.mkdir(parents=True)
    
    # Clean and recreate TotalSegmentator output directories
    # This ensures we don't mix results from multiple runs
    for subdir in ["total", "tissue_types"]:
        p = case_output / subdir
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(exist_ok=True)

    print(f"\n=== Processing Case: {case_id} ===")

    # Determine modality from metadata (created by prepare.py)
    # Default to CT if not found (for legacy compatibility)
    modality = "CT"
    id_json_path = case_output / "id.json"
    if id_json_path.exists():
        try:
            with open(id_json_path, 'r') as f:
                modality = json.load(f).get("Modality", "CT")
        except: 
            pass  # Silently default to CT
            
    print(f"Detected modality: {modality}")

    # ============================================================
    # STEP 1: Parallel Segmentation
    # ============================================================
    # Thread 1: General Anatomy
    #   - CT: 'total' task (104 organs)
    #   - MR: 'total_mr' task (MR-specific segmentation)
    # Both use --fast flag for improved performance
    
    task_gen = "total"
    if modality == "MR":
        task_gen = "total_mr"
        
    # Start general anatomy segmentation in background thread
    t1 = threading.Thread(
        target=run_task, 
        args=(task_gen, nifti_path, case_output / "total", ["--fast"])
    )
    t1.start()
    
    # Thread 2: Tissue Segmentation (CT only)
    #   Segments skeletal muscle, visceral/subcutaneous fat
    t2 = None
    if modality == "CT":
        t2 = threading.Thread(
            target=run_task, 
            args=("tissue_types", nifti_path, case_output / "tissue_types")
        )
        t2.start()

    # Wait for both threads to complete
    t1.join()
    if t2:
        t2.join()

    # ============================================================
    # STEP 1.5: Conditional Specialized Analysis
    # ============================================================
    # If brain detected and modality is CT -> Run hemorrhage detection
    brain_file = case_output / "total" / "brain.nii.gz"
    if modality == "CT" and brain_file.exists():
        try:
             # Check if brain mask is non-empty
             # TotalSegmentator sometimes creates empty placeholder files
             if brain_file.stat().st_size > 1000:  # 1KB threshold
                 print("[Conditional] Brain detected. Running hemorrhage detection...")
                 bleed_output = case_output / "bleed"
                 bleed_output.mkdir(exist_ok=True)
                 run_task("cerebral_bleed", nifti_path, bleed_output)
        except Exception as e:
            print(f"Error during conditional hemorrhage analysis: {e}")

    # ============================================================
    # STEP 2: Metrics Calculation
    # ============================================================
    # Calculate all metrics (volumes, densities, sarcopenia, hemorrhage)
    # Results written to resultados.json
    try:
        metrics = calculate_all_metrics(case_id, nifti_path, case_output)
        
        json_path = case_output / "resultados.json"
        with open(json_path, "w") as f:
            json.dump(metrics, f, indent=2)
            
        print(f"Success. Results saved to: {json_path}")
        
    except Exception as e:
        print(f"Error calculating metrics for {case_id}: {e}")
        
        # Write error log
        with open(case_output / "error.log", "w") as f:
            f.write(str(e))
        
        # Move NIfTI to error directory to prevent infinite reprocessing
        error_dest = ERROR_DIR / nifti_path.name
        try:
            shutil.move(str(nifti_path), str(error_dest))
            print(f"Input moved to error folder: {error_dest}")
        except Exception as move_err:
            print(f"Critical error: Could not move error file {nifti_path}: {move_err}")
            
        return False


    # ============================================================
    # STEP 3: Update Processing Timestamps
    # ============================================================
    # Add end_time and elapsed_time to id.json Pipeline metadata
    if id_json_path.exists():
        try:
            with open(id_json_path, 'r') as f:
                meta = json.load(f)
            
            pipeline_data = meta.get("Pipeline", {})
            start_str = pipeline_data.get("start_time")
            
            end_dt = datetime.datetime.now()
            pipeline_data["end_time"] = end_dt.isoformat()
            
            # Calculate elapsed time
            if start_str:
                try:
                    start_dt = datetime.datetime.fromisoformat(start_str)
                    delta = end_dt - start_dt
                    pipeline_data["elapsed_time"] = str(delta)
                except:
                    pipeline_data["elapsed_time"] = "Error parsing start_time"
            else:
                 pipeline_data["elapsed_time"] = "Unknown start_time"
                 
            meta["Pipeline"] = pipeline_data
            
            with open(id_json_path, 'w') as f:
                json.dump(meta, f, indent=2)
                
        except Exception as e:
            print(f"Error updating pipeline time: {e}")

    # ============================================================
    # STEP 4: Archive NIfTI File
    # ============================================================
    # Move processed NIfTI from input/ to nii/ archive
    # Use clinical naming if available
    try:
        # Try to read ClinicalName from id.json for better file organization
        final_name = case_id
        try:
            with open(case_output / "id.json", 'r') as f:
                idd = json.load(f)
                if "ClinicalName" in idd and idd["ClinicalName"] and idd["ClinicalName"] != "Unknown":
                    final_name = idd["ClinicalName"]
        except: 
            pass  # Use case_id if clinical name not available
        
        final_nii_path = NII_DIR / f"{final_name}.nii.gz"
        shutil.move(str(nifti_path), str(final_nii_path))
        print(f"Input archived to: {final_nii_path}")
    except Exception as e:
        print(f"Error archiving input: {e}")

    return True

def main():
    """
    Main daemon loop.
    
    Monitors input/ directory for new NIfTI files and processes them in parallel.
    Supports up to 3 simultaneous cases for optimal resource utilization.
    """
    print("Starting input/ directory monitoring (Parallel - 3 Cases)...")
    
    max_cases = config.MAX_PARALLEL_CASES  # Maximum concurrent cases from config
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_cases)
    
    processing_files = set()  # Track files currently being processed
    lock = threading.Lock()    # Thread-safe access to processing_files
    
    def on_complete(fut, f_path):
        """Callback when a case finishes processing."""
        with lock:
            if f_path in processing_files:
                processing_files.discard(f_path)
        try:
            fut.result()  # Raise exception if case failed
        except Exception as e:
            print(f"Error in case processing thread {f_path.name}: {e}")

    try:
        while True:
            try:
                # List all NIfTI files in input directory
                current_files = sorted(list(INPUT_DIR.glob("*.nii.gz")))
                
                for f in current_files:
                    with lock:
                        # If we're at max capacity, wait until next iteration
                        if len(processing_files) >= max_cases:
                            break
                        
                        # Skip if file is already being processed
                        if f in processing_files:
                            continue
                            
                        # Submit new case for processing
                        print(f"Submitting new case: {f.name}")
                        processing_files.add(f)
                        future = executor.submit(process_case, f)
                        future.add_done_callback(lambda fut, p=f: on_complete(fut, p))
            
                # Check for new files every 2 seconds
                time.sleep(2)
                
            except Exception as e:
                print(f"Error in main loop: {e}")
                time.sleep(2)
                
    except KeyboardInterrupt:
        print("\nStopping monitoring...")
        executor.shutdown(wait=False)
        print("Executor shutdown complete.")

if __name__ == "__main__":
    main()