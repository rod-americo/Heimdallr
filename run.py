#!/usr/bin/env python3
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

# Import metrics calculation modules
from core import kidney_stone_triage, metrics

# Import centralized configuration
import config
import sqlite3  # For database updates

# Ensure virtual environment binaries (TotalSegmentator, dcm2niix) are in PATH
os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]

# ============================================================
# CONFIGURATION
# ============================================================

# Use centralized configuration
LICENSE = config.TOTALSEGMENTATOR_LICENSE
BASE_DIR = config.BASE_DIR
INPUT_DIR = config.INPUT_DIR
PROCESSING_DIR = config.PROCESSING_DIR
OUTPUT_DIR = config.OUTPUT_DIR
ARCHIVE_DIR = config.NII_DIR
NII_DIR = ARCHIVE_DIR  # Alias for compatibility
ERROR_DIR = config.ERROR_DIR

# Create directories if they don't exist
config.ensure_directories()


# ============================================================
# PIPELINE LOGGER
# ============================================================

class PipelineLogger:
    """
    Dual logger that writes to both console and a log file.
    Used to capture the complete pipeline execution flow.
    """
    def __init__(self, log_file_path=None):
        self.log_file = None
        if log_file_path:
            self.log_file = open(log_file_path, 'w')
            self.log_file.write(f"=== Heimdallr Pipeline Log ===\n")
            self.log_file.write(f"Started: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            self.log_file.flush()
    
    def print(self, message):
        """Print to console and write to log file if available."""
        print(message)
        if self.log_file:
            self.log_file.write(message + "\n")
            self.log_file.flush()
    
    def close(self):
        """Close the log file."""
        if self.log_file:
            self.log_file.write(f"\nFinished: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            self.log_file.close()
            self.log_file = None



def run_task(task_name, input_file, output_folder, extra_args=None, max_retries=3, log_file=None):
    """
    Execute a TotalSegmentator task with retry logic and optional log file redirection.
    
    Args:
        task_name: TotalSegmentator task (e.g., 'total', 'tissue_types', 'cerebral_bleed')
        input_file: Path to input NIfTI file
        output_folder: Directory for output segmentation masks
        extra_args: Additional command-line arguments (e.g., ['--fast'])
        max_retries: Maximum number of retry attempts for config.json race conditions
        log_file: Optional path to write detailed logs (if None, prints to console)
    
    Raises:
        CalledProcessError: If TotalSegmentator exits with non-zero status
    """
    if extra_args is None:
        extra_args = []
    
    # Build TotalSegmentator command
    cmd = [
        "TotalSegmentator",
        "-i", str(input_file),
        "-o", str(output_folder),
        "--task", task_name
    ] + extra_args
    
    # Open log file if specified
    log_handle = None
    if log_file:
        log_handle = open(log_file, 'w')
        log_handle.write(f"=== TotalSegmentator Task: {task_name} ===\n")
        log_handle.write(f"Started: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_handle.write(f"Command: {' '.join(cmd)}\n\n")
        log_handle.flush()
        # Console: just show task name
        print(f"  • {task_name}")
    else:
        # Console: show starting message
        print(f"[{task_name}] Starting...")
    
    # Retry loop to handle transient race conditions on TotalSegmentator config.json
    # We don't recreate the file as it contains important state (prediction_counter, license, etc.)
    for attempt in range(max_retries):
        try:
            # Run with Popen to capture and filter output
            process = subprocess.Popen(
                cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT,
                text=True, 
                bufsize=1  # Line-buffered output
            )
            
            # Stream output line by line
            output_lines = []
            for line in process.stdout:
                output_lines.append(line)
                if log_handle:
                    # Write to log file
                    log_handle.write(line)
                    log_handle.flush()
                else:
                    # Print to console
                    print(line, end="")
            
            # Wait for process completion
            process.wait()
            if process.returncode != 0:
                # Check if error is due to config.json race condition
                full_output = ''.join(output_lines)
                if 'JSONDecodeError' in full_output and 'config.json' in full_output and attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 0.5  # 0.5s, 1s, 2s
                    msg = f"[{task_name}] ⚠️  Config race condition detected. Retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})"
                    if log_handle:
                        log_handle.write(f"\n{msg}\n")
                        log_handle.flush()
                    print(msg)
                    time.sleep(wait_time)  # Wait for other process to finish writing
                    continue
                else:
                    if log_handle:
                        log_handle.write(f"\nFailed with exit code: {process.returncode}\n")
                        log_handle.close()
                    raise subprocess.CalledProcessError(process.returncode, cmd)
            
            # Success
            if log_handle:
                log_handle.write(f"\nFinished: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                log_handle.write(f"Exit code: 0\n")
                log_handle.close()
            else:
                print(f"[{task_name}] Finished.")
            return
            
        except subprocess.CalledProcessError:
            if log_handle:
                log_handle.close()
            raise
        except Exception as e:
            if attempt < max_retries - 1:
                msg = f"[{task_name}] Unexpected error: {e}. Retrying... (attempt {attempt + 1}/{max_retries})"
                if log_handle:
                    log_handle.write(f"\n{msg}\n")
                    log_handle.flush()
                print(msg)
                time.sleep(2 ** attempt)
            else:
                if log_handle:
                    log_handle.close()
                raise
    
    # If we exhausted all retries
    if log_handle:
        log_handle.close()
    raise RuntimeError(f"[{task_name}] Failed after {max_retries} attempts")


def is_file_stable(file_path, min_age_seconds=None):
    """Return True when a new input file looks old enough to process."""
    if min_age_seconds is None:
        min_age_seconds = max(5, config.PROCESSING_SCAN_INTERVAL * 2)
    try:
        age_seconds = time.time() - file_path.stat().st_mtime
    except FileNotFoundError:
        return False
    return age_seconds >= min_age_seconds


def move_case_file(source_path, destination_dir):
    """Move a case NIfTI to a destination directory, overwriting any stale copy."""
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination_path = destination_dir / source_path.name
    if destination_path.exists():
        destination_path.unlink()
    shutil.move(str(source_path), str(destination_path))
    return destination_path


def claim_input_file(input_path):
    """
    Atomically move a case from input/ to processing/ so restarts do not resubmit it.
    """
    processing_path = PROCESSING_DIR / input_path.name
    if processing_path.exists():
        raise FileExistsError(f"Case already exists in processing/: {processing_path.name}")
    shutil.move(str(input_path), str(processing_path))
    return processing_path


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
    case_id = nifti_path.name.replace("".join(nifti_path.suffixes), "")
    case_output = OUTPUT_DIR / case_id
    case_output.mkdir(parents=True, exist_ok=True)

    log_dir = case_output / "logs"
    log_dir.mkdir(exist_ok=True)
    pipeline_log_path = None if config.VERBOSE_CONSOLE else log_dir / "pipeline.log"
    logger = PipelineLogger(pipeline_log_path)

    try:
        nifti_path = claim_input_file(nifti_path)
    except FileNotFoundError:
        logger.print(f"Skipping case because input disappeared before claim: {nifti_path.name}")
        logger.close()
        return False
    except Exception as e:
        logger.print(f"Failed to claim input file {nifti_path.name}: {e}")
        logger.close()
        return False

    try:
        for subdir in ["total", "tissue_types"]:
            p = case_output / subdir
            if p.exists():
                shutil.rmtree(p)
            p.mkdir(exist_ok=True)

        logger.print(f"\n=== Processing Case: {case_id} ===")

        modality = "CT"
        id_json_path = case_output / "id.json"
        if id_json_path.exists():
            try:
                with open(id_json_path, "r") as f:
                    modality = json.load(f).get("Modality", "CT")
            except Exception:
                pass
        logger.print(f"Detected modality: {modality}")

        task_gen = "total_mr" if modality == "MR" else "total"
        log_file_total = None if config.VERBOSE_CONSOLE else log_dir / f"{task_gen}.log"
        log_file_tissue = None if config.VERBOSE_CONSOLE else log_dir / "tissue_types.log"

        if not config.VERBOSE_CONSOLE:
            logger.print(f"\n[Segmentation] Running {2 if modality == 'CT' else 1} task(s) sequentially...")

        seg_start_time = time.time()
        run_task(
            task_gen,
            nifti_path,
            case_output / "total",
            log_file=log_file_total
        )

        if modality == "CT":
            run_task(
                "tissue_types",
                nifti_path,
                case_output / "tissue_types",
                log_file=log_file_tissue
            )

        if not config.VERBOSE_CONSOLE:
            seg_elapsed = time.time() - seg_start_time
            logger.print(f"[Segmentation] ✓ Complete ({seg_elapsed:.1f}s)")
            logger.print(f"  → Logs: {log_dir.relative_to(OUTPUT_DIR)}/")

        brain_file = case_output / "total" / "brain.nii.gz"
        brain_status, _, _ = metrics.get_structure_completeness(brain_file)
        if modality == "CT" and brain_file.exists():
            try:
                if brain_file.stat().st_size > 1000:
                    if brain_status == "Complete":
                        if not config.VERBOSE_CONSOLE:
                            logger.print("\n[Conditional] Brain detected and complete. Running hemorrhage detection...")
                        bleed_output = case_output / "bleed"
                        bleed_output.mkdir(exist_ok=True)
                        log_file_bleed = None if config.VERBOSE_CONSOLE else log_dir / "cerebral_bleed.log"
                        run_task("cerebral_bleed", nifti_path, bleed_output, log_file=log_file_bleed)
                        if not config.VERBOSE_CONSOLE:
                            logger.print("[Conditional] ✓ Hemorrhage detection complete")
                    else:
                        logger.print(f"[Conditional] Skipping hemorrhage detection due to brain status: {brain_status}")
            except Exception as e:
                logger.print(f"[Conditional] Error: {e}")

        kidney_stone_triage_summary = {}
        if modality == "CT":
            triage_dir = case_output / "urology"
            triage_json_path = triage_dir / "kidney_stone_triage.json"
            triage_render_dir = triage_dir / "kidney_stone_renders"
            try:
                if not config.VERBOSE_CONSOLE:
                    logger.print("\n[Conditional] Running kidney stone HU triage...")
                triage_report = kidney_stone_triage.write_report(
                    ct_path=nifti_path,
                    mask_dir=case_output / "total",
                    output_path=triage_json_path,
                    render_dir=triage_render_dir,
                )
                kidney_stone_triage_summary = kidney_stone_triage.summarize_report(
                    triage_report,
                    report_path=triage_json_path,
                    base_dir=case_output,
                )
                if not config.VERBOSE_CONSOLE:
                    logger.print("[Conditional] ✓ Kidney stone triage complete")
            except Exception as e:
                kidney_stone_triage_summary = {
                    "kidney_stone_triage_status": "Error",
                    "kidney_stone_triage_error": str(e),
                    "kidney_stone_triage_report_path": "urology/kidney_stone_triage.json",
                }
                logger.print(f"[Conditional] Kidney stone triage failed: {e}")

        if not config.VERBOSE_CONSOLE:
            logger.print("\n[Metrics] Calculating volumes and densities...")
        else:
            logger.print("Calculating metrics...")

        json_path = case_output / "resultados.json"
        metrics_data = metrics.calculate_all_metrics(case_id, nifti_path, case_output)
        metrics_data.update(kidney_stone_triage_summary)
        with open(json_path, "w") as f:
            json.dump(metrics_data, f, indent=2)
        if not config.VERBOSE_CONSOLE:
            logger.print("[Metrics] ✓ Saved to resultados.json")
        else:
            logger.print(f"Metrics saved to {json_path}")

        try:
            with open(json_path, "r") as f:
                results_data = json.load(f)

            study_uid = None
            id_json_path = case_output / "id.json"
            if id_json_path.exists():
                with open(id_json_path, "r") as f:
                    id_data = json.load(f)
                    study_uid = id_data.get("StudyInstanceUID")

            if study_uid:
                conn = sqlite3.connect(config.DB_PATH)
                c = conn.cursor()
                c.execute(
                    "UPDATE dicom_metadata SET CalculationResults = ? WHERE StudyInstanceUID = ?",
                    (json.dumps(results_data), study_uid)
                )
                conn.commit()
                conn.close()
                if not config.VERBOSE_CONSOLE:
                    logger.print("[Database] ✓ Updated calculation results")
                else:
                    logger.print(f"  [DB] Calculation results updated for {study_uid}")
            else:
                logger.print("[Database] ⚠️  Could not find StudyInstanceUID")
        except Exception as e:
            logger.print(f"  [Warning] Failed to update database with results: {e}")

        try:
            id_json_path = case_output / "id.json"
            if id_json_path.exists():
                with open(id_json_path, "r") as f:
                    meta = json.load(f)

                pipeline_data = meta.get("Pipeline", {})
                start_str = pipeline_data.get("start_time")
                end_dt = datetime.datetime.now()
                pipeline_data["end_time"] = end_dt.isoformat()

                if start_str:
                    try:
                        start_dt = datetime.datetime.fromisoformat(start_str)
                        pipeline_data["elapsed_time"] = str(end_dt - start_dt)
                    except Exception:
                        pipeline_data["elapsed_time"] = "Error parsing start_time"
                else:
                    pipeline_data["elapsed_time"] = "Unknown start_time"

                meta["Pipeline"] = pipeline_data
                with open(id_json_path, "w") as f:
                    json.dump(meta, f, indent=2)

                try:
                    study_uid = meta.get("StudyInstanceUID")
                    if study_uid:
                        conn = sqlite3.connect(config.DB_PATH)
                        c = conn.cursor()
                        c.execute(
                            "UPDATE dicom_metadata SET IdJson = ?, Weight = ?, Height = ? WHERE StudyInstanceUID = ?",
                            (json.dumps(meta), meta.get("Weight"), meta.get("Height"), study_uid)
                        )
                        conn.commit()
                        conn.close()
                        if not config.VERBOSE_CONSOLE:
                            logger.print("[Database] ✓ Updated id.json")
                        else:
                            logger.print(f"  [DB] id.json updated for {study_uid}")
                except Exception as e:
                    logger.print(f"  [Warning] Failed to update database with id.json: {e}")
        except Exception as e:
            logger.print(f"Error updating pipeline time: {e}")

        try:
            final_name = case_id
            try:
                with open(case_output / "id.json", "r") as f:
                    idd = json.load(f)
                    if "ClinicalName" in idd and idd["ClinicalName"] and idd["ClinicalName"] != "Unknown":
                        final_name = idd["ClinicalName"]
            except Exception:
                pass

            final_nii_path = NII_DIR / f"{final_name}.nii.gz"
            if final_nii_path.exists():
                final_nii_path.unlink()
            shutil.move(str(nifti_path), str(final_nii_path))
            if not config.VERBOSE_CONSOLE:
                logger.print(f"\n[Archive] ✓ Moved to nii/{final_name}.nii.gz")
            else:
                logger.print(f"Input archived to: {final_nii_path}")
        except Exception as e:
            logger.print(f"Error archiving input: {e}")

        if not config.VERBOSE_CONSOLE:
            try:
                with open(case_output / "id.json", "r") as f:
                    meta = json.load(f)
                    elapsed_str = meta.get("Pipeline", {}).get("elapsed_time", "Unknown")
                    logger.print(f"\n✅ Case complete ({elapsed_str})")
            except Exception:
                logger.print("\n✅ Case complete")

        logger.close()
        return True

    except Exception as e:
        logger.print(f"Unhandled processing error for {case_id}: {e}")
        try:
            with open(case_output / "error.log", "w") as f:
                f.write(str(e))
        except Exception:
            pass
        try:
            if nifti_path.exists():
                error_dest = move_case_file(nifti_path, ERROR_DIR)
                logger.print(f"Input moved to error folder: {error_dest}")
        except Exception as move_err:
            logger.print(f"Critical error: Could not move error file {nifti_path}: {move_err}")
        logger.close()
        return False


def main():
    """
    Main daemon loop.
    
    Monitors input/ directory for new NIfTI files and processes them in parallel.
    Supports up to 3 simultaneous cases for optimal resource utilization.
    """
    print("Starting input/ directory monitoring...")
    
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

                        # Skip files that may still be mid-copy.
                        if not is_file_stable(f):
                            continue

                        # Skip files that already have a claimed twin in processing/.
                        if (PROCESSING_DIR / f.name).exists():
                            continue
                            
                        # Submit new case for processing
                        print(f"Submitting new case: {f.name}")
                        processing_files.add(f)
                        future = executor.submit(process_case, f)
                        future.add_done_callback(lambda fut, p=f: on_complete(fut, p))
            
                time.sleep(config.PROCESSING_SCAN_INTERVAL)
                
            except Exception as e:
                print(f"Error in main loop: {e}")
                time.sleep(config.PROCESSING_SCAN_INTERVAL)
                
    except KeyboardInterrupt:
        print("\nStopping monitoring...")
        executor.shutdown(wait=False)
        print("Executor shutdown complete.")

if __name__ == "__main__":
    main()
