# ---------------------------------------------------------------------------
# TotalSegmentator Processing API — CT/MR Organ Segmentation Service
#
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
#
# Uses TotalSegmentator by Wasserthal et al.
# Citation: https://pubs.rsna.org/doi/10.1148/ryai.230024
# ---------------------------------------------------------------------------

"""
TotalSegmentator Processing API

Replaces the filesystem-polling daemon (run.py) with an HTTP API.
Keeps the TotalSegmentator module and nnUNet runtime pre-loaded in the
process so that subsequent calls skip the heavy import/init overhead.

Endpoints:
  POST /process   — Process a NIfTI file through the full segmentation pipeline
  GET  /health    — Readiness probe

Port: 8004 (configurable via TOTALSEGMENTATOR_PORT env var)
"""

import os
import sys
import json
import shutil
import asyncio
import contextlib
import time
import datetime
import tempfile
import uuid
import base64
from pathlib import Path
from typing import Dict, Any
from tempfile import NamedTemporaryFile

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Request, Body
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Ensure virtual environment binaries are in PATH
os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]

# Ensure central configuration is accessible
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Import centralized configuration
import config
import sqlite3

# Import metrics calculation module
from metrics import calculate_all_metrics

# Configuration
PORT = int(os.getenv("TOTALSEGMENTATOR_PORT", "8004"))
LICENSE = config.TOTALSEGMENTATOR_LICENSE

# Directory paths from config
BASE_DIR = config.BASE_DIR
OUTPUT_DIR = config.OUTPUT_DIR
NII_DIR = config.NII_DIR
ERROR_DIR = config.ERROR_DIR


# ============================================================
# PIPELINE LOGGER (from run.py)
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


# ============================================================
# GLOBAL STATE
# ============================================================

class AppState:
    """Holds pre-loaded module references and GPU lock."""
    totalsegmentator_fn = None   # Reference to totalsegmentator.python_api.totalsegmentator
    lock = asyncio.Lock()
    ready = False

state = AppState()


# ============================================================
# LIFESPAN — Pre-load TotalSegmentator
# ============================================================

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Pre-import the TotalSegmentator Python API on startup.
    This avoids the heavy import cost (~10-15s) on every request.
    """
    print(f"[TotalSegmentator API] Loading TotalSegmentator Python API...")
    print(f"[TotalSegmentator API] License: {LICENSE[:8]}...")

    try:
        # Import the Python API (this triggers torch, nnunet imports)
        from totalsegmentator.python_api import totalsegmentator as ts_fn
        state.totalsegmentator_fn = ts_fn

        # Set up nnUNet environment
        from totalsegmentator.config import setup_nnunet, setup_totalseg, set_license_number
        setup_nnunet()
        setup_totalseg()
        set_license_number(LICENSE)

        state.ready = True
        print("[TotalSegmentator API] Ready. Module pre-loaded successfully.")

    except Exception as e:
        print(f"[TotalSegmentator API] FATAL: Failed to load: {e}")
        import traceback
        traceback.print_exc()

    yield

    # Cleanup
    print("[TotalSegmentator API] Shutting down...")
    import torch
    if torch.cuda.is_available():
        torch.cuda.empty_cache()


app = FastAPI(title="TotalSegmentator Processing API", lifespan=lifespan)


# ============================================================
# RESPONSE MODEL
# ============================================================

class ProcessResponse(BaseModel):
    status: str
    case_id: str
    timings: Dict[str, float] = Field(..., description="Processing times in seconds")
    metrics_summary: Dict[str, Any] = Field(default_factory=dict, description="Key metrics")
    error: str = None


# ============================================================
# SEGMENTATION RUNNER (replaces run_task subprocess calls)
# ============================================================

def run_segmentation(input_path: Path, output_path: Path, task: str,
                     fast: bool = False, logger: PipelineLogger = None):
    """
    Run TotalSegmentator via Python API for a given task.

    Args:
        input_path: Path to input NIfTI file
        output_path: Output directory for segmentation masks
        task: TotalSegmentator task name (e.g., 'total', 'tissue_types', 'cerebral_bleed')
        fast: Use fast mode (only valid for 'total' and 'total_mr')
        logger: Optional pipeline logger
    """
    log = logger.print if logger else print

    # --fast only valid for total and total_mr
    use_fast = fast and task in ("total", "total_mr")

    log(f"  • {task}" + (" (fast)" if use_fast else ""))

    try:
        state.totalsegmentator_fn(
            input=input_path,
            output=output_path,
            task=task,
            fast=use_fast,
            license_number=LICENSE,
            device="gpu",
            quiet=True,
            verbose=False,
        )
        log(f"  ✓ {task} complete")
    except Exception as e:
        log(f"  ✗ {task} failed: {e}")
        raise


# ============================================================
# MAIN PROCESSING PIPELINE (from run.py process_case)
# ============================================================

def process_case(nifti_path: Path, case_id: str) -> Dict[str, Any]:
    """
    Process a single patient case through the complete pipeline.

    Steps:
    1. Segmentation (organs + tissues, sequential)
    2. Conditional specialized analysis (hemorrhage if brain found)
    3. Metrics calculation and JSON output
    4. Database updates
    5. Archive NIfTI file

    Args:
        nifti_path: Path to NIfTI file
        case_id: Patient case identifier

    Returns:
        dict with status, timings, and metrics summary
    """
    case_output = OUTPUT_DIR / case_id
    timings = {}

    # Create output directory structure
    case_output.mkdir(parents=True, exist_ok=True)
    log_dir = case_output / "logs"
    log_dir.mkdir(exist_ok=True)

    # Initialize pipeline logger
    pipeline_log_path = None if config.VERBOSE_CONSOLE else log_dir / "pipeline.log"
    logger = PipelineLogger(pipeline_log_path)

    # Clean and recreate segmentation output directories
    for subdir in ["total", "tissue_types"]:
        p = case_output / subdir
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(exist_ok=True)

    logger.print(f"\n=== Processing Case: {case_id} ===")

    # Determine modality from metadata (created by prepare.py)
    modality = "CT"
    id_json_path = case_output / "id.json"
    if id_json_path.exists():
        try:
            with open(id_json_path, 'r') as f:
                modality = json.load(f).get("Modality", "CT")
        except:
            pass

    logger.print(f"Detected modality: {modality}")

    # ============================================================
    # STEP 1: Segmentation (Sequential — no multithreading)
    # ============================================================
    # Task 1: General Anatomy
    task_gen = "total_mr" if modality == "MR" else "total"
    # --fast only for 'total' (CT), not for 'total_mr'
    use_fast = (task_gen == "total")

    seg_start_time = time.time()
    logger.print(f"\n[Segmentation] Running tasks sequentially...")

    # Run general anatomy segmentation
    run_segmentation(nifti_path, case_output / "total", task_gen,
                     fast=use_fast, logger=logger)

    # Task 2: Tissue Segmentation (CT only)
    if modality == "CT":
        run_segmentation(nifti_path, case_output / "tissue_types", "tissue_types",
                         fast=False, logger=logger)

    seg_elapsed = time.time() - seg_start_time
    timings["segmentation"] = round(seg_elapsed, 1)
    logger.print(f"[Segmentation] ✓ Complete ({seg_elapsed:.1f}s)")

    # ============================================================
    # STEP 1.5: Conditional Specialized Analysis
    # ============================================================
    brain_file = case_output / "total" / "brain.nii.gz"
    if modality == "CT" and brain_file.exists():
        try:
            if brain_file.stat().st_size > 1000:  # Non-empty threshold
                logger.print("\n[Conditional] Brain detected. Running hemorrhage detection...")
                bleed_output = case_output / "bleed"
                bleed_output.mkdir(exist_ok=True)

                bleed_start = time.time()
                run_segmentation(nifti_path, bleed_output, "cerebral_bleed",
                                 fast=False, logger=logger)
                timings["hemorrhage_detection"] = round(time.time() - bleed_start, 1)
                logger.print("[Conditional] ✓ Hemorrhage detection complete")
        except Exception as e:
            logger.print(f"[Conditional] Error: {e}")

    # ============================================================
    # STEP 2: Metrics Calculation
    # ============================================================
    logger.print("\n[Metrics] Calculating volumes and densities...")
    metrics = {}
    try:
        metrics_start = time.time()
        json_path = case_output / "resultados.json"
        metrics = calculate_all_metrics(case_id, nifti_path, case_output)
        with open(json_path, "w") as f:
            json.dump(metrics, f, indent=2)
        timings["metrics"] = round(time.time() - metrics_start, 1)
        logger.print("[Metrics] ✓ Saved to resultados.json")

        # Update database with calculation results
        _update_db_results(case_output, json_path, metrics, logger)

    except Exception as e:
        logger.print(f"Error calculating metrics for {case_id}: {e}")

        # Write error log
        with open(case_output / "error.log", "w") as f:
            f.write(str(e))

        # Move NIfTI to error directory
        error_dest = ERROR_DIR / nifti_path.name
        try:
            shutil.move(str(nifti_path), str(error_dest))
            logger.print(f"Input moved to error folder: {error_dest}")
        except Exception as move_err:
            logger.print(f"Critical error: Could not move error file: {move_err}")

        logger.close()
        raise HTTPException(status_code=500, detail=f"Metrics calculation failed: {e}")

    # ============================================================
    # STEP 3: Update Pipeline Timing
    # ============================================================
    _update_pipeline_timing(case_output, logger)

    # ============================================================
    # STEP 4: Archive NIfTI File
    # ============================================================
    try:
        final_name = case_id
        try:
            with open(case_output / "id.json", 'r') as f:
                idd = json.load(f)
                if "ClinicalName" in idd and idd["ClinicalName"] and idd["ClinicalName"] != "Unknown":
                    final_name = idd["ClinicalName"]
        except:
            pass

        final_nii_path = NII_DIR / f"{final_name}.nii.gz"
        shutil.move(str(nifti_path), str(final_nii_path))
        logger.print(f"\n[Archive] ✓ Moved to nii/{final_name}.nii.gz")
    except Exception as e:
        logger.print(f"Error archiving input: {e}")

    # ============================================================
    # FINAL: Completion
    # ============================================================
    total_elapsed = sum(timings.values())
    timings["total"] = round(total_elapsed, 1)

    try:
        with open(case_output / "id.json", 'r') as f:
            meta = json.load(f)
            elapsed_str = meta.get("Pipeline", {}).get("elapsed_time", "Unknown")
            logger.print(f"\n✅ Case complete ({elapsed_str})")
    except:
        logger.print(f"\n✅ Case complete")

    logger.close()

    # Build metrics summary for response
    metrics_summary = {}
    if metrics:
        for key in ["body_regions", "hemorrhage_vol_cm3", "liver_vol_cm3",
                     "spleen_vol_cm3", "L3_SMA_cm2"]:
            if key in metrics:
                metrics_summary[key] = metrics[key]

    return {
        "status": "success",
        "case_id": case_id,
        "timings": timings,
        "metrics_summary": metrics_summary,
    }


# ============================================================
# DATABASE HELPER FUNCTIONS (from run.py)
# ============================================================

def _update_db_results(case_output: Path, json_path: Path, results_data: dict,
                       logger: PipelineLogger):
    """Update database with calculation results."""
    try:
        id_json_path = case_output / "id.json"
        study_uid = None
        if id_json_path.exists():
            with open(id_json_path, 'r') as f:
                id_data = json.load(f)
                study_uid = id_data.get("StudyInstanceUID")

        if study_uid:
            db_path = config.DB_PATH
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute(
                "UPDATE dicom_metadata SET CalculationResults = ? WHERE StudyInstanceUID = ?",
                (json.dumps(results_data), study_uid)
            )
            conn.commit()
            conn.close()
            logger.print("[Database] ✓ Updated calculation results")
        else:
            logger.print("[Database] ⚠️  Could not find StudyInstanceUID")

    except Exception as e:
        logger.print(f"  [Warning] Failed to update database with results: {e}")


def _update_pipeline_timing(case_output: Path, logger: PipelineLogger):
    """Record end time and elapsed time in id.json and database."""
    try:
        id_json_path = case_output / "id.json"
        if not id_json_path.exists():
            return

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

        # Update database with complete id.json
        try:
            study_uid = meta.get("StudyInstanceUID")
            if study_uid:
                db_path = config.DB_PATH
                conn = sqlite3.connect(db_path)
                c = conn.cursor()

                weight = meta.get("Weight")
                height = meta.get("Height")

                c.execute(
                    "UPDATE dicom_metadata SET IdJson = ?, Weight = ?, Height = ? WHERE StudyInstanceUID = ?",
                    (json.dumps(meta), weight, height, study_uid)
                )
                conn.commit()
                conn.close()
                logger.print("[Database] ✓ Updated id.json")

        except Exception as e:
            logger.print(f"  [Warning] Failed to update database with id.json: {e}")

    except Exception as e:
        logger.print(f"Error updating pipeline time: {e}")


# ============================================================
# API ENDPOINTS
# ============================================================

@app.post("/process", response_model=ProcessResponse)
async def process(
    file: UploadFile = File(..., description="NIfTI file (.nii.gz)"),
    case_id: str = Form(..., description="Patient case identifier (e.g. 'PatientRACS_20260201_5531196')")
):
    """
    Process a NIfTI file through the full TotalSegmentator pipeline.

    Sequential execution:
    1. General anatomy segmentation (total/total_mr)
    2. Tissue type segmentation (CT only)
    3. Conditional hemorrhage detection
    4. Metrics calculation
    5. Database updates
    6. NIfTI archival
    """
    if not state.ready:
        raise HTTPException(status_code=503, detail="TotalSegmentator not initialized")

    # Validate file
    if not file.filename.lower().endswith((".nii.gz", ".nii")):
        raise HTTPException(status_code=400, detail="Only NIfTI files (.nii.gz) are accepted")

    # Save uploaded file to a temporary location
    temp_dir = Path(tempfile.mkdtemp(prefix="ts_api_"))
    temp_nifti = temp_dir / file.filename

    try:
        content = await file.read()
        with open(temp_nifti, "wb") as f:
            f.write(content)
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise HTTPException(status_code=400, detail=f"Failed to save uploaded file: {e}")

    # Acquire lock — only one segmentation runs at a time
    async with state.lock:
        try:
            # Run the pipeline synchronously (GPU-bound, can't be truly async)
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, process_case, temp_nifti, case_id
            )
            return ProcessResponse(**result)

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Pipeline error: {e}")
        finally:
            # Cleanup temp directory
            shutil.rmtree(temp_dir, ignore_errors=True)


@app.get("/health")
def health_check():
    return {"status": "ok", "ready": state.ready}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("totalsegmentator_api:app", host="0.0.0.0", port=PORT)
