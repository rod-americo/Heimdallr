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
Heimdallr segmentation daemon.

Consumes prepared studies from the segmentation queue, selects the configured
pipeline profile, runs the enabled segmentation tasks, and forwards completed
cases to the metrics stage.
"""

import os
import json
import gzip
import shutil
import subprocess
import threading
import sys
import time
import concurrent.futures  # For parallel case segmentation
from pathlib import Path
import datetime
from zoneinfo import ZoneInfo

from heimdallr.shared import settings
from heimdallr.shared import store
from heimdallr.shared.paths import (
    study_artifacts_dir,
    study_derived_dir,
    study_dir,
    study_id_json,
    study_logs_dir,
    study_metadata_dir,
)
from heimdallr.shared.sqlite import connect as db_connect

settings.configure_service_stdio()

path_entries = [str(settings.TOTALSEG_BIN_DIR), str(Path(sys.executable).parent)]
os.environ["PATH"] = os.pathsep.join(path_entries + [os.environ["PATH"]])
LOCAL_TZ = ZoneInfo(settings.TIMEZONE)

# ============================================================
# CONFIGURATION
# ============================================================

# Use centralized configuration
TOTALSEGMENTATOR_BIN = settings.TOTALSEGMENTATOR_BIN
INPUT_DIR = settings.INPUT_DIR
SEGMENTATION_DIR = settings.SEGMENTATION_DIR
ERROR_DIR = settings.ERROR_DIR

# Create directories if they don't exist
settings.ensure_directories()


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
            self.log_file.write(f"Started: {settings.local_timestamp()}\n\n")
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
            self.log_file.write(f"\nFinished: {settings.local_timestamp()}\n")
            self.log_file.close()
            self.log_file = None


def _record_segmentation_pipeline_state(
    case_id: str,
    *,
    status: str,
    end_dt: datetime.datetime,
    error: str | None = None,
) -> None:
    id_json_path = study_id_json(case_id)
    if not id_json_path.exists():
        return

    with open(id_json_path, "r", encoding="utf-8") as handle:
        meta = json.load(handle)

    pipeline_data = meta.get("Pipeline", {})
    if not isinstance(pipeline_data, dict):
        pipeline_data = {}

    pipeline_data["segmentation_status"] = status
    pipeline_data["end_time"] = end_dt.isoformat()
    pipeline_data["segmentation_end_time"] = end_dt.isoformat()
    if error:
        pipeline_data["segmentation_error"] = error
    elif status != "error":
        pipeline_data.pop("segmentation_error", None)

    start_str = pipeline_data.get("start_time") or pipeline_data.get("segmentation_start_time")
    if start_str:
        try:
            start_dt = datetime.datetime.fromisoformat(start_str)
            elapsed_str = str(end_dt - start_dt)
            pipeline_data["elapsed_time"] = elapsed_str
            pipeline_data["segmentation_elapsed_time"] = elapsed_str
        except Exception:
            pipeline_data["elapsed_time"] = "Error parsing start_time"
            pipeline_data["segmentation_elapsed_time"] = "Error parsing start_time"
    elif status == "error":
        pipeline_data["elapsed_time"] = "Unknown start_time"
        pipeline_data["segmentation_elapsed_time"] = "Unknown start_time"

    prepare_elapsed_seconds = parse_elapsed_seconds(
        pipeline_data.get("prepare_elapsed_time")
    )
    segmentation_elapsed_seconds = parse_elapsed_seconds(
        pipeline_data.get("segmentation_elapsed_time")
    )
    if (
        prepare_elapsed_seconds is not None
        and segmentation_elapsed_seconds is not None
    ):
        pipeline_data["pipeline_active_elapsed_time"] = format_elapsed_seconds(
            prepare_elapsed_seconds + segmentation_elapsed_seconds
        )

    prepare_start_str = pipeline_data.get("prepare_start_time")
    if prepare_start_str:
        try:
            prepare_start_dt = datetime.datetime.fromisoformat(prepare_start_str)
            pipeline_data["pipeline_end_to_end_elapsed_time"] = str(
                end_dt - prepare_start_dt
            )
        except Exception:
            pipeline_data["pipeline_end_to_end_elapsed_time"] = (
                "Error parsing prepare_start_time"
            )

    meta["Pipeline"] = pipeline_data
    with open(id_json_path, "w", encoding="utf-8") as handle:
        json.dump(meta, handle, indent=2)

    study_uid = meta.get("StudyInstanceUID")
    if study_uid:
        conn = db_connect()
        try:
            store.update_id_json(conn, study_uid, meta)
        finally:
            conn.close()


def ensure_segmentation_queue_table():
    """Ensure segmentation queue table/index exist for immediate dispatch flow."""
    conn = db_connect()
    store.ensure_schema(conn)
    conn.close()


def claim_next_pending_segmentation_queue_item():
    """
    Atomically claim a pending queue item.

    Returns:
        tuple|None: (queue_id, case_id, input_path) or None when queue is empty.
    """
    conn = db_connect()
    try:
        return store.claim_next_pending_segmentation_queue_item(conn)
    finally:
        conn.close()


def parse_elapsed_seconds(elapsed_str):
    """Parse a Python timedelta string into seconds."""
    if not elapsed_str or ":" not in str(elapsed_str):
        return None
    try:
        hours, minutes, seconds = str(elapsed_str).split(":")
        return int(hours) * 3600 + int(minutes) * 60 + float(seconds)
    except Exception:
        return None


def format_elapsed_seconds(total_seconds):
    """Render seconds using the default timedelta string format."""
    return str(datetime.timedelta(seconds=round(float(total_seconds), 6)))


def mark_segmentation_queue_item_done(queue_id):
    """Mark queue item as done."""
    conn = db_connect()
    store.mark_segmentation_queue_item_done(conn, queue_id)
    conn.close()


def mark_segmentation_queue_item_error(queue_id, error_message):
    """Mark queue item as error with a truncated message."""
    conn = db_connect()
    store.mark_segmentation_queue_item_error(conn, queue_id, error_message)
    conn.close()


def enqueue_case_for_metrics(case_id, input_path):
    """Queue a processed study for post-segmentation metrics."""
    conn = db_connect()
    try:
        store.enqueue_case_for_metrics(conn, case_id, str(input_path))
    finally:
        conn.close()


def load_series_selection_profile() -> tuple[str, dict]:
    """Load the configured series-selection profile from JSON."""
    config_path = Path(settings.SERIES_SELECTION_CONFIG_PATH)
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    profiles = config.get("profiles", {})
    profile_name = settings.SERIES_SELECTION_PROFILE or config.get("default_profile")
    if not profile_name:
        raise RuntimeError(f"Series selection config has no default_profile: {config_path}")
    profile = profiles.get(profile_name)
    if not profile:
        raise RuntimeError(f"Series selection profile '{profile_name}' not found in {config_path}")
    return profile_name, profile


def load_segmentation_pipeline_profile() -> tuple[str, dict]:
    """Load the configured segmentation pipeline profile from JSON."""
    config_path = Path(settings.SEGMENTATION_PIPELINE_CONFIG_PATH)
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    profiles = config.get("profiles", {})
    profile_name = settings.SEGMENTATION_PIPELINE_PROFILE or config.get("default_profile")
    if not profile_name:
        raise RuntimeError(f"Segmentation pipeline config has no default_profile: {config_path}")
    profile = profiles.get(profile_name)
    if not profile:
        raise RuntimeError(f"Segmentation pipeline profile '{profile_name}' not found in {config_path}")
    return profile_name, profile


def resolve_segmentation_plan(modality, selected_phase) -> tuple[str, list[dict]]:
    """Resolve the active segmentation profile and enabled tasks for a case."""
    profile_name, profile = load_segmentation_pipeline_profile()
    required = profile.get("required", {})
    required_modality = required.get("modality")
    if required_modality and modality != required_modality:
        raise RuntimeError(
            f"Segmentation profile '{profile_name}' requires modality {required_modality}, got {modality}"
        )

    allowed_phases = [_normalize_phase(p) for p in required.get("selected_phase", [])]
    if allowed_phases and _normalize_phase(selected_phase) not in allowed_phases:
        raise RuntimeError(
            f"Segmentation profile '{profile_name}' requires phase in {allowed_phases}, got {_normalize_phase(selected_phase)}"
        )

    tasks = [task for task in profile.get("tasks", []) if task.get("enabled", True)]
    if not tasks:
        raise RuntimeError(f"Segmentation profile '{profile_name}' has no enabled tasks")
    return profile_name, tasks


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_phase(value):
    raw = str(value or "").strip().lower()
    return raw or "unknown"


def _text_tokens(series):
    description = str(series.get("SeriesDescription", "") or "").lower()
    kernel = str(series.get("ConvolutionKernel", "") or "").lower()
    return description, kernel


def _series_text_penalty(series, text_hints):
    description, kernel = _text_tokens(series)
    penalty = 0
    for token in text_hints.get("description_avoid", []):
        if token.lower() in description:
            penalty += 1
    for token in text_hints.get("kernel_avoid", []):
        if token.lower() in kernel:
            penalty += 1
    return penalty


def _series_hard_reject_reason(series, rules):
    description, kernel = _text_tokens(series)
    kernel_compact = kernel.strip()

    for token in rules.get("description_contains", []):
        normalized = str(token or "").strip().lower()
        if normalized and normalized in description:
            return f"description_rejected:{normalized}"

    for token in rules.get("kernel_contains", []):
        normalized = str(token or "").strip().lower()
        if normalized and normalized in kernel:
            return f"kernel_rejected_contains:{normalized}"

    for token in rules.get("kernel_exact", []):
        normalized = str(token or "").strip().lower()
        if normalized and kernel_compact == normalized:
            return f"kernel_rejected_exact:{normalized}"

    return None


def select_prepared_series(case_id, id_data):
    """
    Select the NIfTI series to process from a prepared study.

    Selection is driven by config/series_selection.json and recorded back into id.json.
    """
    available_series = list(id_data.get("AvailableSeries") or [])
    if not available_series:
        raise RuntimeError(f"No AvailableSeries found in study metadata for {case_id}")

    profile_name, profile = load_series_selection_profile()
    required = profile.get("required", {})
    hard_reject = profile.get("hard_reject", {})
    text_hints = profile.get("text_hints", {})
    phase_priority = [_normalize_phase(p) for p in profile.get("phase_priority", ["unknown"])]
    phase_rank = {phase: idx for idx, phase in enumerate(phase_priority)}
    derived_dir = study_derived_dir(case_id)

    candidates = []
    rejected = []
    for series in available_series:
        nifti_rel = series.get("DerivedNiftiPath")
        nifti_path = derived_dir / nifti_rel if nifti_rel else None
        if not nifti_path or not nifti_path.exists():
            rejected.append(
                {
                    "SeriesInstanceUID": series.get("SeriesInstanceUID"),
                    "SeriesNumber": series.get("SeriesNumber"),
                    "reason": "missing_nifti",
                }
            )
            continue

        modality = str(series.get("Modality", "") or "")
        required_modality = required.get("modality")
        if required_modality and modality != required_modality:
            rejected.append(
                {
                    "SeriesInstanceUID": series.get("SeriesInstanceUID"),
                    "SeriesNumber": series.get("SeriesNumber"),
                    "reason": f"modality_mismatch:{modality}",
                }
            )
            continue

        slice_count = _safe_int(series.get("SliceCount"))
        min_slices = _safe_int(required.get("min_slices"), 0)
        if slice_count < min_slices:
            rejected.append(
                {
                    "SeriesInstanceUID": series.get("SeriesInstanceUID"),
                    "SeriesNumber": series.get("SeriesNumber"),
                    "reason": f"below_min_slices:{slice_count}",
                }
            )
            continue

        hard_reject_reason = _series_hard_reject_reason(series, hard_reject)
        if hard_reject_reason:
            rejected.append(
                {
                    "SeriesInstanceUID": series.get("SeriesInstanceUID"),
                    "SeriesNumber": series.get("SeriesNumber"),
                    "reason": hard_reject_reason,
                }
            )
            continue

        phase = _normalize_phase(series.get("DetectedPhase"))
        if phase not in phase_rank:
            rejected.append(
                {
                    "SeriesInstanceUID": series.get("SeriesInstanceUID"),
                    "SeriesNumber": series.get("SeriesNumber"),
                    "reason": f"phase_not_allowed:{phase}",
                }
            )
            continue
        phase_data = series.get("PhaseData") or {}
        probability = _safe_float(phase_data.get("probability"))
        phase_detected = bool(series.get("PhaseDetected"))
        text_penalty = _series_text_penalty(series, text_hints)

        candidates.append(
            {
                "series": series,
                "path": nifti_path,
                "phase": phase,
                "phase_detected": phase_detected,
                "phase_probability": probability,
                "slice_count": slice_count,
                "text_penalty": text_penalty,
                "phase_rank": phase_rank.get(phase, len(phase_priority)),
            }
        )

    if not candidates:
        raise RuntimeError(
            f"No eligible series found for profile '{profile_name}' in study {case_id}. Rejected: {json.dumps(rejected, ensure_ascii=False)}"
        )

    candidates.sort(
        key=lambda c: (
            c["phase_rank"],
            0 if c["phase_detected"] else 1,
            -c["phase_probability"],
            -c["slice_count"],
            c["text_penalty"],
            str(c["series"].get("SeriesNumber", "")),
        )
    )
    selected = candidates[0]
    selected_series = dict(selected["series"])
    selection_info = {
        "Profile": profile_name,
        "SelectedSeriesInstanceUID": selected_series.get("SeriesInstanceUID"),
        "SelectedSeriesNumber": selected_series.get("SeriesNumber"),
        "SelectedSeriesDescription": selected_series.get("SeriesDescription", ""),
        "SelectedDerivedNiftiPath": str(selected["path"].relative_to(study_dir(case_id))),
        "SelectedPhase": selected["phase"],
        "PhaseDetected": selected["phase_detected"],
        "PhaseProbability": selected["phase_probability"],
        "SliceCount": selected["slice_count"],
        "SelectionReason": (
            f"phase={selected['phase']}, detected={selected['phase_detected']}, "
            f"probability={selected['phase_probability']}, slices={selected['slice_count']}"
        ),
        "RejectedSeries": rejected,
    }
    return selected["path"], selection_info


def materialize_canonical_nifti(source_path, final_path):
    """Persist a canonical case NIfTI without removing the selected source series."""
    final_path.parent.mkdir(parents=True, exist_ok=True)
    if final_path.exists():
        try:
            if final_path.samefile(source_path):
                return final_path
        except FileNotFoundError:
            pass
        final_path.unlink()

    try:
        os.link(source_path, final_path)
    except OSError:
        shutil.copy2(source_path, final_path)
    return final_path


def run_segmentation_pipeline(case_id, modality, selected_phase, nifti_path, case_output, artifacts_dir, log_dir, logger):
    """Execute the configured segmentation task list for the selected series."""
    profile_name, tasks = resolve_segmentation_plan(modality, selected_phase)

    logger.print(f"[Segmentation] Profile: {profile_name}")
    logger.print(f"[Segmentation] Selected phase: {_normalize_phase(selected_phase)}")
    logger.print(f"[Segmentation] Tasks: {', '.join(task['name'] for task in tasks)}")

    for task in tasks:
        task_name = task["name"]
        extra_args = list(task.get("extra_args", []))
        output_dir = case_output / task["output_dir"]
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        log_file = None if settings.VERBOSE_CONSOLE else log_dir / f"{task_name}.log"
        run_task(
            task_name,
            nifti_path,
            output_dir,
            extra_args=extra_args,
            log_file=log_file,
        )

    return {
        "profile": profile_name,
        "tasks": [
            {
                "name": task["name"],
                "output_dir": task["output_dir"],
                "extra_args": list(task.get("extra_args", [])),
                "license_required": bool(task.get("license_required")),
            }
            for task in tasks
        ],
    }


def _segmentation_outputs_exist(case_output: Path, tasks: list[dict]) -> bool:
    for task in tasks:
        output_dir = case_output / task["output_dir"]
        if not output_dir.exists():
            return False
        files = [path for path in output_dir.iterdir() if path.is_file()]
        if not files:
            return False
        for path in files:
            if path.suffixes[-2:] == [".nii", ".gz"] and not _gzip_nifti_is_readable(path):
                return False
    return True


def _gzip_nifti_is_readable(path: Path) -> bool:
    try:
        with gzip.open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                if not chunk:
                    break
        return True
    except (OSError, EOFError):
        return False


def should_reuse_existing_segmentation(
    study_uid: str | None,
    case_output: Path,
    selection_info: dict | None,
    profile_name: str,
    tasks: list[dict],
) -> bool:
    if not study_uid or not selection_info:
        return False

    selected_series_uid = selection_info.get("SelectedSeriesInstanceUID")
    selected_slice_count = _safe_int(selection_info.get("SliceCount"), -1)
    planned_task_names = [task["name"] for task in tasks]

    conn = db_connect()
    try:
        row = store.get_recorded_segmentation_signature(conn, study_uid)
    finally:
        conn.close()

    if not row or not row["SegmentationCompletedAt"]:
        return False
    if row["SegmentationSeriesInstanceUID"] != selected_series_uid:
        return False
    if _safe_int(row["SegmentationSliceCount"], -1) != selected_slice_count:
        return False
    if row["SegmentationProfile"] != profile_name:
        return False

    try:
        recorded_tasks = json.loads(row["SegmentationTasks"] or "[]")
    except Exception:
        recorded_tasks = []
    if recorded_tasks != planned_task_names:
        return False

    return _segmentation_outputs_exist(case_output, tasks)

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
        TOTALSEGMENTATOR_BIN,
        "-i", str(input_file),
        "-o", str(output_folder),
        "--task", task_name
    ] + extra_args
    
    # Open log file if specified
    log_handle = None
    if log_file:
        log_handle = open(log_file, 'w')
        log_handle.write(f"=== TotalSegmentator Task: {task_name} ===\n")
        log_handle.write(f"Started: {settings.local_timestamp()}\n")
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
                log_handle.write(f"\nFinished: {settings.local_timestamp()}\n")
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
        min_age_seconds = max(5, settings.SEGMENTATION_SCAN_INTERVAL * 2)
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
    Atomically move a case from input/ to segmentation/ so restarts do not resubmit it.
    """
    segmentation_path = SEGMENTATION_DIR / input_path.name
    if segmentation_path.exists():
        raise FileExistsError(f"Case already exists in segmentation/: {segmentation_path.name}")
    shutil.move(str(input_path), str(segmentation_path))
    return segmentation_path


def segment_case(case_input):
    """
    Process a single patient case through the complete pipeline.
    
    Steps:
    1. Parallel segmentation (organs + tissues)
    2. Conditional specialized analysis (e.g., hemorrhage if brain found)
    3. Metrics calculation and JSON output
    4. Update segmentation timestamps
    5. Archive NIfTI file
    
    Args:
        case_input: Path to a queued study directory or a legacy NIfTI file
    
    Returns:
        bool: True if successful, False on error
    """
    case_input = Path(case_input)
    selected_from_prepared_study = case_input.is_dir()
    selection_info = None

    if selected_from_prepared_study:
        case_id = case_input.name
    else:
        case_id = case_input.name.replace("".join(case_input.suffixes), "")
    case_output = study_dir(case_id)
    artifacts_dir = study_artifacts_dir(case_id)
    derived_dir = study_derived_dir(case_id)
    log_dir = study_logs_dir(case_id)
    study_metadata_dir(case_id).mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    derived_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    pipeline_log_path = None if settings.VERBOSE_CONSOLE else log_dir / "pipeline.log"
    error_log_path = log_dir / "error.log"
    logger = PipelineLogger(pipeline_log_path)

    nifti_path = None
    if selected_from_prepared_study:
        id_json_path = study_id_json(case_id)
        if not id_json_path.exists():
            logger.print(f"Skipping prepared study without id.json: {case_input}")
            logger.close()
            return False
        try:
            with open(id_json_path, "r", encoding="utf-8") as f:
                id_data = json.load(f)
            nifti_path, selection_info = select_prepared_series(case_id, id_data)
        except Exception as e:
            logger.print(f"Failed to select series for case {case_id}: {e}")
            logger.close()
            return False
    else:
        try:
            nifti_path = claim_input_file(case_input)
        except FileNotFoundError:
            logger.print(f"Skipping case because input disappeared before claim: {case_input.name}")
            logger.close()
            return False
        except Exception as e:
            logger.print(f"Failed to claim input file {case_input.name}: {e}")
            logger.close()
            return False

    try:
        try:
            if error_log_path.exists():
                error_log_path.unlink()
        except Exception:
            pass

        segmentation_start_dt = datetime.datetime.now(LOCAL_TZ)
        logger.print(f"\n=== Segmentation Case: {case_id} ===")

        modality = "CT"
        selected_phase = "unknown"
        study_uid = None
        id_json_path = study_id_json(case_id)
        if id_json_path.exists():
            try:
                with open(id_json_path, "r") as f:
                    meta = json.load(f)
                modality = meta.get("Modality", "CT")
                study_uid = meta.get("StudyInstanceUID")
                if selection_info is not None:
                    selected_phase = selection_info.get("SelectedPhase", "unknown")

                pipeline_data = meta.get("Pipeline", {})
                pipeline_data["start_time"] = segmentation_start_dt.isoformat()
                pipeline_data["segmentation_start_time"] = segmentation_start_dt.isoformat()
                if selection_info is not None:
                    pipeline_data["series_selection"] = selection_info
                meta["Pipeline"] = pipeline_data

                with open(id_json_path, "w") as f:
                    json.dump(meta, f, indent=2)
            except Exception:
                pass
        logger.print(f"Detected modality: {modality}")
        if selection_info is not None:
            logger.print(
                f"Selected series {selection_info['SelectedSeriesNumber']} "
                f"({selection_info['SelectedPhase']}, {selection_info['SliceCount']} slices)"
            )
        profile_name, planned_tasks = resolve_segmentation_plan(modality, selected_phase)
        seg_start_time = time.time()
        if should_reuse_existing_segmentation(study_uid, case_output, selection_info, profile_name, planned_tasks):
            segmentation_info = {
                "profile": profile_name,
                "tasks": [
                    {
                        "name": task["name"],
                        "output_dir": task["output_dir"],
                        "extra_args": list(task.get("extra_args", [])),
                        "license_required": bool(task.get("license_required")),
                    }
                    for task in planned_tasks
                ],
                "reused_existing_outputs": True,
                "reuse_reason": "sqlite_signature_match",
            }
            seg_elapsed = time.time() - seg_start_time
            logger.print("[Segmentation] Reusing existing outputs for identical selected series signature")
            logger.print(f"[Segmentation] ✓ Complete ({seg_elapsed:.1f}s)")
        else:
            segmentation_info = run_segmentation_pipeline(
                case_id=case_id,
                modality=modality,
                selected_phase=selected_phase,
                nifti_path=nifti_path,
                case_output=case_output,
                artifacts_dir=artifacts_dir,
                log_dir=log_dir,
                logger=logger,
            )
            seg_elapsed = time.time() - seg_start_time
            logger.print(f"[Segmentation] ✓ Complete ({seg_elapsed:.1f}s)")
            if study_uid and selection_info is not None:
                conn = db_connect()
                try:
                    store.update_segmentation_signature(
                        conn,
                        study_uid,
                        series_instance_uid=selection_info.get("SelectedSeriesInstanceUID"),
                        slice_count=_safe_int(selection_info.get("SliceCount"), 0),
                        profile_name=segmentation_info["profile"],
                        task_names=[task["name"] for task in segmentation_info["tasks"]],
                    )
                finally:
                    conn.close()
        if not settings.VERBOSE_CONSOLE:
            logger.print(f"  → Logs: {log_dir.relative_to(settings.STUDIES_DIR.parent)}/")

        try:
            id_json_path = study_id_json(case_id)
            if id_json_path.exists():
                with open(id_json_path, "r") as f:
                    meta = json.load(f)

                pipeline_data = meta.get("Pipeline", {})
                start_str = pipeline_data.get("start_time")
                end_dt = datetime.datetime.now(LOCAL_TZ)
                pipeline_data["end_time"] = end_dt.isoformat()
                pipeline_data["segmentation_end_time"] = end_dt.isoformat()
                pipeline_data["segmentation_status"] = "done"
                pipeline_data.pop("segmentation_error", None)

                if start_str:
                    try:
                        start_dt = datetime.datetime.fromisoformat(start_str)
                        elapsed_str = str(end_dt - start_dt)
                        pipeline_data["elapsed_time"] = elapsed_str
                        pipeline_data["segmentation_elapsed_time"] = elapsed_str
                    except Exception:
                        pipeline_data["elapsed_time"] = "Error parsing start_time"
                        pipeline_data["segmentation_elapsed_time"] = "Error parsing start_time"
                else:
                    pipeline_data["elapsed_time"] = "Unknown start_time"
                    pipeline_data["segmentation_elapsed_time"] = "Unknown start_time"

                prepare_elapsed_seconds = parse_elapsed_seconds(
                    pipeline_data.get("prepare_elapsed_time")
                )
                segmentation_elapsed_seconds = parse_elapsed_seconds(
                    pipeline_data.get("segmentation_elapsed_time")
                )
                if (
                    prepare_elapsed_seconds is not None
                    and segmentation_elapsed_seconds is not None
                ):
                    pipeline_data["pipeline_active_elapsed_time"] = format_elapsed_seconds(
                        prepare_elapsed_seconds + segmentation_elapsed_seconds
                    )

                prepare_start_str = pipeline_data.get("prepare_start_time")
                if prepare_start_str:
                    try:
                        prepare_start_dt = datetime.datetime.fromisoformat(prepare_start_str)
                        pipeline_data["pipeline_end_to_end_elapsed_time"] = str(
                            end_dt - prepare_start_dt
                        )
                    except Exception:
                        pipeline_data["pipeline_end_to_end_elapsed_time"] = (
                            "Error parsing prepare_start_time"
                        )

                pipeline_data["segmentation_pipeline"] = segmentation_info

                meta["Pipeline"] = pipeline_data
                with open(id_json_path, "w") as f:
                    json.dump(meta, f, indent=2)

                try:
                    study_uid = meta.get("StudyInstanceUID")
                    if study_uid:
                        conn = db_connect()
                        store.update_id_json(conn, study_uid, meta)
                        conn.close()
                        if not settings.VERBOSE_CONSOLE:
                            logger.print("[Database] ✓ Updated id.json")
                        else:
                            logger.print(f"  [DB] id.json updated for {study_uid}")
                except Exception as e:
                    logger.print(f"  [Warning] Failed to update database with id.json: {e}")
        except Exception as e:
            logger.print(f"Error updating pipeline time: {e}")

        try:
            final_nii_path = derived_dir / f"{case_id}.nii.gz"
            if nifti_path.exists():
                if selected_from_prepared_study:
                    materialize_canonical_nifti(nifti_path, final_nii_path)
                else:
                    if final_nii_path.exists():
                        try:
                            same_file = nifti_path.samefile(final_nii_path)
                        except FileNotFoundError:
                            same_file = False
                        if same_file:
                            nifti_path.unlink()
                        else:
                            if final_nii_path.exists():
                                final_nii_path.unlink()
                            shutil.move(str(nifti_path), str(final_nii_path))
                    else:
                        shutil.move(str(nifti_path), str(final_nii_path))

            if not settings.VERBOSE_CONSOLE:
                logger.print(f"\n[Archive] ✓ Canonical NIfTI in {final_nii_path.relative_to(settings.STUDIES_DIR.parent)}")
            else:
                logger.print(f"Canonical NIfTI: {final_nii_path}")
        except Exception as e:
            logger.print(f"Error finalizing NIfTI: {e}")

        if not settings.VERBOSE_CONSOLE:
            try:
                with open(study_id_json(case_id), "r") as f:
                    meta = json.load(f)
                    elapsed_str = meta.get("Pipeline", {}).get("elapsed_time", "Unknown")
                    logger.print(f"\n✅ Case complete ({elapsed_str})")
            except Exception:
                logger.print("\n✅ Case complete")

        try:
            if error_log_path.exists():
                error_log_path.unlink()
        except Exception:
            pass

        try:
            enqueue_case_for_metrics(case_id, study_dir(case_id))
            logger.print("[Metrics] Enqueued for metrics stage")
        except Exception as e:
            logger.print(f"[Metrics] Warning: failed to enqueue metrics stage: {e}")

        logger.close()
        return True

    except Exception as e:
        try:
            _record_segmentation_pipeline_state(
                case_id,
                status="error",
                end_dt=datetime.datetime.now(LOCAL_TZ),
                error=str(e),
            )
        except Exception:
            pass
        logger.print(f"Unhandled segmentation error for {case_id}: {e}")
        try:
            with open(error_log_path, "w") as f:
                f.write(str(e))
        except Exception:
            pass
        try:
            if nifti_path.exists() and not selected_from_prepared_study:
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
    ensure_segmentation_queue_table()
    
    max_cases = settings.MAX_PARALLEL_CASES  # Maximum concurrent cases from config
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_cases)
    
    segmentation_files = set()  # Track files currently being processed
    lock = threading.Lock()    # Thread-safe access to segmentation_files
    
    def on_complete(fut, f_path, queue_id=None):
        """Callback when a case finishes segmentation."""
        with lock:
            if f_path in segmentation_files:
                segmentation_files.discard(f_path)
        try:
            ok = fut.result()  # Raise exception if case failed
            print(f"[Segmentation] {'Done' if ok else 'Failed'}: {f_path.name}")
            if queue_id is not None:
                if ok:
                    mark_segmentation_queue_item_done(queue_id)
                else:
                    mark_segmentation_queue_item_error(queue_id, "Case finished with failure return status")
        except Exception as e:
            if queue_id is not None:
                mark_segmentation_queue_item_error(queue_id, e)
            print(f"Error in case segmentation thread {f_path.name}: {e}")

    try:
        while True:
            try:
                # Priority path: consume explicit queue signals first.
                while True:
                    with lock:
                        if len(segmentation_files) >= max_cases:
                            break
                    queue_item = claim_next_pending_segmentation_queue_item()
                    if not queue_item:
                        break

                    queue_id, _, input_path_str = queue_item
                    input_path = Path(input_path_str)
                    if not input_path.is_absolute():
                        input_path = INPUT_DIR / input_path.name

                    if not input_path.exists():
                        mark_segmentation_queue_item_error(queue_id, f"Input file not found: {input_path}")
                        continue

                    with lock:
                        if input_path in segmentation_files:
                            mark_segmentation_queue_item_error(queue_id, f"Input file already in segmentation set: {input_path.name}")
                            continue
                        print(f"[Segmentation] Claimed queue item: {input_path.name}")
                        segmentation_files.add(input_path)
                        future = executor.submit(segment_case, input_path)
                        future.add_done_callback(lambda fut, p=input_path, qid=queue_id: on_complete(fut, p, qid))

                # List all NIfTI files in input directory
                current_files = sorted(list(INPUT_DIR.glob("*.nii.gz")))
                
                for f in current_files:
                    with lock:
                        # If we're at max capacity, wait until next iteration
                        if len(segmentation_files) >= max_cases:
                            break
                        
                        # Skip if file is already being processed
                        if f in segmentation_files:
                            continue

                        # Skip files that may still be mid-copy.
                        if not is_file_stable(f):
                            continue

                        # Skip files that already have a claimed twin in segmentation/.
                        if (SEGMENTATION_DIR / f.name).exists():
                            continue
                            
                        # Submit new case for segmentation
                        print(f"[Segmentation] Claimed input file: {f.name}")
                        segmentation_files.add(f)
                        future = executor.submit(segment_case, f)
                        future.add_done_callback(lambda fut, p=f: on_complete(fut, p, None))
            
                time.sleep(settings.SEGMENTATION_SCAN_INTERVAL)
                
            except Exception as e:
                print(f"Error in main loop: {e}")
                time.sleep(settings.SEGMENTATION_SCAN_INTERVAL)
                
    except KeyboardInterrupt:
        print("\nStopping monitoring...")
        executor.shutdown(wait=False)
        print("Executor shutdown complete.")

if __name__ == "__main__":
    main()
