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
import signal
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
from heimdallr.shared.segmentation_coverage import classify_segmentation_coverage
from heimdallr.shared.sqlite import connect as db_connect

settings.configure_service_stdio()

path_entries = [str(settings.TOTALSEG_BIN_DIR), str(Path(sys.executable).parent)]
os.environ["PATH"] = os.pathsep.join(path_entries + [os.environ["PATH"]])
LOCAL_TZ = ZoneInfo(settings.TIMEZONE)
_ACTIVE_CHILD_PROCESSES: set[subprocess.Popen[str]] = set()
_ACTIVE_CHILDREN_LOCK = threading.Lock()
_SHUTDOWN_EVENT = threading.Event()

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


def recover_claimed_segmentation_queue_items() -> int:
    """Recover claimed queue rows after a worker restart."""
    conn = db_connect()
    try:
        return store.reset_claimed_segmentation_queue_items(conn)
    finally:
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


def touch_segmentation_queue_item_claim(queue_id: int) -> bool:
    conn = db_connect()
    try:
        return store.touch_segmentation_queue_item_claim(conn, queue_id)
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


def _latest_output_activity_timestamp(output_folder: Path) -> float:
    if not output_folder.exists():
        return 0.0
    latest = output_folder.stat().st_mtime
    for path in output_folder.iterdir():
        try:
            latest = max(latest, path.stat().st_mtime)
        except FileNotFoundError:
            continue
    return latest


def _register_child_process(process: subprocess.Popen[str]) -> None:
    with _ACTIVE_CHILDREN_LOCK:
        _ACTIVE_CHILD_PROCESSES.add(process)


def _unregister_child_process(process: subprocess.Popen[str]) -> None:
    with _ACTIVE_CHILDREN_LOCK:
        _ACTIVE_CHILD_PROCESSES.discard(process)


def _terminate_process_group(process: subprocess.Popen[str], *, reason: str, kill_after_seconds: float = 5.0) -> None:
    if process.poll() is not None:
        return

    try:
        pgid = os.getpgid(process.pid)
    except ProcessLookupError:
        return

    print(f"[Segmentation] Terminating child process group {pgid}: {reason}")
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return

    deadline = time.monotonic() + max(kill_after_seconds, 0.5)
    while process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.2)

    if process.poll() is not None:
        return

    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        return

    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


def _terminate_registered_child_processes(*, reason: str) -> None:
    with _ACTIVE_CHILDREN_LOCK:
        processes = list(_ACTIVE_CHILD_PROCESSES)
    for process in processes:
        _terminate_process_group(process, reason=reason)


def _install_signal_handlers() -> None:
    def _handle_signal(signum, _frame):
        if _SHUTDOWN_EVENT.is_set():
            return
        _SHUTDOWN_EVENT.set()
        print(f"[Segmentation] Received signal {signum}; stopping active child processes")
        _terminate_registered_child_processes(reason=f"worker signal {signum}")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)


def _start_claim_heartbeat(queue_id: int, *, case_label: str) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()

    def _heartbeat() -> None:
        while not stop_event.wait(settings.SEGMENTATION_CLAIM_HEARTBEAT_SECONDS):
            try:
                if not touch_segmentation_queue_item_claim(queue_id):
                    print(
                        f"[Segmentation] Claim heartbeat lost for queue item {queue_id} ({case_label})"
                    )
                    return
            except Exception as exc:
                print(
                    f"[Segmentation] Claim heartbeat error for queue item {queue_id} ({case_label}): {exc}"
                )

    thread = threading.Thread(
        target=_heartbeat,
        name=f"segmentation-heartbeat-{queue_id}",
        daemon=True,
    )
    thread.start()
    return stop_event, thread


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

    allowed_phases = _expand_allowed_phases_with_portal_fallback(required.get("selected_phase", []))
    normalized_selected_phase = _normalize_phase(selected_phase)
    if allowed_phases and not _phase_allowed_with_fallback(allowed_phases, normalized_selected_phase):
        raise RuntimeError(
            f"Segmentation profile '{profile_name}' requires phase in {allowed_phases}, got {normalized_selected_phase}"
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


def _is_contrast_phase(value) -> bool:
    phase = _normalize_phase(value)
    return any(token in phase for token in ("arterial", "venous", "portal", "delayed", "contrast", "enhanced", "post"))


def _expand_allowed_phases_with_portal_fallback(phases):
    allowed = [_normalize_phase(item) for item in phases]
    if "native" in allowed and "portal_venous" not in allowed:
        allowed.append("portal_venous")
    return allowed


def _phase_allowed_with_fallback(allowed_phases, selected_phase) -> bool:
    normalized_selected_phase = _normalize_phase(selected_phase)
    if normalized_selected_phase in allowed_phases:
        return True
    return "native" in allowed_phases and _is_contrast_phase(normalized_selected_phase)


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


def _series_region_hint(series: dict) -> str:
    description = str(series.get("SeriesDescription", "") or "").lower()
    if any(token in description for token in ("abdome", "abdomen", "abdominal")):
        return "abdomen"
    if any(token in description for token in ("torax", "tórax", "thorax", "chest", "pulmao", "pulmão", "mediast")):
        return "chest"
    return "unknown"


def _choose_follow_up_candidate(
    selected: dict,
    candidates: list[dict],
    *,
    preferred_region: str,
    previous_series_uid: str | None,
) -> dict:
    selected_uid = str(selected["series"].get("SeriesInstanceUID") or "")
    previous_uid = str(previous_series_uid or "")
    if not previous_uid or selected_uid != previous_uid:
        return selected

    alternative_candidates = [
        candidate
        for candidate in candidates
        if str(candidate["series"].get("SeriesInstanceUID") or "") != previous_uid
    ]
    if not alternative_candidates:
        return selected

    preferred_candidates = [
        candidate
        for candidate in alternative_candidates
        if _series_region_hint(candidate["series"]) == preferred_region
    ]
    if preferred_candidates:
        return preferred_candidates[0]
    return alternative_candidates[0]


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
    follow_up_coverage = profile.get("follow_up_coverage", {})
    phase_priority = [_normalize_phase(p) for p in profile.get("phase_priority", ["unknown"])]
    phase_rank = {phase: idx for idx, phase in enumerate(phase_priority)}
    contrast_fallback_rank = len(phase_priority)
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
        fallback_reason = None
        if phase in phase_rank:
            resolved_phase_rank = phase_rank.get(phase, len(phase_priority))
        elif "native" in phase_priority and _is_contrast_phase(phase):
            resolved_phase_rank = contrast_fallback_rank
            fallback_reason = "contrast_fallback"
        else:
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
                "phase_rank": resolved_phase_rank,
                "fallback_reason": fallback_reason,
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
    recorded = None
    study_uid = str(id_data.get("StudyInstanceUID", "") or "").strip()
    if study_uid:
        conn = db_connect()
        try:
            recorded = store.get_recorded_segmentation_signature(conn, study_uid)
        finally:
            conn.close()
        if (
            bool(follow_up_coverage.get("enabled"))
            and recorded
            and str(recorded["SegmentationCoverageClass"] or "")
            in {
                str(item or "").strip()
                for item in (follow_up_coverage.get("when_previous_coverage") or [])
                if str(item or "").strip()
            }
        ):
            follow_up_selected = _choose_follow_up_candidate(
                selected,
                candidates,
                preferred_region=str(follow_up_coverage.get("prefer_region", "") or "abdomen").strip() or "abdomen",
                previous_series_uid=(
                    recorded["SegmentationSeriesInstanceUID"]
                    if bool(follow_up_coverage.get("require_different_series", True))
                    else None
                ),
            )
            if follow_up_selected is not selected:
                selected = follow_up_selected
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
            + (
                f", fallback={selected['fallback_reason']}"
                if selected.get("fallback_reason")
                else ""
            )
            + (
                f", follow_up_policy={str(follow_up_coverage.get('prefer_region', '') or 'abdomen')}"
                f", follow_up_after={recorded['SegmentationCoverageClass']}"
                if study_uid
                and recorded
                and bool(follow_up_coverage.get("enabled"))
                and str(recorded["SegmentationCoverageClass"] or "")
                in {
                    str(item or "").strip()
                    for item in (follow_up_coverage.get("when_previous_coverage") or [])
                    if str(item or "").strip()
                }
                and str(selected_series.get("SeriesInstanceUID") or "")
                != str(recorded["SegmentationSeriesInstanceUID"] or "")
                else ""
            )
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
) -> tuple[bool, str | None]:
    if not study_uid or not selection_info:
        return False, None

    selected_series_uid = selection_info.get("SelectedSeriesInstanceUID")
    selected_slice_count = _safe_int(selection_info.get("SliceCount"), -1)
    planned_task_names = [task["name"] for task in tasks]

    conn = db_connect()
    try:
        row = store.get_recorded_segmentation_signature(conn, study_uid)
    finally:
        conn.close()

    if not row or not row["SegmentationCompletedAt"]:
        return False, None
    if row["SegmentationSeriesInstanceUID"] != selected_series_uid:
        return False, None
    if _safe_int(row["SegmentationSliceCount"], -1) != selected_slice_count:
        return False, None
    if row["SegmentationProfile"] != profile_name:
        return False, None

    try:
        recorded_tasks = json.loads(row["SegmentationTasks"] or "[]")
    except Exception:
        recorded_tasks = []
    if recorded_tasks != planned_task_names:
        return False, None

    outputs_exist = _segmentation_outputs_exist(case_output, tasks)
    if not outputs_exist:
        return False, None
    return True, str(row["SegmentationElapsedTime"] or "") or None

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
    
    no_progress_timeout = max(1, settings.SEGMENTATION_NO_PROGRESS_TIMEOUT_SECONDS)

    def _log_message(message: str) -> None:
        if log_handle:
            log_handle.write(message + "\n")
            log_handle.flush()
        print(message)

    try:
        # Retry loop to handle transient race conditions on TotalSegmentator config.json
        # We don't recreate the file as it contains important state (prediction_counter, license, etc.)
        for attempt in range(max_retries):
            try:
                output_lines: list[str] = []
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    start_new_session=True,
                )
                _register_child_process(process)
                progress_lock = threading.Lock()
                last_stdout_progress = time.monotonic()
                last_artifact_mtime = _latest_output_activity_timestamp(output_folder)
                last_progress_at = time.monotonic()

                def _stream_output() -> None:
                    nonlocal last_stdout_progress, last_progress_at
                    assert process.stdout is not None
                    try:
                        for line in process.stdout:
                            output_lines.append(line)
                            with progress_lock:
                                last_stdout_progress = time.monotonic()
                                last_progress_at = last_stdout_progress
                            if log_handle:
                                log_handle.write(line)
                                log_handle.flush()
                            else:
                                print(line, end="")
                    finally:
                        process.stdout.close()

                reader = threading.Thread(
                    target=_stream_output,
                    name=f"totalseg-output-{task_name}",
                    daemon=True,
                )
                reader.start()

                timed_out_message = None
                try:
                    while process.poll() is None:
                        current_artifact_mtime = _latest_output_activity_timestamp(output_folder)
                        with progress_lock:
                            if current_artifact_mtime > last_artifact_mtime:
                                last_artifact_mtime = current_artifact_mtime
                                last_progress_at = time.monotonic()
                            idle_seconds = time.monotonic() - max(last_progress_at, last_stdout_progress)

                        if _SHUTDOWN_EVENT.wait(1.0):
                            timed_out_message = (
                                f"[{task_name}] Worker shutdown requested while task was still running"
                            )
                            _terminate_process_group(process, reason=timed_out_message)
                            break

                        if idle_seconds >= no_progress_timeout:
                            timed_out_message = (
                                f"[{task_name}] No stdout or artifact progress for "
                                f"{no_progress_timeout}s; aborting task"
                            )
                            _log_message(timed_out_message)
                            _terminate_process_group(process, reason=timed_out_message)
                            break

                    process.wait(timeout=5)
                    reader.join(timeout=5)
                finally:
                    _unregister_child_process(process)

                if timed_out_message is not None:
                    raise TimeoutError(timed_out_message)

                if process.returncode != 0:
                    full_output = "".join(output_lines)
                    if (
                        "JSONDecodeError" in full_output
                        and "config.json" in full_output
                        and attempt < max_retries - 1
                    ):
                        wait_time = (2 ** attempt) * 0.5
                        msg = (
                            f"[{task_name}] Config race condition detected. Retrying in "
                            f"{wait_time}s... (attempt {attempt + 1}/{max_retries})"
                        )
                        _log_message(msg)
                        time.sleep(wait_time)
                        continue
                    if log_handle:
                        log_handle.write(f"\nFailed with exit code: {process.returncode}\n")
                        log_handle.flush()
                    raise subprocess.CalledProcessError(process.returncode, cmd)

                if log_handle:
                    log_handle.write(f"\nFinished: {settings.local_timestamp()}\n")
                    log_handle.write("Exit code: 0\n")
                    log_handle.flush()
                else:
                    print(f"[{task_name}] Finished.")
                return
            except subprocess.CalledProcessError:
                raise
            except TimeoutError:
                raise
            except Exception as exc:
                if attempt >= max_retries - 1:
                    raise
                msg = (
                    f"[{task_name}] Unexpected error: {exc}. Retrying... "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                _log_message(msg)
                time.sleep(2 ** attempt)

    finally:
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
            if case_input.parent == SEGMENTATION_DIR and case_input.exists():
                nifti_path = case_input
            else:
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
        should_reuse, recorded_elapsed_time = should_reuse_existing_segmentation(
            study_uid,
            case_output,
            selection_info,
            profile_name,
            planned_tasks,
        )
        if should_reuse:
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
            if recorded_elapsed_time:
                segmentation_info["original_elapsed_time"] = recorded_elapsed_time
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
                coverage_class = classify_segmentation_coverage(artifacts_dir / "total")
                conn = db_connect()
                try:
                    store.update_segmentation_signature(
                        conn,
                        study_uid,
                        series_instance_uid=selection_info.get("SelectedSeriesInstanceUID"),
                        slice_count=_safe_int(selection_info.get("SliceCount"), 0),
                        profile_name=segmentation_info["profile"],
                        task_names=[task["name"] for task in segmentation_info["tasks"]],
                        elapsed_time=str(datetime.timedelta(seconds=round(seg_elapsed, 6))),
                        coverage_class=coverage_class,
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
                original_elapsed_time = segmentation_info.get("original_elapsed_time")
                if segmentation_info.get("reused_existing_outputs") and original_elapsed_time:
                    pipeline_data["segmentation_original_elapsed_time"] = str(original_elapsed_time)
                elif not segmentation_info.get("reused_existing_outputs"):
                    pipeline_data["segmentation_original_elapsed_time"] = elapsed_str

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
    _install_signal_handlers()
    ensure_segmentation_queue_table()
    recovered_claims = recover_claimed_segmentation_queue_items()
    if recovered_claims:
        print(f"[Segmentation] Recovered {recovered_claims} claimed queue item(s) on startup")
    
    max_cases = settings.MAX_PARALLEL_CASES  # Maximum concurrent cases from config
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_cases)
    
    active_futures: set[concurrent.futures.Future] = set()
    lock = threading.Lock()

    def _active_case_count() -> int:
        with lock:
            return len(active_futures)

    def _submit_case(case_path: Path, *, queue_id: int | None = None) -> None:
        if queue_id is None:
            future = executor.submit(segment_case, case_path)
        else:
            future = executor.submit(_segment_case_with_heartbeat, case_path, queue_id)
        with lock:
            active_futures.add(future)
        future.add_done_callback(
            lambda fut, p=case_path, qid=queue_id: on_complete(fut, p, qid)
        )
    
    def on_complete(fut, f_path, queue_id=None):
        """Callback when a case finishes segmentation."""
        with lock:
            active_futures.discard(fut)
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
        while not _SHUTDOWN_EVENT.is_set():
            try:
                # Priority path: consume explicit queue signals first.
                while _active_case_count() < max_cases and not _SHUTDOWN_EVENT.is_set():
                    queue_item = claim_next_pending_segmentation_queue_item()
                    if not queue_item:
                        break

                    queue_id, _, input_path_str = queue_item
                    input_path = Path(input_path_str)
                    if not input_path.is_absolute():
                        input_path = INPUT_DIR / input_path.name
                    if not input_path.exists() and not input_path.is_dir():
                        claimed_input_path = SEGMENTATION_DIR / input_path.name
                        if claimed_input_path.exists():
                            input_path = claimed_input_path

                    if not input_path.exists():
                        mark_segmentation_queue_item_error(queue_id, f"Input file not found: {input_path}")
                        continue

                    print(f"[Segmentation] Claimed queue item: {input_path.name}")
                    _submit_case(input_path, queue_id=queue_id)

                # List all NIfTI files in input directory
                current_files = sorted(list(INPUT_DIR.glob("*.nii.gz")))
                
                for f in current_files:
                    if _active_case_count() >= max_cases:
                        break

                    if not is_file_stable(f):
                        continue

                    if (SEGMENTATION_DIR / f.name).exists():
                        continue

                    try:
                        claimed_input = claim_input_file(f)
                    except FileNotFoundError:
                        continue
                    except FileExistsError:
                        continue
                    except Exception as exc:
                        print(f"[Segmentation] Failed to claim input file {f.name}: {exc}")
                        continue

                    print(f"[Segmentation] Claimed input file: {claimed_input.name}")
                    _submit_case(claimed_input)
            
                time.sleep(settings.SEGMENTATION_SCAN_INTERVAL)
                
            except Exception as e:
                print(f"Error in main loop: {e}")
                time.sleep(settings.SEGMENTATION_SCAN_INTERVAL)
                
    except KeyboardInterrupt:
        print("\nStopping monitoring...")
        _SHUTDOWN_EVENT.set()
    finally:
        _terminate_registered_child_processes(reason="segmentation worker shutdown")
        executor.shutdown(wait=False, cancel_futures=True)
        print("Executor shutdown complete.")


def _segment_case_with_heartbeat(case_input: Path, queue_id: int) -> bool:
    stop_event, heartbeat_thread = _start_claim_heartbeat(queue_id, case_label=case_input.name)
    try:
        return segment_case(case_input)
    finally:
        stop_event.set()
        heartbeat_thread.join(timeout=5)

if __name__ == "__main__":
    main()
