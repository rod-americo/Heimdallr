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
import copy
import json
import gzip
import shutil
import signal
import subprocess
import threading
import sys
import time
import concurrent.futures  # For parallel case segmentation
import re
import unicodedata
from pathlib import Path
import datetime
from zoneinfo import ZoneInfo

import nibabel as nib
import numpy as np

from heimdallr.metrics.head import HEAD_COMPONENT_MASKS, collect_mask_statuses, compute_mask_status
from heimdallr.shared import settings
from heimdallr.shared import store
from heimdallr.integration.delivery import enqueue_case_failed_delivery
from heimdallr.shared.paths import (
    study_artifacts_dir,
    study_derived_dir,
    study_dir,
    study_id_json,
    study_logs_dir,
    study_metadata_dir,
)
from heimdallr.shared.automatic_ct import (
    automatic_ct_planning_enabled,
    filter_jobs_by_inventory,
    required_segmentation_tasks_for_jobs,
    resolve_requested_metrics_jobs,
)
from heimdallr.shared.segmentation_inventory import (
    build_segmentation_inventory,
    mask_inventory_status,
    write_segmentation_inventory,
)
from heimdallr.shared.segmentation_coverage import classify_segmentation_coverage
from heimdallr.shared.segmentation_coverage import mask_complete as mask_complete_along_z
from heimdallr.shared.sqlite import connect as db_connect

settings.configure_service_stdio()

path_entries = [str(settings.TOTALSEG_BIN_DIR), str(Path(sys.executable).parent)]
os.environ["PATH"] = os.pathsep.join(path_entries + [os.environ["PATH"]])
LOCAL_TZ = ZoneInfo(settings.TIMEZONE)
_ACTIVE_CHILD_PROCESSES: set[subprocess.Popen[str]] = set()
_ACTIVE_CHILDREN_LOCK = threading.Lock()
_SHUTDOWN_EVENT = threading.Event()

# ============================================================


def _enqueue_external_failure_if_present(case_id: str, *, failure_stage: str, error_message: str) -> None:
    try:
        id_json_path = study_id_json(case_id)
        if not id_json_path.exists():
            return
        with open(id_json_path, "r", encoding="utf-8") as handle:
            metadata = json.load(handle)
        external_delivery = metadata.get("ExternalDelivery")
        if not isinstance(external_delivery, dict):
            return
        if enqueue_case_failed_delivery(
            case_id=case_id,
            study_uid=str(metadata.get("StudyInstanceUID", "") or "").strip() or None,
            external_delivery=external_delivery,
            failure_stage=failure_stage,
            error_message=error_message,
        ):
            print(f"[Integration] Enqueued case.failed callback for {case_id}")
    except Exception as exc:
        print(f"[Integration] Warning: failed to enqueue case.failed callback for {case_id}: {exc}")
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


class WorkerShutdownRequestedError(RuntimeError):
    """Raised when the worker is stopping while a task is still running."""


def _is_ineligible_selection_error(exc: Exception) -> bool:
    return "No eligible series found for profile" in str(exc or "")


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


def is_segmentation_queue_item_canceled(queue_id: int) -> bool:
    conn = db_connect()
    try:
        return store.is_queue_item_canceled(conn, "segmentation_queue", queue_id)
    finally:
        conn.close()


def mark_segmentation_queue_item_error(queue_id, error_message):
    """Mark queue item as error with a truncated message."""
    conn = db_connect()
    store.mark_segmentation_queue_item_error(conn, queue_id, error_message)
    conn.close()


def retry_segmentation_queue_item(queue_id, error_message) -> bool:
    conn = db_connect()
    try:
        return store.retry_segmentation_queue_item(
            conn,
            queue_id,
            error_message,
            max_attempts=settings.SEGMENTATION_SHUTDOWN_RETRIES + 1,
        )
    finally:
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


def _external_series_selection_policy(id_data: dict) -> dict:
    external_delivery = id_data.get("ExternalDelivery")
    if not isinstance(external_delivery, dict):
        return {}
    policy = external_delivery.get("series_selection_policy")
    return policy if isinstance(policy, dict) else {}


def _merge_series_selection_profile(base: dict, overrides: dict, *, top_level: bool = False) -> dict:
    merged = copy.deepcopy(base)
    reserved_keys = {"name", "profile", "profile_name", "base_profile", "schema_version"}
    for key, value in overrides.items():
        if top_level and key in reserved_keys:
            continue
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_series_selection_profile(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def resolve_series_selection_profile_for_case(id_data: dict) -> tuple[str, dict, str, str | None]:
    profile_name, profile = load_series_selection_profile()
    policy = _external_series_selection_policy(id_data)
    if not policy:
        return profile_name, profile, "config", None

    policy_name = str(policy.get("name") or policy.get("profile_name") or "external_submission").strip()
    if not policy_name:
        policy_name = "external_submission"
    merged = _merge_series_selection_profile(profile, policy, top_level=True)
    return f"{profile_name}+{policy_name}", merged, "external_delivery", policy_name


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


def load_metrics_pipeline_profile_for_segmentation() -> tuple[str, dict]:
    """Load metrics profile metadata needed to filter segmentation tasks."""
    config_path = Path(settings.METRICS_PIPELINE_CONFIG_PATH)
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    profiles = config.get("profiles", {})
    profile_name = settings.METRICS_PIPELINE_PROFILE or config.get("default_profile")
    if not profile_name:
        raise RuntimeError(f"Metrics pipeline config has no default_profile: {config_path}")
    profile = profiles.get(profile_name)
    if not profile:
        raise RuntimeError(f"Metrics pipeline profile '{profile_name}' not found in {config_path}")
    return profile_name, profile


def _normalize_job_needs(job: dict) -> list[str]:
    raw_needs = job.get("needs", [])
    if raw_needs in (None, ""):
        return []
    if not isinstance(raw_needs, list):
        raise RuntimeError(f"Metrics job '{job.get('name', '<unknown>')}' needs must be a list")
    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw_needs:
        need = str(item or "").strip()
        if not need or need in seen:
            continue
        normalized.append(need)
        seen.add(need)
    return normalized


def _normalize_required_segmentation_tasks(job: dict) -> list[str] | None:
    raw_tasks = job.get("requires_segmentation_tasks")
    if raw_tasks is None:
        raw_tasks = job.get("segmentation_tasks")
    if raw_tasks is None:
        return None
    if not isinstance(raw_tasks, list):
        raise RuntimeError(
            f"Metrics job '{job.get('name', '<unknown>')}' requires_segmentation_tasks must be a list"
        )
    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw_tasks:
        task_name = str(item or "").strip()
        if not task_name or task_name in seen:
            continue
        normalized.append(task_name)
        seen.add(task_name)
    return normalized


def _is_automatic_metrics_job(job: dict) -> bool:
    return bool(job.get("automatic", False))


def _requested_segmentation_task_names(requested_job_names: list[str] | None) -> set[str] | None:
    if not requested_job_names:
        return None

    _metrics_profile_name, metrics_profile = load_metrics_pipeline_profile_for_segmentation()
    jobs: list[dict] = []
    seen_names: set[str] = set()
    for raw_job in metrics_profile.get("jobs", []):
        if not raw_job.get("enabled", True):
            continue
        job = dict(raw_job)
        name = str(job.get("name", "") or "").strip()
        if not name:
            raise RuntimeError("Metrics job is missing a name")
        if name in seen_names:
            raise RuntimeError(f"Metrics profile contains duplicate job '{name}'")
        seen_names.add(name)
        job["name"] = name
        job["needs"] = _normalize_job_needs(job)
        jobs.append(job)

    jobs_by_name = {job["name"]: job for job in jobs}
    unknown = [name for name in requested_job_names if name not in jobs_by_name]
    if unknown:
        raise RuntimeError(f"Requested metrics job(s) not found in profile: {', '.join(unknown)}")

    resolved_job_names: list[str] = []
    seen_resolved: set[str] = set()

    def include_job(name: str) -> None:
        if name in seen_resolved:
            return
        for need in jobs_by_name[name]["needs"]:
            include_job(need)
        resolved_job_names.append(name)
        seen_resolved.add(name)

    for name in requested_job_names:
        include_job(name)
    for name, job in jobs_by_name.items():
        if _is_automatic_metrics_job(job):
            include_job(name)

    required_tasks: set[str] = set()
    for name in resolved_job_names:
        required = _normalize_required_segmentation_tasks(jobs_by_name[name])
        if required is None:
            return None
        required_tasks.update(required)
    return required_tasks


def _requested_metrics_modules_from_metadata(metadata: dict) -> list[str]:
    external_delivery = metadata.get("ExternalDelivery", {})
    if not isinstance(external_delivery, dict):
        return []
    raw = external_delivery.get("requested_metrics_modules", [])
    if not isinstance(raw, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw:
        name = str(item or "").strip()
        if not name or name in seen:
            continue
        normalized.append(name)
        seen.add(name)
    return normalized


def resolve_segmentation_plan(
    modality,
    selected_phase,
    requested_metrics_modules: list[str] | None = None,
) -> tuple[str, list[dict]]:
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
    required_task_names = _requested_segmentation_task_names(requested_metrics_modules)
    if required_task_names is not None:
        enabled_task_names = {
            str(task.get("name", "") or "").strip()
            for task in tasks
            if str(task.get("name", "") or "").strip()
        }
        missing_required_tasks = sorted(required_task_names - enabled_task_names)
        if missing_required_tasks:
            raise RuntimeError(
                f"Requested metrics require segmentation task(s) not enabled in profile "
                f"'{profile_name}': {', '.join(missing_required_tasks)}"
            )
        tasks = [
            task
            for task in tasks
            if str(task.get("name", "") or "").strip() in required_task_names
        ]
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


def _positive_float(value) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _series_geometry_metrics(series: dict) -> dict:
    coverage_mm = _positive_float(series.get("CoverageMm"))
    z_spacing_mm = _positive_float(series.get("ZSpacingMm"))
    spacing_between_mm = _positive_float(series.get("SpacingBetweenSlicesMm"))
    slice_thickness_mm = _positive_float(series.get("SliceThicknessMm"))
    if slice_thickness_mm is None:
        slice_thickness_mm = _positive_float(series.get("SliceThickness"))
    effective_thickness_mm = z_spacing_mm or spacing_between_mm or slice_thickness_mm
    return {
        "coverage_mm": coverage_mm,
        "z_spacing_mm": z_spacing_mm,
        "spacing_between_mm": spacing_between_mm,
        "slice_thickness_mm": slice_thickness_mm,
        "effective_thickness_mm": effective_thickness_mm,
        "geometry_confidence": str(series.get("GeometryConfidence", "") or ""),
    }


def _geometry_priority_settings(profile: dict) -> dict:
    raw = profile.get("geometry_priority")
    if not isinstance(raw, dict):
        return {"enabled": False}
    return {
        "enabled": bool(raw.get("enabled", False)),
        "coverage_equivalence_ratio": _safe_float(raw.get("coverage_equivalence_ratio"), 0.92),
        "coverage_equivalence_mm": _safe_float(raw.get("coverage_equivalence_mm"), 50.0),
        "prefer_thinner_within_equivalent_coverage": bool(
            raw.get("prefer_thinner_within_equivalent_coverage", True)
        ),
    }


def _apply_geometry_priority(candidates: list[dict], settings: dict) -> bool:
    if not settings.get("enabled"):
        return False

    grouped_max_coverage: dict[int, float] = {}
    for candidate in candidates:
        coverage = candidate.get("coverage_mm")
        if coverage is None:
            continue
        rank = int(candidate["phase_rank"])
        grouped_max_coverage[rank] = max(grouped_max_coverage.get(rank, 0.0), float(coverage))

    if not grouped_max_coverage:
        return False

    ratio = min(max(float(settings.get("coverage_equivalence_ratio", 0.92)), 0.0), 1.0)
    equivalence_mm = max(float(settings.get("coverage_equivalence_mm", 50.0)), 0.0)
    for candidate in candidates:
        max_coverage = grouped_max_coverage.get(int(candidate["phase_rank"]))
        coverage = candidate.get("coverage_mm")
        if max_coverage is None or coverage is None:
            candidate["geometry_missing"] = True
            candidate["coverage_tier"] = 2
            candidate["max_coverage_mm"] = max_coverage
            candidate["coverage_equivalence_floor_mm"] = None
            continue
        coverage_floor = max(max_coverage * ratio, max_coverage - equivalence_mm)
        candidate["geometry_missing"] = False
        candidate["max_coverage_mm"] = max_coverage
        candidate["coverage_equivalence_floor_mm"] = coverage_floor
        candidate["coverage_tier"] = 0 if float(coverage) >= coverage_floor else 1
    return True


def _candidate_geometry_audit(candidate: dict) -> dict:
    series = candidate["series"]
    return {
        "SeriesInstanceUID": series.get("SeriesInstanceUID"),
        "SeriesNumber": series.get("SeriesNumber"),
        "Phase": candidate.get("phase"),
        "CoverageMm": candidate.get("coverage_mm"),
        "ZSpacingMm": candidate.get("z_spacing_mm"),
        "SliceThicknessMm": candidate.get("slice_thickness_mm"),
        "EffectiveThicknessMm": candidate.get("effective_thickness_mm"),
        "CoverageTier": candidate.get("coverage_tier"),
        "PreferenceScore": candidate.get("preference_score"),
        "WindowClass": candidate.get("window_class"),
        "ManufacturerHintRules": candidate.get("manufacturer_hint_rules"),
    }


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
    return {
        "description": _normalize_search_text(series.get("SeriesDescription")),
        "kernel": _normalize_search_text(series.get("ConvolutionKernel")),
        "protocol": _normalize_search_text(series.get("ProtocolName")),
        "manufacturer": _normalize_search_text(series.get("Manufacturer")),
        "model": _normalize_search_text(series.get("ManufacturerModelName")),
    }


def _normalize_search_text(value) -> str:
    if isinstance(value, (list, tuple)):
        value = " ".join(str(item) for item in value)
    normalized = unicodedata.normalize("NFKD", str(value or "").lower())
    return "".join(char for char in normalized if not unicodedata.combining(char))


def _normalized_hint_tokens(values) -> set[str]:
    return {
        normalized
        for value in (values or [])
        if (normalized := _normalize_search_text(value).strip())
    }


def _matching_manufacturer_hints(series, manufacturer_hints) -> list[dict]:
    tokens = _text_tokens(series)
    matched = []
    for rule in manufacturer_hints or []:
        manufacturers = _normalized_hint_tokens(rule.get("manufacturer_contains"))
        models = _normalized_hint_tokens(rule.get("model_contains"))
        manufacturer_match = not manufacturers or any(
            token in tokens["manufacturer"] for token in manufacturers
        )
        model_match = not models or any(token in tokens["model"] for token in models)
        if manufacturer_match and model_match:
            matched.append(rule)
    return matched


def _hint_score(text: str, values, weight: int) -> int:
    return sum(weight for token in _normalized_hint_tokens(values) if token in text)


def _first_numeric_hint(value) -> float | None:
    if isinstance(value, (list, tuple)):
        value = value[0] if value else None
    match = re.search(r"[-+]?\d+(?:[.,]\d+)?", str(value or ""))
    if not match:
        return None
    return _safe_float(match.group(0).replace(",", "."), None)


def _window_preference(series, window_hints) -> tuple[int, str]:
    if not window_hints:
        return 0, "unknown"
    center = _first_numeric_hint(series.get("WindowCenter"))
    width = _first_numeric_hint(series.get("WindowWidth"))
    if center is None or width is None:
        return 0, "unknown"

    center_range = window_hints.get("soft_tissue_center_range") or []
    width_range = window_hints.get("soft_tissue_width_range") or []
    if (
        len(center_range) == 2
        and len(width_range) == 2
        and float(center_range[0]) <= center <= float(center_range[1])
        and float(width_range[0]) <= width <= float(width_range[1])
    ):
        return -1, "soft_tissue"

    lung_center_max = _safe_float(window_hints.get("lung_center_max"), None)
    lung_width_min = _safe_float(window_hints.get("lung_width_min"), None)
    if (
        lung_center_max is not None
        and lung_width_min is not None
        and center <= lung_center_max
        and width >= lung_width_min
    ):
        return 1, "lung"
    return 0, "other"


def _series_preference(series, text_hints, manufacturer_hints, window_hints):
    tokens = _text_tokens(series)
    applied_rules = _matching_manufacturer_hints(series, manufacturer_hints)
    hint_sets = [text_hints, *applied_rules]
    score = 0
    for hints in hint_sets:
        score += _hint_score(tokens["description"], hints.get("description_avoid"), 1)
        score += _hint_score(tokens["kernel"], hints.get("kernel_avoid"), 1)
        score += _hint_score(tokens["protocol"], hints.get("protocol_avoid"), 1)
        score += _hint_score(tokens["description"], hints.get("description_prefer"), -1)
        score += _hint_score(tokens["kernel"], hints.get("kernel_prefer"), -1)
        score += _hint_score(tokens["protocol"], hints.get("protocol_prefer"), -1)
    window_score, window_class = _window_preference(series, window_hints)
    return {
        "score": score + window_score,
        "window_class": window_class,
        "manufacturer_hint_rules": [str(rule.get("name") or "unnamed") for rule in applied_rules],
    }


def _series_hard_reject_reason(series, rules):
    tokens = _text_tokens(series)
    description = tokens["description"]
    kernel = tokens["kernel"]
    kernel_compact = kernel.strip()

    for token in rules.get("description_contains", []):
        normalized = _normalize_search_text(token).strip()
        if normalized and normalized in description:
            return f"description_rejected:{normalized}"

    for token in rules.get("kernel_contains", []):
        normalized = _normalize_search_text(token).strip()
        if normalized and normalized in kernel:
            return f"kernel_rejected_contains:{normalized}"

    for token in rules.get("kernel_exact", []):
        normalized = _normalize_search_text(token).strip()
        if normalized and kernel_compact == normalized:
            return f"kernel_rejected_exact:{normalized}"

    return None


def _series_region_hint(series: dict) -> str:
    description = _normalize_search_text(series.get("SeriesDescription"))
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

    profile_name, profile, policy_source, external_policy_name = resolve_series_selection_profile_for_case(id_data)
    required = profile.get("required", {})
    hard_reject = profile.get("hard_reject", {})
    text_hints = profile.get("text_hints", {})
    manufacturer_hints = profile.get("manufacturer_hints", [])
    window_hints = profile.get("window_hints", {})
    follow_up_coverage = profile.get("follow_up_coverage", {})
    geometry_priority = _geometry_priority_settings(profile)
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
        preference = _series_preference(series, text_hints, manufacturer_hints, window_hints)
        geometry_metrics = _series_geometry_metrics(series)

        candidates.append(
            {
                "series": series,
                "path": nifti_path,
                "phase": phase,
                "phase_detected": phase_detected,
                "phase_probability": probability,
                "slice_count": slice_count,
                "preference_score": preference["score"],
                "window_class": preference["window_class"],
                "manufacturer_hint_rules": preference["manufacturer_hint_rules"],
                "phase_rank": resolved_phase_rank,
                "fallback_reason": fallback_reason,
                **geometry_metrics,
            }
        )

    if not candidates:
        raise RuntimeError(
            f"No eligible series found for profile '{profile_name}' in study {case_id}. Rejected: {json.dumps(rejected, ensure_ascii=False)}"
        )

    geometry_priority_applied = _apply_geometry_priority(candidates, geometry_priority)
    if geometry_priority_applied:
        prefer_thinner = bool(geometry_priority.get("prefer_thinner_within_equivalent_coverage", True))
        candidates.sort(
            key=lambda c: (
                c["phase_rank"],
                1 if c.get("geometry_missing") else 0,
                c.get("coverage_tier", 2),
                (
                    c.get("effective_thickness_mm")
                    if (
                        prefer_thinner
                        and c.get("coverage_tier") == 0
                        and c.get("effective_thickness_mm") is not None
                    )
                    else float("inf")
                ),
                -(c.get("coverage_mm") or 0.0),
                c["preference_score"],
                0 if c["phase_detected"] else 1,
                -c["phase_probability"],
                -c["slice_count"],
                str(c["series"].get("SeriesNumber", "")),
            )
        )
    else:
        candidates.sort(
            key=lambda c: (
                c["phase_rank"],
                c["preference_score"],
                0 if c["phase_detected"] else 1,
                -c["phase_probability"],
                -c["slice_count"],
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
        "PolicySource": policy_source,
        "SelectedSeriesInstanceUID": selected_series.get("SeriesInstanceUID"),
        "SelectedSeriesNumber": selected_series.get("SeriesNumber"),
        "SelectedSeriesDescription": selected_series.get("SeriesDescription", ""),
        "SelectedDerivedNiftiPath": str(selected["path"].relative_to(study_dir(case_id))),
        "SelectedPhase": selected["phase"],
        "PhaseDetected": selected["phase_detected"],
        "PhaseProbability": selected["phase_probability"],
        "SliceCount": selected["slice_count"],
        "GeometryPriorityApplied": geometry_priority_applied,
        "SelectedCoverageMm": selected.get("coverage_mm"),
        "SelectedZSpacingMm": selected.get("z_spacing_mm"),
        "SelectedSliceThicknessMm": selected.get("slice_thickness_mm"),
        "SelectedEffectiveThicknessMm": selected.get("effective_thickness_mm"),
        "MaxCoverageMm": selected.get("max_coverage_mm"),
        "CoverageEquivalenceFloorMm": selected.get("coverage_equivalence_floor_mm"),
        "SelectedPreferenceScore": selected.get("preference_score"),
        "SelectedWindowClass": selected.get("window_class"),
        "SelectedManufacturerHintRules": selected.get("manufacturer_hint_rules"),
        "SelectionReason": (
            f"phase={selected['phase']}, detected={selected['phase_detected']}, "
            f"probability={selected['phase_probability']}, slices={selected['slice_count']}"
            f", preference_score={selected.get('preference_score')}"
            f", window_class={selected.get('window_class')}"
            + (
                f", coverage_mm={selected.get('coverage_mm')}, "
                f"effective_thickness_mm={selected.get('effective_thickness_mm')}"
                if geometry_priority_applied
                else ""
            )
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
    if external_policy_name:
        selection_info["ExternalPolicyName"] = external_policy_name
    if geometry_priority_applied:
        selection_info["CandidateSeriesGeometry"] = [_candidate_geometry_audit(candidate) for candidate in candidates]
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


def _task_name(task: dict) -> str:
    return str(task.get("name", "") or "").strip()


def _task_output_path(case_output: Path, task: dict) -> Path:
    return case_output / str(task.get("output_dir") or f"artifacts/{_task_name(task)}")


def _task_record(task: dict) -> dict:
    return {
        "name": _task_name(task),
        "output_dir": str(task.get("output_dir") or f"artifacts/{_task_name(task)}"),
        "extra_args": list(task.get("extra_args", [])),
        "license_required": bool(task.get("license_required")),
    }


def _clear_task_output(case_output: Path, task: dict) -> None:
    output_dir = _task_output_path(case_output, task)
    if output_dir.exists():
        shutil.rmtree(output_dir)


def _load_mask_status(mask_path: Path, reference_image_path: Path) -> dict:
    return mask_inventory_status(mask_path, reference_image_path)


def _head_union_status(total_dir: Path, head_components: dict, spacing_xyz: tuple[float, float, float]) -> dict:
    union: np.ndarray | None = None
    for mask_name in HEAD_COMPONENT_MASKS:
        status = head_components.get("masks", {}).get(mask_name, {})
        if not status.get("present") or not status.get("complete"):
            return compute_mask_status(None, spacing_xyz)
        mask_path = total_dir / f"{mask_name}.nii.gz"
        image = nib.load(str(mask_path))
        mask = np.asarray(image.get_fdata(), dtype=np.float32) > 0
        union = mask if union is None else (union | mask)
    return compute_mask_status(union, spacing_xyz)


def _head_gatekeeper(total_dir: Path, reference_image_path: Path) -> dict:
    try:
        reference_image = nib.load(str(reference_image_path))
        reference_shape = tuple(int(value) for value in reference_image.shape[:3])
        spacing_xyz = tuple(float(value) for value in reference_image.header.get_zooms()[:3])
    except Exception as exc:
        return {
            "complete": False,
            "reason": "reference_read_error",
            "error": str(exc),
        }

    try:
        head_components = collect_mask_statuses(
            total_dir,
            list(HEAD_COMPONENT_MASKS),
            spacing_xyz,
            reference_shape=reference_shape,
        )
        head_union = _head_union_status(total_dir, head_components, spacing_xyz)
    except Exception as exc:
        return {
            "complete": False,
            "reason": "head_gate_error",
            "error": str(exc),
        }

    brain_status = head_components.get("masks", {}).get("brain", {})
    complete = bool(brain_status.get("complete"))
    return {
        "complete": complete,
        "reason": "complete_brain" if complete else "incomplete_brain",
        "head_components": head_components,
        "head_union": head_union,
    }


def _segmentation_profile_uses_automatic_ct(profile_name: str) -> bool:
    if str(profile_name or "").startswith("ct_automatic_"):
        return True
    try:
        active_profile_name, profile = load_segmentation_pipeline_profile()
    except Exception:
        return False
    if active_profile_name != profile_name:
        return False
    return automatic_ct_planning_enabled(profile)


def _automatic_ct_required_task_names(
    *,
    inventory: dict,
    requested_metrics_modules: list[str] | None,
) -> tuple[set[str] | None, dict]:
    metrics_profile_name, metrics_profile = load_metrics_pipeline_profile_for_segmentation()
    requested_jobs = resolve_requested_metrics_jobs(
        metrics_profile,
        requested_job_names=requested_metrics_modules,
    )
    selected_jobs, skipped_jobs = filter_jobs_by_inventory(requested_jobs, inventory)
    required_task_names = required_segmentation_tasks_for_jobs(selected_jobs)
    if required_task_names is not None:
        required_task_names.add("total")
    return required_task_names, {
        "mode": "automatic_ct",
        "metrics_profile": metrics_profile_name,
        "selected_jobs": [job["name"] for job in selected_jobs],
        "skipped_jobs": skipped_jobs,
        "required_segmentation_tasks": sorted(required_task_names) if required_task_names else None,
    }


def run_segmentation_pipeline(
    case_id,
    modality,
    selected_phase,
    nifti_path,
    case_output,
    artifacts_dir,
    log_dir,
    logger,
    requested_metrics_modules: list[str] | None = None,
):
    """Execute the configured segmentation task list for the selected series."""
    profile_name, tasks = resolve_segmentation_plan(
        modality,
        selected_phase,
        requested_metrics_modules=requested_metrics_modules,
    )
    ordered_tasks = sorted(tasks, key=lambda task: 0 if _task_name(task) == "total" else 1)
    automatic_ct = _segmentation_profile_uses_automatic_ct(profile_name)

    logger.print(f"[Segmentation] Profile: {profile_name}")
    logger.print(f"[Segmentation] Selected phase: {_normalize_phase(selected_phase)}")
    if requested_metrics_modules:
        logger.print(f"[Segmentation] Requested metrics: {', '.join(requested_metrics_modules)}")
    logger.print(f"[Segmentation] Tasks: {', '.join(_task_name(task) for task in ordered_tasks)}")

    gatekeepers: dict[str, dict] = {}
    executed_tasks: list[dict] = []
    skipped_tasks: list[dict] = []
    head_gate: dict | None = None
    inventory: dict | None = None
    inventory_path: Path | None = None
    automatic_plan: dict | None = None
    automatic_required_task_names: set[str] | None = None
    automatic_plan_resolved = False

    if automatic_ct and not any(_task_name(task) == "total" for task in ordered_tasks):
        raise RuntimeError(f"Automatic CT segmentation profile '{profile_name}' must enable the total task")

    for task in ordered_tasks:
        task_name = _task_name(task)
        if (
            automatic_ct
            and task_name != "total"
            and automatic_plan_resolved
            and automatic_required_task_names is not None
            and task_name not in automatic_required_task_names
        ):
            _clear_task_output(case_output, task)
            skipped_tasks.append(
                {
                    **_task_record(task),
                    "reason": "not_required_by_automatic_ct_plan",
                }
            )
            continue

        if task_name == "tissue_types":
            l3_gate = gatekeepers.get("l3_complete")
            if l3_gate is None:
                l3_gate = _load_mask_status(
                    artifacts_dir / "total" / "vertebrae_L3.nii.gz",
                    nifti_path,
                )
                gatekeepers["l3_complete"] = l3_gate
            if not l3_gate.get("complete"):
                _clear_task_output(case_output, task)
                logger.print(f"[Segmentation] Skipping tissue_types: {l3_gate.get('reason')}")
                skipped_tasks.append(
                    {
                        **_task_record(task),
                        "reason": "l3_gatekeeper_failed",
                        "gatekeeper": "l3_complete",
                    }
                )
                continue

        if task_name in {"cerebral_bleed", "brain_structures"}:
            if head_gate is None:
                head_gate = _head_gatekeeper(artifacts_dir / "total", nifti_path)
                gatekeepers["head_complete"] = head_gate
            if not head_gate.get("complete"):
                _clear_task_output(case_output, task)
                logger.print(f"[Segmentation] Skipping {task_name}: {head_gate.get('reason')}")
                skipped_tasks.append(
                    {
                        **_task_record(task),
                        "reason": "head_gatekeeper_failed",
                        "gatekeeper": "head_complete",
                    }
                )
                continue

        extra_args = [str(arg) for arg in task.get("extra_args", [])]
        output_dir = _task_output_path(case_output, task)
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
        executed = dict(task)
        executed["extra_args"] = extra_args
        executed_tasks.append(_task_record(executed))

        if automatic_ct and task_name == "total":
            inventory = build_segmentation_inventory(artifacts_dir / "total", nifti_path)
            inventory_path = write_segmentation_inventory(artifacts_dir, inventory)
            automatic_required_task_names, automatic_plan = _automatic_ct_required_task_names(
                inventory=inventory,
                requested_metrics_modules=requested_metrics_modules,
            )
            automatic_plan_resolved = True
            logger.print(
                "[Segmentation] Automatic CT jobs: "
                + (
                    ", ".join(automatic_plan["selected_jobs"])
                    if automatic_plan.get("selected_jobs")
                    else "none"
                )
            )

    payload = {
        "profile": profile_name,
        "requested_metrics_modules": list(requested_metrics_modules or []),
        "tasks": executed_tasks,
        "skipped_tasks": skipped_tasks,
        "gatekeepers": gatekeepers,
    }
    if inventory is not None:
        payload["segmentation_inventory"] = {
            "path": str(inventory_path.relative_to(case_output)) if inventory_path else None,
            "summary": {
                "brain_complete": bool(inventory.get("brain", {}).get("complete")),
                "l3_complete": bool(inventory.get("vertebrae_L3", {}).get("complete")),
                "parenchymal_organs_present": list(
                    inventory.get("parenchymal_organs", {}).get("present", [])
                ),
                "lungs_present": list(inventory.get("lungs", {}).get("present", [])),
                "lungs_any_present": bool(inventory.get("lungs", {}).get("any_present")),
            },
        }
    if automatic_plan is not None:
        payload["automatic_ct_plan"] = automatic_plan
    return payload


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
                    if "Worker shutdown requested while task was still running" in timed_out_message:
                        raise WorkerShutdownRequestedError(timed_out_message)
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
            except WorkerShutdownRequestedError:
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


def segment_case(case_input, queue_id: int | None = None):
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
            if _is_ineligible_selection_error(e):
                _record_segmentation_pipeline_state(
                    case_id,
                    status="ineligible",
                    end_dt=datetime.datetime.now(LOCAL_TZ),
                )
                logger.close()
                return "ineligible"
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
        requested_metrics_modules: list[str] = []
        id_json_path = study_id_json(case_id)
        if id_json_path.exists():
            try:
                with open(id_json_path, "r") as f:
                    meta = json.load(f)
                modality = meta.get("Modality", "CT")
                study_uid = meta.get("StudyInstanceUID")
                requested_metrics_modules = _requested_metrics_modules_from_metadata(meta)
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
        profile_name, planned_tasks = resolve_segmentation_plan(
            modality,
            selected_phase,
            requested_metrics_modules=requested_metrics_modules,
        )
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
                "requested_metrics_modules": list(requested_metrics_modules),
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
                requested_metrics_modules=requested_metrics_modules,
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

        if queue_id is not None and is_segmentation_queue_item_canceled(queue_id):
            logger.print("[Metrics] Skipping metrics enqueue because the queue item was canceled")
        else:
            try:
                enqueue_case_for_metrics(case_id, study_dir(case_id))
                logger.print("[Metrics] Enqueued for metrics stage")
            except Exception as e:
                logger.print(f"[Metrics] Warning: failed to enqueue metrics stage: {e}")

        logger.close()
        return True

    except WorkerShutdownRequestedError as e:
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
        logger.close()
        raise

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

    Monitors input/ directory for new NIfTI files and processes them with
    configurable case concurrency. The default operational policy is one case
    at a time.
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
            if ok == "ineligible":
                print(f"[Segmentation] Ineligible: {f_path.name}")
                if queue_id is not None:
                    mark_segmentation_queue_item_done(queue_id)
                return
            print(f"[Segmentation] {'Done' if ok else 'Failed'}: {f_path.name}")
            if queue_id is not None:
                if ok:
                    mark_segmentation_queue_item_done(queue_id)
                else:
                    error_message = "Case finished with failure return status"
                    mark_segmentation_queue_item_error(queue_id, error_message)
                    _enqueue_external_failure_if_present(
                        f_path.name,
                        failure_stage="segmentation",
                        error_message=error_message,
                    )
        except WorkerShutdownRequestedError as e:
            if queue_id is not None and retry_segmentation_queue_item(queue_id, e):
                print(f"[Segmentation] Requeued after worker shutdown: {f_path.name}")
                return
            if queue_id is not None:
                mark_segmentation_queue_item_error(queue_id, e)
                _enqueue_external_failure_if_present(
                    f_path.name,
                    failure_stage="segmentation",
                    error_message=str(e),
                )
            print(f"Error in case segmentation thread {f_path.name}: {e}")
        except Exception as e:
            if queue_id is not None:
                mark_segmentation_queue_item_error(queue_id, e)
                _enqueue_external_failure_if_present(
                    f_path.name,
                    failure_stage="segmentation",
                    error_message=str(e),
                )
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
        return segment_case(case_input, queue_id=queue_id)
    finally:
        stop_event.set()
        heartbeat_thread.join(timeout=5)

if __name__ == "__main__":
    main()
