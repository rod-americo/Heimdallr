#!/usr/bin/env python3
"""Resident worker for post-segmentation Heimdallr metrics."""

from __future__ import annotations

import concurrent.futures
import datetime
import json
import subprocess
import threading
import time
from pathlib import Path
from zoneinfo import ZoneInfo

from heimdallr.shared import settings, store
from heimdallr.shared.paths import study_dir, study_id_json, study_logs_dir, study_results_json
from heimdallr.shared.sqlite import connect as db_connect


LOCAL_TZ = ZoneInfo(settings.TIMEZONE)
JOB_MODULES = {
    "l3_muscle_area": "heimdallr.metrics.jobs.l3_muscle_area",
    "bone_health_l1_hu": "heimdallr.metrics.jobs.bone_health_l1_hu",
    "bone_health_l1_volumetric": "heimdallr.metrics.jobs.bone_health_l1_volumetric",
    "vertebral_fracture_screen": "heimdallr.metrics.jobs.vertebral_fracture_screen",
    "opportunistic_osteoporosis_composite": "heimdallr.metrics.jobs.opportunistic_osteoporosis_composite",
    "body_fat_abdominal_volumes": "heimdallr.metrics.jobs.body_fat_abdominal_volumes",
    "body_fat_l3_slice": "heimdallr.metrics.jobs.body_fat_l3_slice",
}

settings.ensure_directories()


class MetricsLogger:
    """Simple dual logger for metrics pipeline execution."""

    def __init__(self, log_path: Path | None):
        self.log_handle = None
        if log_path is not None:
            self.log_handle = open(log_path, "w", encoding="utf-8")
            self.log(f"=== Heimdallr Metrics Log ===")
            self.log(f"Started: {settings.local_timestamp()}")
            self.log("")

    def log(self, message: str) -> None:
        print(message)
        if self.log_handle is not None:
            self.log_handle.write(message + "\n")
            self.log_handle.flush()

    def close(self) -> None:
        if self.log_handle is not None:
            self.log_handle.write(f"\nFinished: {settings.local_timestamp()}\n")
            self.log_handle.close()
            self.log_handle = None


def ensure_metrics_queue_table() -> None:
    conn = db_connect()
    try:
        store.ensure_schema(conn)
    finally:
        conn.close()


def claim_next_pending_metrics_queue_item():
    conn = db_connect()
    try:
        return store.claim_next_pending_metrics_queue_item(conn)
    finally:
        conn.close()


def mark_metrics_queue_item_done(queue_id: int) -> None:
    conn = db_connect()
    try:
        store.mark_metrics_queue_item_done(conn, queue_id)
    finally:
        conn.close()


def mark_metrics_queue_item_error(queue_id: int, error_message) -> None:
    conn = db_connect()
    try:
        store.mark_metrics_queue_item_error(conn, queue_id, error_message)
    finally:
        conn.close()


def load_metrics_pipeline_profile() -> tuple[str, dict]:
    config_path = Path(settings.METRICS_PIPELINE_CONFIG_PATH)
    with open(config_path, "r", encoding="utf-8") as handle:
        config = json.load(handle)

    profiles = config.get("profiles", {})
    profile_name = settings.METRICS_PIPELINE_PROFILE or config.get("default_profile")
    if not profile_name:
        raise RuntimeError(f"Metrics pipeline config has no default_profile: {config_path}")
    profile = profiles.get(profile_name)
    if not profile:
        raise RuntimeError(f"Metrics pipeline profile '{profile_name}' not found in {config_path}")
    return profile_name, profile


def _load_case_metadata(case_id: str) -> dict:
    metadata_path = study_id_json(case_id)
    with open(metadata_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_case_metadata(case_id: str, metadata: dict) -> None:
    metadata_path = study_id_json(case_id)
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    study_uid = metadata.get("StudyInstanceUID")
    if study_uid:
        conn = db_connect()
        try:
            store.update_id_json(conn, study_uid, metadata)
        finally:
            conn.close()


def _upsert_results(case_id: str, metric_key: str, payload: dict, metadata: dict) -> None:
    results_path = study_results_json(case_id)
    if results_path.exists():
        try:
            results = json.loads(results_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            results = {}
    else:
        results = {}
    if not isinstance(results, dict):
        results = {}

    results.setdefault("metrics", {})[metric_key] = payload
    results_path.write_text(json.dumps(results, indent=2), encoding="utf-8")

    study_uid = metadata.get("StudyInstanceUID")
    if study_uid:
        conn = db_connect()
        try:
            store.update_calculation_results(conn, study_uid, results)
        finally:
            conn.close()


def _validate_case_against_profile(case_id: str, metadata: dict, profile_name: str, profile: dict) -> None:
    required = profile.get("required", {})
    modality = str(metadata.get("Modality", "") or "")
    required_modality = required.get("modality")
    if required_modality and modality != required_modality:
        raise RuntimeError(
            f"Metrics profile '{profile_name}' requires modality {required_modality}, got {modality}"
        )

    required_phases = {str(item).strip().lower() for item in required.get("selected_phase", [])}
    selected_phase = (
        metadata.get("Pipeline", {})
        .get("series_selection", {})
        .get("SelectedPhase", "")
    ).strip().lower()
    if required_phases and selected_phase not in required_phases:
        raise RuntimeError(
            f"Metrics profile '{profile_name}' requires phase in {sorted(required_phases)}, got {selected_phase or 'unknown'}"
        )


def _run_job(case_id: str, job: dict, log_dir: Path) -> dict:
    job_name = job["name"]
    module_name = JOB_MODULES.get(job_name)
    if module_name is None:
        raise RuntimeError(f"Metrics job '{job_name}' is not registered")

    cmd = [
        settings.METRICS_PYTHON,
        "-m",
        module_name,
        "--case-id",
        case_id,
        "--job-config-json",
        json.dumps(job),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)

    job_log_path = log_dir / f"metrics_{job_name}.log"
    job_log_path.write_text(
        "\n".join(
            [
                f"Command: {' '.join(cmd)}",
                "",
                "=== STDOUT ===",
                proc.stdout.rstrip(),
                "",
                "=== STDERR ===",
                proc.stderr.rstrip(),
                "",
                f"Exit code: {proc.returncode}",
            ]
        ).rstrip()
        + "\n",
        encoding="utf-8",
    )

    stdout = proc.stdout.strip()
    if not stdout:
        raise RuntimeError(f"Metrics job '{job_name}' produced no stdout")

    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Metrics job '{job_name}' emitted non-JSON stdout"
        ) from exc

    if proc.returncode != 0:
        message = payload.get("error") or f"Metrics job '{job_name}' failed"
        raise RuntimeError(message)
    return payload


def _payload_summary(payload: dict) -> str:
    measurement = payload.get("measurement", {}) if isinstance(payload.get("measurement"), dict) else {}
    candidates = [
        measurement.get("skeletal_muscle_area_cm2"),
        measurement.get("l1_trabecular_hu_mean"),
        measurement.get("bone_health_l1_volumetric_trabecular_hu_mean"),
        measurement.get("aggregate", {}).get("visceral_proxy_volume_cm3")
        if isinstance(measurement.get("aggregate"), dict)
        else None,
        measurement.get("visceral_proxy_area_cm2"),
        measurement.get("opportunistic_osteoporosis_composite"),
        measurement.get("overall_suspicion"),
        measurement.get("job_status"),
    ]
    for candidate in candidates:
        if candidate not in (None, "", []):
            return str(candidate)
    return str(payload.get("status", "n/a"))


def process_case_metrics(case_input: Path) -> bool:
    case_id = case_input.name
    log_dir = study_logs_dir(case_id)
    log_dir.mkdir(parents=True, exist_ok=True)
    pipeline_log_path = None if settings.VERBOSE_CONSOLE else log_dir / "metrics_pipeline.log"
    logger = MetricsLogger(pipeline_log_path)

    try:
        metadata = _load_case_metadata(case_id)
        profile_name, profile = load_metrics_pipeline_profile()
        _validate_case_against_profile(case_id, metadata, profile_name, profile)
        jobs = [job for job in profile.get("jobs", []) if job.get("enabled", True)]
        if not jobs:
            logger.log(f"[Metrics] No enabled jobs for profile {profile_name}")
            logger.close()
            return True

        start_dt = datetime.datetime.now(LOCAL_TZ)
        logger.log(f"=== Metrics Case: {case_id} ===")
        logger.log(f"[Metrics] Profile: {profile_name}")
        logger.log(f"[Metrics] Jobs: {', '.join(job['name'] for job in jobs)}")

        pipeline = metadata.get("Pipeline", {})
        pipeline["metrics_start_time"] = start_dt.isoformat()
        pipeline["metrics_profile"] = profile_name
        metadata["Pipeline"] = pipeline
        _write_case_metadata(case_id, metadata)

        completed_jobs = []
        for job in jobs:
            job_name = job["name"]
            logger.log(f"[Metrics] Running {job_name}")
            payload = _run_job(case_id, job, log_dir)
            _upsert_results(case_id, job_name, payload, metadata)
            completed_jobs.append(
                {
                    "name": job_name,
                    "status": payload.get("status"),
                    "result_json": payload.get("artifacts", {}).get("result_json"),
                    "overlay_png": payload.get("artifacts", {}).get("overlay_png"),
                }
            )
            logger.log(f"[Metrics] ✓ {job_name} ({_payload_summary(payload)})")

        end_dt = datetime.datetime.now(LOCAL_TZ)
        pipeline = metadata.get("Pipeline", {})
        pipeline["metrics_end_time"] = end_dt.isoformat()
        pipeline["metrics_elapsed_time"] = str(end_dt - start_dt)
        pipeline["metrics_pipeline"] = {
            "profile": profile_name,
            "jobs": completed_jobs,
        }
        metadata["Pipeline"] = pipeline
        _write_case_metadata(case_id, metadata)
        logger.log(f"[Metrics] ✓ Complete ({pipeline['metrics_elapsed_time']})")
        logger.close()
        return True
    except Exception as exc:
        logger.log(f"[Metrics] Error for {case_id}: {exc}")
        logger.close()
        return False


def main() -> int:
    print("Starting metrics queue monitoring...")
    ensure_metrics_queue_table()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    processing_cases = set()
    lock = threading.Lock()

    def on_complete(fut, case_path: Path, queue_id: int | None = None):
        with lock:
            processing_cases.discard(case_path)
        try:
            ok = fut.result()
            if queue_id is not None:
                if ok:
                    mark_metrics_queue_item_done(queue_id)
                else:
                    mark_metrics_queue_item_error(queue_id, "Metrics finished with failure return status")
        except Exception as exc:
            if queue_id is not None:
                mark_metrics_queue_item_error(queue_id, exc)
            print(f"Error in metrics thread {case_path.name}: {exc}")

    try:
        while True:
            try:
                with lock:
                    busy = len(processing_cases) >= 1
                if not busy:
                    queue_item = claim_next_pending_metrics_queue_item()
                    if queue_item:
                        queue_id, _, input_path_str = queue_item
                        case_path = Path(input_path_str)
                        if not case_path.exists():
                            mark_metrics_queue_item_error(queue_id, f"Input path not found: {case_path}")
                        else:
                            with lock:
                                processing_cases.add(case_path)
                            future = executor.submit(process_case_metrics, case_path)
                            future.add_done_callback(
                                lambda fut, p=case_path, qid=queue_id: on_complete(fut, p, qid)
                            )

                time.sleep(settings.METRICS_SCAN_INTERVAL)
            except Exception as exc:
                print(f"Error in metrics main loop: {exc}")
                time.sleep(settings.METRICS_SCAN_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopping metrics monitoring...")
        executor.shutdown(wait=False)
        return 0
