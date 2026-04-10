#!/usr/bin/env python3
"""Resident worker for post-segmentation Heimdallr metrics."""

from __future__ import annotations

import concurrent.futures
import datetime
import hashlib
import importlib.util
import json
import re
import subprocess
import threading
import time
from pathlib import Path
from zoneinfo import ZoneInfo

from heimdallr.dicom_egress.config import build_egress_queue_items
from heimdallr.shared import settings, store
from heimdallr.shared.paths import study_dir, study_id_json, study_logs_dir, study_results_json
from heimdallr.shared.sqlite import connect as db_connect

settings.configure_service_stdio()


LOCAL_TZ = ZoneInfo(settings.TIMEZONE)
JOB_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
JOB_MODULE_PREFIX = "heimdallr.metrics.jobs."
JOB_ALLOWED_MODULE_PREFIXES = (
    "heimdallr.metrics.jobs.",
    "heimdallr.metrics.analysis.",
)

# Metrics job resolution is intentionally convention-based so operational
# changes live in config/metrics_pipeline.json instead of this worker:
# - jobs[].name="l3_muscle_area" -> heimdallr.metrics.jobs.l3_muscle_area
# - jobs[].module may override the module path inside
#   heimdallr.metrics.jobs or heimdallr.metrics.analysis.
# This means toggling or parameterizing jobs in the JSON does not require a
# worker restart; only changes to this worker module itself do.

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


def _artifact_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
    module_name = _resolve_job_module_name(job)

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


def _resolve_job_module_name(job: dict) -> str:
    """Resolve a configured metrics job to an importable module path.

    Keep new jobs under `heimdallr.metrics.jobs` and prefer matching the
    filename to `jobs[].name` in config/metrics_pipeline.json. For example:

    1. Create `heimdallr/metrics/jobs/my_new_job.py`
    2. Add `{ "name": "my_new_job", ... }` to the metrics profile JSON
    3. The worker will resolve it automatically without further code changes

    `jobs[].module` remains available for rare cases where the JSON name should
    not match the module filename, including experimental jobs under
    `heimdallr.metrics.jobs.tests` or analysis entrypoints under
    `heimdallr.metrics.analysis`.
    """
    job_name = str(job.get("name", "") or "").strip()
    if not JOB_NAME_PATTERN.fullmatch(job_name):
        raise RuntimeError(f"Metrics job name is invalid: {job_name or '<empty>'}")

    configured_module = str(job.get("module", "") or "").strip()
    module_name = configured_module or f"{JOB_MODULE_PREFIX}{job_name}"
    if not any(module_name.startswith(prefix) for prefix in JOB_ALLOWED_MODULE_PREFIXES):
        raise RuntimeError(
            "Metrics job "
            f"'{job_name}' must resolve inside one of "
            f"{', '.join(prefix.rstrip('.') for prefix in JOB_ALLOWED_MODULE_PREFIXES)}"
        )
    if importlib.util.find_spec(module_name) is None:
        raise RuntimeError(f"Metrics job '{job_name}' module not found: {module_name}")
    return module_name


def _payload_summary(payload: dict) -> str:
    measurement = payload.get("measurement", {}) if isinstance(payload.get("measurement"), dict) else {}
    candidates = [
        measurement.get("skeletal_muscle_area_cm2"),
        measurement.get("exported_slice_count"),
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


def _normalize_dicom_exports(payload: dict) -> list[dict]:
    exports: list[dict] = []
    raw_exports = payload.get("dicom_exports")
    if isinstance(raw_exports, list):
        for item in raw_exports:
            if not isinstance(item, dict):
                continue
            path = str(item.get("path", "") or "").strip()
            kind = str(
                item.get("kind") or item.get("artifact_type") or item.get("type") or ""
            ).strip()
            if not path or not kind:
                continue
            exports.append({"path": path, "kind": kind})

    artifacts = payload.get("artifacts", {})
    if isinstance(artifacts, dict):
        legacy_overlay_path = str(artifacts.get("overlay_sc_dcm", "") or "").strip()
        if legacy_overlay_path and not any(
            export["path"] == legacy_overlay_path for export in exports
        ):
            exports.append(
                {
                    "path": legacy_overlay_path,
                    "kind": "secondary_capture",
                }
            )
    return exports


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
        if not need:
            continue
        if need in seen:
            continue
        normalized.append(need)
        seen.add(need)
    return normalized


def _resolve_enabled_jobs(profile: dict) -> list[dict]:
    jobs = [dict(job) for job in profile.get("jobs", []) if job.get("enabled", True)]
    seen_names: set[str] = set()
    for job in jobs:
        name = str(job.get("name", "") or "").strip()
        if not name:
            raise RuntimeError("Metrics job is missing a name")
        if name in seen_names:
            raise RuntimeError(f"Metrics profile contains duplicate job '{name}'")
        seen_names.add(name)
        job["name"] = name
        job["needs"] = _normalize_job_needs(job)
    return jobs


def _validate_job_dependency_graph(jobs: list[dict]) -> None:
    jobs_by_name = {job["name"]: job for job in jobs}
    for job in jobs:
        name = job["name"]
        for need in job["needs"]:
            if need not in jobs_by_name:
                raise RuntimeError(f"Metrics job '{name}' depends on missing job '{need}'")
            if need == name:
                raise RuntimeError(f"Metrics job '{name}' cannot depend on itself")

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visited:
            return
        if name in visiting:
            raise RuntimeError(f"Metrics job dependency cycle detected at '{name}'")
        visiting.add(name)
        for need in jobs_by_name[name]["needs"]:
            visit(need)
        visiting.remove(name)
        visited.add(name)

    for job in jobs:
        visit(job["name"])


def _resolve_max_parallel_jobs(profile: dict) -> int:
    execution = profile.get("execution", {})
    if not isinstance(execution, dict):
        execution = {}
    value = execution.get("max_parallel_jobs", 1)
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        raise RuntimeError(f"Invalid max_parallel_jobs value: {value!r}")


def _execute_jobs(
    case_id: str,
    jobs: list[dict],
    *,
    max_parallel_jobs: int,
    log_dir: Path,
    logger: MetricsLogger,
    metadata: dict,
) -> tuple[list[dict], list[dict]]:
    completed_jobs_by_name: dict[str, dict] = {}
    dicom_exports_by_name: dict[str, list[dict]] = {}
    job_payloads: dict[str, dict] = {}
    remaining_needs = {job["name"]: set(job["needs"]) for job in jobs}
    submitted: dict[str, concurrent.futures.Future] = {}
    future_to_name: dict[concurrent.futures.Future, str] = {}
    failed_job: tuple[str, Exception] | None = None
    job_names_in_order = [job["name"] for job in jobs]
    jobs_by_name = {job["name"]: job for job in jobs}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_jobs) as executor:
        while len(job_payloads) < len(jobs):
            if failed_job is None:
                for job_name in job_names_in_order:
                    if job_name in submitted or job_name in job_payloads:
                        continue
                    if remaining_needs[job_name]:
                        continue
                    logger.log(f"[Metrics] Running {job_name}")
                    future = executor.submit(_run_job, case_id, jobs_by_name[job_name], log_dir)
                    submitted[job_name] = future
                    future_to_name[future] = job_name
                    if len(submitted) >= max_parallel_jobs:
                        break

            if not future_to_name:
                if failed_job is not None:
                    break
                unresolved = {name: sorted(needs) for name, needs in remaining_needs.items() if name not in job_payloads}
                raise RuntimeError(f"Metrics job scheduling deadlock: {json.dumps(unresolved, ensure_ascii=False)}")

            done, _ = concurrent.futures.wait(
                list(future_to_name.keys()),
                return_when=concurrent.futures.FIRST_COMPLETED,
            )

            for future in done:
                job_name = future_to_name.pop(future)
                submitted.pop(job_name, None)
                try:
                    payload = future.result()
                except Exception as exc:
                    if failed_job is None:
                        failed_job = (job_name, exc)
                    continue

                job_payloads[job_name] = payload
                _upsert_results(case_id, job_name, payload, metadata)
                job_dicom_exports = _normalize_dicom_exports(payload)
                dicom_exports_by_name[job_name] = job_dicom_exports
                completed_jobs_by_name[job_name] = {
                    "name": job_name,
                    "status": payload.get("status"),
                    "result_json": payload.get("artifacts", {}).get("result_json"),
                    "overlay_png": payload.get("artifacts", {}).get("overlay_png"),
                    "dicom_exports": job_dicom_exports,
                }
                logger.log(f"[Metrics] ✓ {job_name} ({_payload_summary(payload)})")

                for dependent_name, needs in remaining_needs.items():
                    needs.discard(job_name)

        if failed_job is not None:
            failed_name, failed_exc = failed_job
            raise RuntimeError(f"Metrics job '{failed_name}' failed: {failed_exc}") from failed_exc

    completed_jobs = [completed_jobs_by_name[name] for name in job_names_in_order if name in completed_jobs_by_name]
    dicom_exports: list[dict] = []
    for name in job_names_in_order:
        dicom_exports.extend(dicom_exports_by_name.get(name, []))
    return completed_jobs, dicom_exports


def _enqueue_case_dicom_exports(
    case_id: str,
    metadata: dict,
    dicom_exports: list[dict],
    logger: MetricsLogger,
) -> int:
    if not dicom_exports:
        return 0

    queue_items = build_egress_queue_items(case_id, metadata, dicom_exports)
    if not queue_items:
        logger.log("[Metrics] No eligible DICOM egress destinations for generated artifacts")
        return 0

    enqueued = 0
    conn = db_connect()
    try:
        for item in queue_items:
            artifact_abspath = study_dir(case_id) / item["artifact_path"]
            if not artifact_abspath.exists():
                logger.log(f"[Metrics] Skipping missing DICOM artifact: {artifact_abspath}")
                continue
            store.enqueue_dicom_export(
                conn,
                **item,
                artifact_digest=_artifact_sha256(artifact_abspath),
            )
            enqueued += 1
    finally:
        conn.close()

    logger.log(f"[Metrics] Enqueued {enqueued} DICOM exports")
    return enqueued


def segment_case_metrics(case_input: Path) -> bool:
    case_id = case_input.name
    log_dir = study_logs_dir(case_id)
    log_dir.mkdir(parents=True, exist_ok=True)
    pipeline_log_path = None if settings.VERBOSE_CONSOLE else log_dir / "metrics_pipeline.log"
    logger = MetricsLogger(pipeline_log_path)

    try:
        metadata = _load_case_metadata(case_id)
        profile_name, profile = load_metrics_pipeline_profile()
        _validate_case_against_profile(case_id, metadata, profile_name, profile)
        jobs = _resolve_enabled_jobs(profile)
        if not jobs:
            logger.log(f"[Metrics] No enabled jobs for profile {profile_name}")
            logger.close()
            return True
        _validate_job_dependency_graph(jobs)
        max_parallel_jobs = _resolve_max_parallel_jobs(profile)

        start_dt = datetime.datetime.now(LOCAL_TZ)
        logger.log(f"=== Metrics Case: {case_id} ===")
        logger.log(f"[Metrics] Profile: {profile_name}")
        logger.log(f"[Metrics] Jobs: {', '.join(job['name'] for job in jobs)}")
        logger.log(f"[Metrics] Max parallel jobs: {max_parallel_jobs}")

        pipeline = metadata.get("Pipeline", {})
        pipeline["metrics_start_time"] = start_dt.isoformat()
        pipeline["metrics_profile"] = profile_name
        metadata["Pipeline"] = pipeline
        _write_case_metadata(case_id, metadata)

        completed_jobs, dicom_exports = _execute_jobs(
            case_id,
            jobs,
            max_parallel_jobs=max_parallel_jobs,
            log_dir=log_dir,
            logger=logger,
            metadata=metadata,
        )

        try:
            enqueued_dicom_exports = _enqueue_case_dicom_exports(
                case_id,
                metadata,
                dicom_exports,
                logger,
            )
        except Exception as exc:
            enqueued_dicom_exports = 0
            logger.log(f"[Metrics] Warning: failed to enqueue DICOM egress items: {exc}")

        end_dt = datetime.datetime.now(LOCAL_TZ)
        pipeline = metadata.get("Pipeline", {})
        pipeline["metrics_end_time"] = end_dt.isoformat()
        pipeline["metrics_elapsed_time"] = str(end_dt - start_dt)
        pipeline["metrics_pipeline"] = {
            "profile": profile_name,
            "max_parallel_jobs": max_parallel_jobs,
            "jobs": completed_jobs,
            "dicom_egress_items_enqueued": enqueued_dicom_exports,
        }
        metadata["Pipeline"] = pipeline
        _write_case_metadata(case_id, metadata)
        study_uid = str(metadata.get("StudyInstanceUID", "") or "").strip()
        if study_uid:
            conn = db_connect()
            try:
                store.update_metrics_completion(
                    conn,
                    study_uid,
                    profile_name=profile_name,
                )
            finally:
                conn.close()
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
    segmentation_cases = set()
    lock = threading.Lock()

    def on_complete(fut, case_path: Path, queue_id: int | None = None):
        with lock:
            segmentation_cases.discard(case_path)
        try:
            ok = fut.result()
            print(f"[Metrics] {'Done' if ok else 'Failed'}: {case_path.name}")
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
                    busy = len(segmentation_cases) >= 1
                if not busy:
                    queue_item = claim_next_pending_metrics_queue_item()
                    if queue_item:
                        queue_id, _, input_path_str = queue_item
                        case_path = Path(input_path_str)
                        if not case_path.exists():
                            mark_metrics_queue_item_error(queue_id, f"Input path not found: {case_path}")
                        else:
                            print(f"[Metrics] Claimed queue item: {case_path.name}")
                            with lock:
                                segmentation_cases.add(case_path)
                            future = executor.submit(segment_case_metrics, case_path)
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
