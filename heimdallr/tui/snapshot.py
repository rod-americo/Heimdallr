"""Runtime snapshot builder for the Heimdallr Textual dashboard."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
import statistics
import subprocess
from typing import Any
from zoneinfo import ZoneInfo

from heimdallr.shared import settings
from heimdallr.shared.patient_names import normalize_patient_name_display


LOCAL_TZ = ZoneInfo(settings.TIMEZONE)


def _display_patient_name(name: str) -> str:
    normalized = normalize_patient_name_display(name, settings.PATIENT_NAME_PROFILE)
    return normalized.upper()


@dataclass(slots=True)
class RuntimeLayout:
    """Filesystem layout used by the dashboard."""

    runtime_dir: Path
    intake_dir: Path
    uploads_dir: Path
    uploads_failed_dir: Path
    dicom_incoming_dir: Path
    dicom_failed_dir: Path
    pending_dir: Path
    active_dir: Path
    failed_dir: Path
    studies_dir: Path

    @classmethod
    def from_settings(cls) -> "RuntimeLayout":
        return cls(
            runtime_dir=settings.RUNTIME_DIR,
            intake_dir=settings.INTAKE_DIR,
            uploads_dir=settings.UPLOAD_DIR,
            uploads_failed_dir=settings.UPLOAD_FAILED_DIR,
            dicom_incoming_dir=settings.DICOM_INCOMING_DIR,
            dicom_failed_dir=settings.DICOM_FAILED_DIR,
            pending_dir=settings.INPUT_DIR,
            active_dir=settings.PROCESSING_DIR,
            failed_dir=settings.ERROR_DIR,
            studies_dir=settings.STUDIES_DIR,
        )


@dataclass(slots=True)
class ServiceStatus:
    slug: str
    label: str
    running: bool
    instances: int
    summary: str
    details: list[str] = field(default_factory=list)


@dataclass(slots=True)
class StageMetrics:
    slug: str
    label: str
    state: str
    queued: int
    active: int
    completed: int
    failed: int
    oldest_age_seconds: float | None
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class CaseOverview:
    case_id: str
    patient_name: str
    modality: str
    accession_number: str
    study_date: str
    stage_label: str
    queue_status: str
    signal: str
    updated_at: datetime | None
    prepare_elapsed: str
    processing_elapsed: str
    total_elapsed: str
    selected_series: int
    discarded_series: int
    path: Path | None
    error: str
    sort_timestamp: float


@dataclass(slots=True)
class DashboardSnapshot:
    generated_at: datetime
    services: list[ServiceStatus]
    stages: list[StageMetrics]
    cases: list[CaseOverview]
    alerts: list[str]
    total_cases: int
    processed_cases: int
    backlog_cases: int
    failed_cases: int
    avg_prepare_seconds: float | None
    avg_processing_seconds: float | None


def build_snapshot(
    *,
    layout: RuntimeLayout | None = None,
    db_path: Path | None = None,
) -> DashboardSnapshot:
    """Assemble a full operational snapshot from runtime storage and SQLite."""
    layout = layout or RuntimeLayout.from_settings()
    db_path = db_path or settings.DB_PATH
    generated_at = datetime.now(LOCAL_TZ)

    queue_rows = _load_processing_queue(db_path)
    metadata_rows = _load_metadata_rows(db_path)
    process_scan = _scan_system_processes()
    study_entries = _load_studies(layout.studies_dir)
    pending_files = _collect_files(layout.pending_dir, suffix=".nii.gz")
    active_files = _collect_files(layout.active_dir, suffix=".nii.gz")
    failed_files = _collect_files(layout.failed_dir, suffix=".nii.gz")
    upload_files = _collect_files(layout.uploads_dir, suffix=".zip", include_claimed=False)
    claimed_uploads = _collect_files(layout.uploads_dir, suffix=".working", include_claimed=True)
    failed_uploads = _collect_files(layout.uploads_failed_dir, suffix=".zip", include_claimed=True)
    incoming_items = _collect_runtime_items(layout.dicom_incoming_dir)
    failed_dicom_items = _collect_runtime_items(layout.dicom_failed_dir)

    case_map: dict[str, dict[str, Any]] = {}
    prepare_samples: list[float] = []
    processing_samples: list[float] = []

    for row in metadata_rows:
        case_id = _case_id_from_metadata_row(row)
        if not case_id:
            continue
        case = case_map.setdefault(case_id, _empty_case(case_id))
        patient_name = row.get("PatientName") or case["patient_name"]
        case["patient_name"] = _display_patient_name(patient_name)
        case["modality"] = row.get("Modality") or case["modality"]
        case["accession_number"] = row.get("AccessionNumber") or case["accession_number"]
        case["study_date"] = row.get("StudyDate") or case["study_date"]
        processed_at = _parse_datetime(row.get("ProcessedAt"))
        case["updated_at"] = _max_datetime(case["updated_at"], processed_at)

    for case_id, study in study_entries.items():
        case = case_map.setdefault(case_id, _empty_case(case_id))
        case.update(
            {
                "patient_name": study.get("patient_name") or case["patient_name"],
                "modality": study.get("modality") or case["modality"],
                "accession_number": study.get("accession_number") or case["accession_number"],
                "study_date": study.get("study_date") or case["study_date"],
                "prepare_elapsed": study.get("prepare_elapsed") or case["prepare_elapsed"],
                "processing_elapsed": study.get("processing_elapsed") or case["processing_elapsed"],
                "total_elapsed": study.get("total_elapsed") or case["total_elapsed"],
                "selected_series": study.get("selected_series", case["selected_series"]),
                "discarded_series": study.get("discarded_series", case["discarded_series"]),
                "path": study.get("path") or case["path"],
                "updated_at": _max_datetime(case["updated_at"], study.get("updated_at")),
                "has_results": study.get("has_results", False),
                "has_error_log": study.get("has_error_log", False),
                "error": study.get("error") or case["error"],
            }
        )
        prepare_seconds = _duration_to_seconds(study.get("prepare_elapsed", ""))
        if prepare_seconds is not None:
            prepare_samples.append(prepare_seconds)
        processing_seconds = _duration_to_seconds(study.get("processing_elapsed", ""))
        if processing_seconds is not None:
            processing_samples.append(processing_seconds)

    queue_status_counts: dict[str, int] = {}
    queue_pending_times: list[datetime] = []
    processing_claim_times: list[datetime] = []

    for row in queue_rows:
        case_id = row["case_id"]
        case = case_map.setdefault(case_id, _empty_case(case_id))
        case["queue_status"] = row["status"]
        case["error"] = row["error"] or case["error"]
        case["updated_at"] = _max_datetime(
            case["updated_at"],
            _parse_datetime(row["finished_at"]) or _parse_datetime(row["claimed_at"]) or _parse_datetime(row["created_at"]),
        )
        queue_status_counts[row["status"]] = queue_status_counts.get(row["status"], 0) + 1
        if row["status"] == "pending":
            created_at = _parse_datetime(row["created_at"])
            if created_at is not None:
                queue_pending_times.append(created_at)
        if row["status"] == "claimed":
            claimed_at = _parse_datetime(row["claimed_at"])
            if claimed_at is not None:
                processing_claim_times.append(claimed_at)

    pending_case_ids = {_case_id_from_nifti(path) for path in pending_files}
    active_case_ids = {_case_id_from_nifti(path) for path in active_files}
    failed_case_ids = {_case_id_from_nifti(path) for path in failed_files}

    for case_id in pending_case_ids | active_case_ids | failed_case_ids:
        case = case_map.setdefault(case_id, _empty_case(case_id))
        if case_id in pending_case_ids:
            case["pending_file"] = True
        if case_id in active_case_ids:
            case["active_file"] = True
        if case_id in failed_case_ids:
            case["failed_file"] = True

    cases = [_finalize_case(case) for case in case_map.values()]
    cases.sort(key=lambda item: item.sort_timestamp, reverse=True)

    intake_stage = _build_intake_stage(
        upload_files=upload_files,
        claimed_uploads=claimed_uploads,
        failed_uploads=failed_uploads,
        incoming_items=incoming_items,
        failed_dicom_items=failed_dicom_items,
        services=process_scan,
        now=generated_at,
    )
    prepare_stage = _build_prepare_stage(
        cases=cases,
        claimed_uploads=claimed_uploads,
        failed_uploads=failed_uploads,
        services=process_scan,
        now=generated_at,
    )
    process_stage = _build_process_stage(
        cases=cases,
        pending_case_ids=pending_case_ids,
        active_case_ids=active_case_ids,
        failed_case_ids=failed_case_ids,
        queue_status_counts=queue_status_counts,
        queue_pending_times=queue_pending_times,
        processing_claim_times=processing_claim_times,
        services=process_scan,
        now=generated_at,
    )

    services = _build_services(process_scan, cases, intake_stage, prepare_stage, process_stage)
    alerts = _build_alerts(
        services=services,
        intake_stage=intake_stage,
        prepare_stage=prepare_stage,
        process_stage=process_stage,
        queue_status_counts=queue_status_counts,
    )

    processed_cases = sum(1 for case in cases if case.stage_label == "Processed")
    failed_cases = sum(1 for case in cases if case.stage_label == "Failed")
    backlog_cases = sum(1 for case in cases if case.stage_label in {"Queued", "Prepared", "Processing"})

    return DashboardSnapshot(
        generated_at=generated_at,
        services=services,
        stages=[intake_stage, prepare_stage, process_stage],
        cases=cases,
        alerts=alerts,
        total_cases=len(cases),
        processed_cases=processed_cases,
        backlog_cases=backlog_cases,
        failed_cases=failed_cases,
        avg_prepare_seconds=_safe_mean(prepare_samples),
        avg_processing_seconds=_safe_mean(processing_samples),
    )


def _build_services(
    process_scan: dict[str, list[dict[str, str]]],
    cases: list[CaseOverview],
    intake_stage: StageMetrics,
    prepare_stage: StageMetrics,
    process_stage: StageMetrics,
) -> list[ServiceStatus]:
    case_by_stage = {
        "intake": intake_stage,
        "prepare": prepare_stage,
        "processing": process_stage,
    }
    labels = {
        "intake": "Intake",
        "prepare": "Prepare",
        "processing": "Process",
    }
    statuses: list[ServiceStatus] = []
    for slug, processes in process_scan.items():
        stage = case_by_stage[slug]
        running = bool(processes)
        details = []
        for item in processes[:3]:
            details.append(f"PID {item['pid']} • up {item['etime']} • {item['command']}")
        if not details and stage.queued + stage.active == 0:
            details.append("No pipeline pressure detected.")
        elif not details:
            details.append("Backlog exists, but no matching worker process was found.")
        summary = "running" if running else "not detected"
        statuses.append(
            ServiceStatus(
                slug=slug,
                label=labels[slug],
                running=running,
                instances=len(processes),
                summary=summary,
                details=details,
            )
        )
    return statuses


def _build_intake_stage(
    *,
    upload_files: list[Path],
    claimed_uploads: list[Path],
    failed_uploads: list[Path],
    incoming_items: list[Path],
    failed_dicom_items: list[Path],
    services: dict[str, list[dict[str, str]]],
    now: datetime,
) -> StageMetrics:
    oldest = _oldest_age_seconds(upload_files + incoming_items + claimed_uploads, now)
    state = "flow"
    if failed_uploads or failed_dicom_items:
        state = "warning"
    if (upload_files or incoming_items) and not services["intake"]:
        state = "blocked"
    notes = [
        f"{len(upload_files)} ZIPs staged for prepare",
        f"{len(claimed_uploads)} uploads currently claimed",
        f"{len(incoming_items)} active DICOM landing items",
        f"{len(failed_uploads) + len(failed_dicom_items)} intake failures retained",
    ]
    if upload_files:
        notes.append(f"Oldest staged upload: {_friendly_age(oldest)}")
    return StageMetrics(
        slug="intake",
        label="Intake",
        state=state,
        queued=len(upload_files),
        active=len(claimed_uploads) + len(incoming_items),
        completed=0,
        failed=len(failed_uploads) + len(failed_dicom_items),
        oldest_age_seconds=oldest,
        notes=notes,
    )


def _build_prepare_stage(
    *,
    cases: list[CaseOverview],
    claimed_uploads: list[Path],
    failed_uploads: list[Path],
    services: dict[str, list[dict[str, str]]],
    now: datetime,
) -> StageMetrics:
    prepared = [case for case in cases if case.stage_label in {"Prepared", "Queued", "Processing", "Processed"}]
    queued = [case for case in cases if case.stage_label == "Prepared"]
    oldest = _oldest_case_age_seconds(queued, now)
    state = "flow"
    if failed_uploads:
        state = "warning"
    if claimed_uploads and not services["prepare"]:
        state = "blocked"
    notes = [
        f"{len(prepared)} studies have id.json and prepare context",
        f"{len(queued)} prepared studies are staged for processing",
        f"{sum(case.selected_series for case in prepared)} candidate series across prepared cases",
        f"{len(claimed_uploads)} uploads appear to be inside prepare right now",
    ]
    if queued:
        notes.append(f"Oldest prepared case waiting: {_friendly_age(oldest)}")
    return StageMetrics(
        slug="prepare",
        label="Prepare",
        state=state,
        queued=len(queued),
        active=len(claimed_uploads),
        completed=len(prepared),
        failed=len(failed_uploads),
        oldest_age_seconds=oldest,
        notes=notes,
    )


def _build_process_stage(
    *,
    cases: list[CaseOverview],
    pending_case_ids: set[str],
    active_case_ids: set[str],
    failed_case_ids: set[str],
    queue_status_counts: dict[str, int],
    queue_pending_times: list[datetime],
    processing_claim_times: list[datetime],
    services: dict[str, list[dict[str, str]]],
    now: datetime,
) -> StageMetrics:
    completed = [case for case in cases if case.stage_label == "Processed"]
    queued_cases = [case for case in cases if case.stage_label == "Queued"]
    active_cases = [case for case in cases if case.stage_label == "Processing"]
    failed_cases = [case for case in cases if case.stage_label == "Failed"]
    queued_ids = {case.case_id for case in queued_cases}
    active_ids = {case.case_id for case in active_cases}
    failed_ids = {case.case_id for case in failed_cases}
    oldest = _oldest_pending_from_timestamps(queue_pending_times + processing_claim_times, now)
    state = "flow"
    if failed_cases or failed_case_ids:
        state = "warning"
    if (queued_cases or active_cases) and not services["processing"]:
        state = "blocked"
    notes = [
        f"{len(queued_cases)} queued in processing table or pending folder",
        f"{len(active_cases)} running in active queue",
        f"{len(completed)} completed cases with results",
        f"{len(failed_cases)} cases marked as failed",
    ]
    if queue_status_counts:
        notes.append(
            "Queue states: "
            + ", ".join(f"{status}={count}" for status, count in sorted(queue_status_counts.items()))
        )
    if oldest is not None:
        notes.append(f"Oldest processing pressure: {_friendly_age(oldest)}")
    return StageMetrics(
        slug="processing",
        label="Process",
        state=state,
        queued=len(queued_ids | pending_case_ids),
        active=len(active_ids | active_case_ids),
        completed=len(completed),
        failed=len(failed_ids | failed_case_ids),
        oldest_age_seconds=oldest,
        notes=notes,
    )


def _build_alerts(
    *,
    services: list[ServiceStatus],
    intake_stage: StageMetrics,
    prepare_stage: StageMetrics,
    process_stage: StageMetrics,
    queue_status_counts: dict[str, int],
) -> list[str]:
    alerts: list[str] = []
    services_by_slug = {service.slug: service for service in services}

    if intake_stage.failed:
        alerts.append(f"Intake has {intake_stage.failed} retained failures.")
    if process_stage.queued and not services_by_slug["processing"].running:
        alerts.append("Processing backlog exists, but the processing worker is not detected.")
    if queue_status_counts.get("error"):
        alerts.append(f"{queue_status_counts['error']} processing queue item(s) are in error.")
    if intake_stage.oldest_age_seconds and intake_stage.oldest_age_seconds > 900:
        alerts.append("Intake backlog is older than 15 minutes.")
    if process_stage.oldest_age_seconds and process_stage.oldest_age_seconds > 1800:
        alerts.append("Processing pressure is older than 30 minutes.")
    if not alerts:
        alerts.append("No blocking condition detected in the current snapshot.")
    return alerts


def _finalize_case(case: dict[str, Any]) -> CaseOverview:
    stage_label = _derive_stage_label(case)
    signal = _derive_case_signal(case, stage_label)
    updated_at = case["updated_at"]
    sort_timestamp = updated_at.timestamp() if updated_at is not None else 0.0
    return CaseOverview(
        case_id=case["case_id"],
        patient_name=case["patient_name"] or "Unknown",
        modality=case["modality"] or "-",
        accession_number=case["accession_number"] or "-",
        study_date=case["study_date"] or "-",
        stage_label=stage_label,
        queue_status=case["queue_status"] or "-",
        signal=signal,
        updated_at=updated_at,
        prepare_elapsed=_display_duration(case["prepare_elapsed"]),
        processing_elapsed=_display_duration(case["processing_elapsed"]),
        total_elapsed=_display_duration(case["total_elapsed"] or case["processing_elapsed"] or case["prepare_elapsed"]),
        selected_series=case["selected_series"],
        discarded_series=case["discarded_series"],
        path=case["path"],
        error=case["error"] or "",
        sort_timestamp=sort_timestamp,
    )


def _derive_stage_label(case: dict[str, Any]) -> str:
    queue_status = case["queue_status"]
    if case["has_results"] or queue_status == "done":
        return "Processed"
    if queue_status == "error" or case["failed_file"] or case["has_error_log"]:
        return "Failed"
    if queue_status == "claimed" or case["active_file"]:
        return "Processing"
    if queue_status == "pending" or case["pending_file"]:
        return "Queued"
    if case["path"] is not None:
        return "Prepared"
    return "Intake"


def _derive_case_signal(case: dict[str, Any], stage_label: str) -> str:
    if case["error"]:
        return _truncate(case["error"], 52)
    if stage_label == "Processed":
        return "results.json ready"
    if stage_label == "Processing":
        return "segmentation + metrics"
    if stage_label == "Queued":
        return "waiting for processing slot"
    if stage_label == "Prepared":
        return f"{case['selected_series']} series ready"
    return "awaiting intake metadata"


def _empty_case(case_id: str) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "patient_name": "",
        "modality": "",
        "accession_number": "",
        "study_date": "",
        "queue_status": "",
        "updated_at": None,
        "prepare_elapsed": "",
        "processing_elapsed": "",
        "total_elapsed": "",
        "selected_series": 0,
        "discarded_series": 0,
        "path": None,
        "error": "",
        "has_results": False,
        "has_error_log": False,
        "pending_file": False,
        "active_file": False,
        "failed_file": False,
    }


def _load_processing_queue(db_path: Path) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    query = """
        SELECT case_id, status, created_at, claimed_at, finished_at, error
        FROM processing_queue
        ORDER BY COALESCE(finished_at, claimed_at, created_at) DESC
    """
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(query).fetchall()]
    except sqlite3.DatabaseError:
        return []


def _load_metadata_rows(db_path: Path) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    query = """
        SELECT StudyInstanceUID, PatientName, ClinicalName, AccessionNumber, StudyDate,
               Modality, IdJson, CalculationResults, ProcessedAt
        FROM dicom_metadata
        ORDER BY ProcessedAt DESC
    """
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(query).fetchall()]
    except sqlite3.DatabaseError:
        return []


def _load_studies(studies_dir: Path) -> dict[str, dict[str, Any]]:
    if not studies_dir.exists():
        return {}
    studies: dict[str, dict[str, Any]] = {}
    for study_dir in sorted(studies_dir.iterdir(), key=lambda item: item.stat().st_mtime, reverse=True):
        if not study_dir.is_dir():
            continue
        metadata_dir = study_dir / "metadata"
        id_json_path = metadata_dir / "id.json"
        results_path = metadata_dir / "resultados.json"
        log_error = study_dir / "logs" / "error.log"
        if not id_json_path.exists():
            continue
        payload = _read_json(id_json_path)
        case_id = payload.get("CaseID") or study_dir.name
        pipeline = payload.get("Pipeline", {})
        studies[case_id] = {
            "patient_name": _display_patient_name(payload.get("PatientName", "")),
            "modality": payload.get("Modality", ""),
            "accession_number": payload.get("AccessionNumber", ""),
            "study_date": payload.get("StudyDate", ""),
            "prepare_elapsed": pipeline.get("prepare_elapsed_time", ""),
            "processing_elapsed": pipeline.get("processing_elapsed_time", ""),
            "total_elapsed": pipeline.get("elapsed_time", ""),
            "selected_series": len(payload.get("AvailableSeries", [])),
            "discarded_series": len(payload.get("DiscardedSeries", [])),
            "updated_at": _latest_mtime([id_json_path, results_path if results_path.exists() else None, log_error if log_error.exists() else None]),
            "path": study_dir,
            "has_results": results_path.exists(),
            "has_error_log": log_error.exists(),
            "error": _tail_text(log_error, 200) if log_error.exists() else "",
        }
    return studies


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _tail_text(path: Path, limit: int) -> str:
    try:
        content = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return _truncate(" ".join(content.split()), limit)


def _collect_files(directory: Path, *, suffix: str, include_claimed: bool = False) -> list[Path]:
    if not directory.exists():
        return []
    files = [path for path in directory.iterdir() if path.is_file() and path.name.endswith(suffix)]
    if not include_claimed:
        files = [path for path in files if not path.name.endswith(".working")]
    return sorted(files, key=lambda item: item.stat().st_mtime, reverse=True)


def _collect_runtime_items(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    items = [path for path in directory.iterdir() if not path.name.startswith(".")]
    return sorted(items, key=lambda item: item.stat().st_mtime, reverse=True)


def _scan_system_processes() -> dict[str, list[dict[str, str]]]:
    groups = {"intake": [], "prepare": [], "processing": []}
    try:
        result = subprocess.run(
            ["ps", "-axo", "pid=,etime=,command="],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return groups
    if result.returncode != 0:
        return groups

    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(None, 2)
        if len(parts) != 3:
            continue
        pid, etime, command = parts
        normalized = command.lower()
        for slug, patterns in _service_patterns().items():
            if any(pattern in normalized for pattern in patterns):
                groups[slug].append({"pid": pid, "etime": etime, "command": _truncate(command, 92)})
                break
    return groups


def _service_patterns() -> dict[str, tuple[str, ...]]:
    return {
        "intake": ("-m heimdallr.intake", "heimdallr/intake", "services/dicom_listener.py"),
        "prepare": ("-m heimdallr.prepare", "heimdallr/prepare", "prepare/worker.py"),
        "processing": ("-m heimdallr.processing", "heimdallr/processing", "/run.py", " run.py"),
    }


def _case_id_from_metadata_row(row: dict[str, Any]) -> str:
    id_json = row.get("IdJson")
    if id_json:
        try:
            payload = json.loads(id_json)
            if isinstance(payload, dict) and payload.get("CaseID"):
                return str(payload["CaseID"])
        except json.JSONDecodeError:
            pass
    if row.get("ClinicalName"):
        return str(row["ClinicalName"])
    if row.get("StudyInstanceUID"):
        return str(row["StudyInstanceUID"])
    return ""


def _case_id_from_nifti(path: Path) -> str:
    name = path.name
    if name.endswith(".nii.gz"):
        return name[: -len(".nii.gz")]
    return path.stem


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=LOCAL_TZ)
        return value.astimezone(LOCAL_TZ)
    text = str(value).strip()
    if not text:
        return None
    candidates = (
        datetime.fromisoformat,
        lambda raw: datetime.strptime(raw, "%Y-%m-%d %H:%M:%S"),
        lambda raw: datetime.strptime(raw, "%Y%m%d"),
    )
    for parser in candidates:
        try:
            parsed = parser(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=LOCAL_TZ)
            return parsed.astimezone(LOCAL_TZ)
        except ValueError:
            continue
    return None


def _latest_mtime(paths: list[Path | None]) -> datetime | None:
    timestamps = []
    for path in paths:
        if path is None:
            continue
        try:
            timestamps.append(datetime.fromtimestamp(path.stat().st_mtime, tz=LOCAL_TZ))
        except OSError:
            continue
    return max(timestamps) if timestamps else None


def _max_datetime(left: datetime | None, right: datetime | None) -> datetime | None:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def _duration_to_seconds(value: str) -> float | None:
    if not value or value == "-":
        return None
    chunks = value.split(":")
    if len(chunks) != 3:
        return None
    try:
        hours = int(chunks[0])
        minutes = int(chunks[1])
        seconds = float(chunks[2])
    except ValueError:
        return None
    delta = timedelta(hours=hours, minutes=minutes, seconds=seconds)
    return delta.total_seconds()


def _display_duration(value: str | None) -> str:
    if not value:
        return "-"
    seconds = _duration_to_seconds(value)
    if seconds is None:
        return str(value)
    total = int(seconds)
    hours, remainder = divmod(total, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}:{minutes:02d}:{seconds:02d}"


def _safe_mean(values: list[float]) -> float | None:
    return statistics.mean(values) if values else None


def _oldest_age_seconds(paths: list[Path], now: datetime) -> float | None:
    ages = []
    for path in paths:
        try:
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=LOCAL_TZ)
        except OSError:
            continue
        ages.append((now - mtime).total_seconds())
    return max(ages) if ages else None


def _oldest_case_age_seconds(cases: list[CaseOverview], now: datetime) -> float | None:
    ages = []
    for case in cases:
        if case.updated_at is None:
            continue
        ages.append((now - case.updated_at).total_seconds())
    return max(ages) if ages else None


def _oldest_pending_from_timestamps(timestamps: list[datetime], now: datetime) -> float | None:
    if not timestamps:
        return None
    return max((now - timestamp).total_seconds() for timestamp in timestamps)


def _friendly_age(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"
    minutes = int(seconds // 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m"


def _truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"
