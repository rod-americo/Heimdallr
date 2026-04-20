"""Runtime snapshot builder for the Heimdallr Textual dashboard."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
from pathlib import Path
import shutil
import sqlite3
import statistics
import subprocess
from typing import Any
from zoneinfo import ZoneInfo

from heimdallr.shared import settings
from heimdallr.shared.patient_names import normalize_patient_name_display
from .i18n import no_data, queue_status_label, service_label, stage_label, tui


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
            active_dir=settings.SEGMENTATION_DIR,
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
    newest_age_seconds: float | None = None
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class CaseOverview:
    case_id: str
    patient_name: str
    origin: str
    modality: str
    accession_number: str
    study_date: str
    stage_key: str
    stage_label: str
    queue_status_key: str
    queue_status: str
    signal: str
    updated_at: datetime | None
    prepare_elapsed: str
    segmentation_elapsed: str
    metrics_elapsed: str
    total_elapsed: str
    selected_series: int
    discarded_series: int
    path: Path | None
    error: str
    segmentation_status: str
    sort_timestamp: float


@dataclass(slots=True)
class AlertItem:
    level: str
    message: str


@dataclass(slots=True)
class DashboardSnapshot:
    generated_at: datetime
    services: list[ServiceStatus]
    stages: list[StageMetrics]
    cases: list[CaseOverview]
    alerts: list[AlertItem]
    total_cases: int
    processed_cases: int
    backlog_cases: int
    failed_cases: int
    avg_prepare_seconds: float | None
    avg_segmentation_seconds: float | None
    avg_metrics_seconds: float | None


def build_snapshot(
    *,
    layout: RuntimeLayout | None = None,
    db_path: Path | None = None,
) -> DashboardSnapshot:
    """Assemble a full operational snapshot from runtime storage and SQLite."""
    layout = layout or RuntimeLayout.from_settings()
    db_path = db_path or settings.DB_PATH
    generated_at = datetime.now(LOCAL_TZ)

    segmentation_queue_rows = _load_segmentation_queue(db_path)
    metrics_queue_rows = _load_metrics_queue(db_path)
    metadata_rows = _load_metadata_rows(db_path)
    process_scan = _scan_system_processes()
    study_entries = _load_studies(layout.studies_dir, generated_at)
    pending_files = _collect_files(layout.pending_dir, suffix=".nii.gz")
    active_files = _collect_files(layout.active_dir, suffix=".nii.gz")
    failed_files = _collect_files(layout.failed_dir, suffix=".nii.gz")
    upload_files = _collect_files(layout.uploads_dir, suffix=".zip", include_claimed=False, recursive=True)
    claimed_uploads = _collect_files(layout.uploads_dir, suffix=".working", include_claimed=True, recursive=True)
    failed_uploads = _collect_files(layout.uploads_failed_dir, suffix=".zip", include_claimed=True)
    incoming_items = _collect_runtime_items(layout.dicom_incoming_dir)
    failed_dicom_items = _collect_runtime_items(layout.dicom_failed_dir)

    case_map: dict[str, dict[str, Any]] = {}
    prepare_samples: list[float] = []
    segmentation_samples: list[float] = []
    metrics_samples: list[float] = []

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
                "origin": study.get("origin") or case["origin"],
                "modality": study.get("modality") or case["modality"],
                "accession_number": study.get("accession_number") or case["accession_number"],
                "study_date": study.get("study_date") or case["study_date"],
                "prepare_elapsed": study.get("prepare_elapsed") or case["prepare_elapsed"],
                "segmentation_elapsed": study.get("segmentation_elapsed") or case["segmentation_elapsed"],
                "metrics_elapsed": study.get("metrics_elapsed") or case["metrics_elapsed"],
                "total_elapsed": study.get("total_elapsed") or case["total_elapsed"],
                "segmentation_reused": study.get("segmentation_reused", case["segmentation_reused"]),
                "segmentation_status": study.get("segmentation_status") or case["segmentation_status"],
                "segmentation_reuse_reason": study.get(
                    "segmentation_reuse_reason",
                    case["segmentation_reuse_reason"],
                ),
                "segmentation_original_elapsed": study.get(
                    "segmentation_original_elapsed",
                    case["segmentation_original_elapsed"],
                ),
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
        segmentation_seconds = _duration_to_seconds(study.get("segmentation_elapsed", ""))
        if segmentation_seconds is not None:
            segmentation_samples.append(segmentation_seconds)
        metrics_seconds = _duration_to_seconds(study.get("metrics_elapsed", ""))
        if metrics_seconds is not None:
            metrics_samples.append(metrics_seconds)

    segmentation_queue_status_counts: dict[str, int] = {}
    metrics_queue_status_counts: dict[str, int] = {}
    segmentation_queue_pending_times: list[datetime] = []
    segmentation_claim_times: list[datetime] = []
    metrics_queue_pending_times: list[datetime] = []
    metrics_claim_times: list[datetime] = []

    for row in segmentation_queue_rows:
        case_id = row["case_id"]
        case = case_map.setdefault(case_id, _empty_case(case_id))
        case["segmentation_queue_status"] = row["status"]
        case["error"] = row["error"] or case["error"]
        case["updated_at"] = _max_datetime(
            case["updated_at"],
            _parse_datetime(row["finished_at"]) or _parse_datetime(row["claimed_at"]) or _parse_datetime(row["created_at"]),
        )
        segmentation_queue_status_counts[row["status"]] = segmentation_queue_status_counts.get(row["status"], 0) + 1
        if row["status"] == "pending":
            created_at = _parse_datetime(row["created_at"])
            if created_at is not None:
                segmentation_queue_pending_times.append(created_at)
        if row["status"] == "claimed":
            claimed_at = _parse_datetime(row["claimed_at"])
            if claimed_at is not None:
                segmentation_claim_times.append(claimed_at)

    for row in metrics_queue_rows:
        case_id = row["case_id"]
        case = case_map.setdefault(case_id, _empty_case(case_id))
        case["metrics_queue_status"] = row["status"]
        case["error"] = row["error"] or case["error"]
        case["updated_at"] = _max_datetime(
            case["updated_at"],
            _parse_datetime(row["finished_at"]) or _parse_datetime(row["claimed_at"]) or _parse_datetime(row["created_at"]),
        )
        metrics_queue_status_counts[row["status"]] = metrics_queue_status_counts.get(row["status"], 0) + 1
        if row["status"] == "pending":
            created_at = _parse_datetime(row["created_at"])
            if created_at is not None:
                metrics_queue_pending_times.append(created_at)
        if row["status"] == "claimed":
            claimed_at = _parse_datetime(row["claimed_at"])
            if claimed_at is not None:
                metrics_claim_times.append(claimed_at)

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
    segmentation_stage = _build_segmentation_stage(
        cases=cases,
        pending_case_ids=pending_case_ids,
        active_case_ids=active_case_ids,
        failed_case_ids=failed_case_ids,
        queue_status_counts=segmentation_queue_status_counts,
        queue_pending_times=segmentation_queue_pending_times,
        segmentation_claim_times=segmentation_claim_times,
        services=process_scan,
        now=generated_at,
    )
    metrics_stage = _build_metrics_stage(
        cases=cases,
        queue_status_counts=metrics_queue_status_counts,
        queue_pending_times=metrics_queue_pending_times,
        metrics_claim_times=metrics_claim_times,
        services=process_scan,
        now=generated_at,
    )

    services = _build_services(process_scan, cases, intake_stage, prepare_stage, segmentation_stage, metrics_stage)
    alerts = _build_alerts(
        services=services,
        intake_stage=intake_stage,
        prepare_stage=prepare_stage,
        segmentation_stage=segmentation_stage,
        metrics_stage=metrics_stage,
        segmentation_queue_status_counts=segmentation_queue_status_counts,
        metrics_queue_status_counts=metrics_queue_status_counts,
    )

    processed_cases = sum(1 for case in cases if case.stage_key == "processed")
    failed_cases = sum(1 for case in cases if case.stage_key == "failed")
    backlog_cases = sum(1 for case in cases if case.stage_key in {"queued", "prepared", "segmentation", "metrics"})

    return DashboardSnapshot(
        generated_at=generated_at,
        services=services,
        stages=[intake_stage, prepare_stage, segmentation_stage, metrics_stage],
        cases=cases,
        alerts=alerts,
        total_cases=len(cases),
        processed_cases=processed_cases,
        backlog_cases=backlog_cases,
        failed_cases=failed_cases,
        avg_prepare_seconds=_safe_mean(prepare_samples),
        avg_segmentation_seconds=_safe_mean(segmentation_samples),
        avg_metrics_seconds=_safe_mean(metrics_samples),
    )


def _build_services(
    process_scan: dict[str, list[dict[str, str]]],
    cases: list[CaseOverview],
    intake_stage: StageMetrics,
    prepare_stage: StageMetrics,
    segmentation_stage: StageMetrics,
    metrics_stage: StageMetrics,
) -> list[ServiceStatus]:
    case_by_stage = {
        "intake": intake_stage,
        "prepare": prepare_stage,
        "segmentation": segmentation_stage,
        "metrics": metrics_stage,
    }
    labels = {
        "intake": service_label("intake"),
        "prepare": service_label("prepare"),
        "segmentation": service_label("segmentation"),
        "metrics": service_label("metrics"),
        "space_manager": service_label("space_manager"),
    }
    statuses: list[ServiceStatus] = []
    for slug, processes in process_scan.items():
        stage = case_by_stage.get(slug)
        running = bool(processes)
        details = []
        if slug == "intake" and processes:
            details.append(_build_intake_service_detail(processes[0], intake_stage))
        elif slug == "space_manager" and processes:
            details.append(_build_space_manager_service_detail(processes[0]))
        for item in processes[:3]:
            if slug in {"intake", "space_manager"} and details:
                break
            details.append(
                tui("snapshot.service.detail", pid=item["pid"], etime=item["etime"], command=item["command"])
            )
        if not details and (stage is None or stage.queued + stage.active == 0):
            details.append(tui("snapshot.service.no_pressure"))
        elif not details:
            details.append(tui("snapshot.service.backlog_without_worker"))
        summary = tui("snapshot.service.running") if running else tui("snapshot.service.not_detected")
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
    oldest_incoming = _oldest_age_seconds(incoming_items, now)
    newest_incoming = _newest_age_seconds(incoming_items, now)
    state = "flow"
    if (upload_files or incoming_items) and not services["intake"]:
        state = "blocked"
    notes = [
        tui(
            "snapshot.intake.note.idle_window",
            idle=_friendly_age(float(settings.DICOM_IDLE_SECONDS)),
        ),
    ]
    if incoming_items and oldest_incoming is not None and oldest_incoming < settings.DICOM_IDLE_SECONDS:
        notes.append(
            tui(
                "snapshot.intake.note.awaiting_idle_window",
                remaining=_friendly_age(settings.DICOM_IDLE_SECONDS - oldest_incoming),
            )
        )
    notes.extend(
        [
            tui("snapshot.intake.note.zips_staged", count=len(upload_files)),
            tui("snapshot.intake.note.uploads_claimed", count=len(claimed_uploads)),
            tui("snapshot.intake.note.active_dicom", count=len(incoming_items)),
            tui("snapshot.intake.note.failures_retained", count=len(failed_uploads) + len(failed_dicom_items)),
        ]
    )
    if upload_files:
        notes.append(tui("snapshot.intake.note.oldest_upload", age=_friendly_age(oldest)))
    return StageMetrics(
        slug="intake",
        label=service_label("intake"),
        state=state,
        queued=len(upload_files),
        active=len(claimed_uploads) + len(incoming_items),
        completed=0,
        failed=0,
        oldest_age_seconds=oldest,
        newest_age_seconds=newest_incoming,
        notes=notes,
    )


def _build_intake_service_detail(process: dict[str, str], stage: StageMetrics) -> str:
    detail = tui("snapshot.service.detail_short", pid=process["pid"], etime=process["etime"])
    active_age = stage.newest_age_seconds
    if stage.active and active_age is not None:
        since_last = _friendly_age(active_age)
        remaining_seconds = max(0.0, float(settings.DICOM_IDLE_SECONDS) - active_age)
        remaining = _friendly_age(remaining_seconds)
        return tui(
            "snapshot.service.intake_wait_detail",
            detail=detail,
            since_last=since_last,
            remaining=remaining,
        )
    return tui(
        "snapshot.service.intake_idle_detail",
        detail=detail,
        idle=_friendly_age(float(settings.DICOM_IDLE_SECONDS)),
    )


def _build_space_manager_service_detail(process: dict[str, str]) -> str:
    detail = tui("snapshot.service.detail_short", pid=process["pid"], etime=process["etime"])
    free_bytes = _disk_free_bytes(settings.STUDIES_DIR)
    return tui(
        "snapshot.service.space_detail",
        detail=detail,
        free=_bytes_human(free_bytes) if free_bytes is not None else no_data(),
    )


def _build_prepare_stage(
    *,
    cases: list[CaseOverview],
    claimed_uploads: list[Path],
    failed_uploads: list[Path],
    services: dict[str, list[dict[str, str]]],
    now: datetime,
) -> StageMetrics:
    prepared = [case for case in cases if case.stage_key in {"prepared", "queued", "segmentation", "processed"}]
    queued = [case for case in cases if case.stage_key == "prepared"]
    oldest = _oldest_case_age_seconds(queued, now)
    state = "flow"
    if claimed_uploads and not services["prepare"]:
        state = "blocked"
    notes = [
        tui("snapshot.prepare.note.studies_prepared", count=len(prepared)),
        tui("snapshot.prepare.note.studies_staged", count=len(queued)),
        tui("snapshot.prepare.note.candidate_series", count=sum(case.selected_series for case in prepared)),
        tui("snapshot.prepare.note.uploads_inside_prepare", count=len(claimed_uploads)),
    ]
    if queued:
        notes.append(tui("snapshot.prepare.note.oldest_waiting", age=_friendly_age(oldest)))
    return StageMetrics(
        slug="prepare",
        label=service_label("prepare"),
        state=state,
        queued=len(queued),
        active=len(claimed_uploads),
        completed=len(prepared),
        failed=0,
        oldest_age_seconds=oldest,
        newest_age_seconds=None,
        notes=notes,
    )


def _build_segmentation_stage(
    *,
    cases: list[CaseOverview],
    pending_case_ids: set[str],
    active_case_ids: set[str],
    failed_case_ids: set[str],
    queue_status_counts: dict[str, int],
    queue_pending_times: list[datetime],
    segmentation_claim_times: list[datetime],
    services: dict[str, list[dict[str, str]]],
    now: datetime,
) -> StageMetrics:
    completed = [case for case in cases if case.stage_key == "processed"]
    queued_cases = [case for case in cases if case.stage_key == "queued"]
    active_cases = [case for case in cases if case.stage_key == "segmentation"]
    failed_cases = [case for case in cases if case.stage_key == "failed"]
    queued_ids = {case.case_id for case in queued_cases}
    active_ids = {case.case_id for case in active_cases}
    failed_ids = {case.case_id for case in failed_cases}
    oldest = _oldest_pending_from_timestamps(queue_pending_times + segmentation_claim_times, now)
    state = "flow"
    if failed_cases or failed_case_ids:
        state = "warning"
    if (queued_cases or active_cases) and not services["segmentation"]:
        state = "blocked"
    notes = [
        tui("snapshot.segmentation.note.queued", count=len(queued_cases)),
        tui("snapshot.segmentation.note.active", count=len(active_cases)),
        tui("snapshot.segmentation.note.completed", count=len(completed)),
        tui("snapshot.segmentation.note.failed", count=len(failed_cases)),
    ]
    if queue_status_counts:
        notes.append(
            tui(
                "snapshot.segmentation.note.queue_states",
                value=", ".join(
                    f"{queue_status_label(status)}={count}" for status, count in sorted(queue_status_counts.items())
                ),
            )
        )
    if oldest is not None:
        notes.append(tui("snapshot.segmentation.note.oldest_pressure", age=_friendly_age(oldest)))
    return StageMetrics(
        slug="segmentation",
        label=service_label("segmentation"),
        state=state,
        queued=len(queued_ids | pending_case_ids),
        active=len(active_ids | active_case_ids),
        completed=len(completed),
        failed=len(failed_ids | failed_case_ids),
        oldest_age_seconds=oldest,
        newest_age_seconds=None,
        notes=notes,
    )


def _build_metrics_stage(
    *,
    cases: list[CaseOverview],
    queue_status_counts: dict[str, int],
    queue_pending_times: list[datetime],
    metrics_claim_times: list[datetime],
    services: dict[str, list[dict[str, str]]],
    now: datetime,
) -> StageMetrics:
    queued_cases = [case for case in cases if case.stage_key == "metrics" and case.queue_status_key == "pending"]
    active_cases = [case for case in cases if case.stage_key == "metrics" and case.queue_status_key == "claimed"]
    completed = [case for case in cases if case.stage_key == "processed"]
    failed_cases = [case for case in cases if case.stage_key == "failed" and case.queue_status_key in {"error", "claimed"}]
    oldest = _oldest_pending_from_timestamps(queue_pending_times + metrics_claim_times, now)
    state = "flow"
    if failed_cases:
        state = "warning"
    if (queued_cases or active_cases) and not services["metrics"]:
        state = "blocked"
    notes = [
        tui("snapshot.metrics.note.queued", count=len(queued_cases)),
        tui("snapshot.metrics.note.active", count=len(active_cases)),
        tui("snapshot.metrics.note.completed", count=len(completed)),
        tui("snapshot.metrics.note.failed", count=len(failed_cases)),
    ]
    if queue_status_counts:
        notes.append(
            tui(
                "snapshot.metrics.note.queue_states",
                value=", ".join(
                    f"{queue_status_label(status)}={count}" for status, count in sorted(queue_status_counts.items())
                ),
            )
        )
    if oldest is not None:
        notes.append(tui("snapshot.metrics.note.oldest_pressure", age=_friendly_age(oldest)))
    return StageMetrics(
        slug="metrics",
        label=service_label("metrics"),
        state=state,
        queued=queue_status_counts.get("pending", 0),
        active=queue_status_counts.get("claimed", 0),
        completed=len(completed),
        failed=queue_status_counts.get("error", 0),
        oldest_age_seconds=oldest,
        newest_age_seconds=None,
        notes=notes,
    )


def _build_alerts(
    *,
    services: list[ServiceStatus],
    intake_stage: StageMetrics,
    prepare_stage: StageMetrics,
    segmentation_stage: StageMetrics,
    metrics_stage: StageMetrics,
    segmentation_queue_status_counts: dict[str, int],
    metrics_queue_status_counts: dict[str, int],
) -> list[AlertItem]:
    alerts: list[AlertItem] = []
    services_by_slug = {service.slug: service for service in services}

    if segmentation_stage.queued and not services_by_slug["segmentation"].running:
        alerts.append(AlertItem("warning", tui("snapshot.alert.segmentation_backlog_no_worker")))
    if metrics_stage.queued and not services_by_slug["metrics"].running:
        alerts.append(AlertItem("warning", tui("snapshot.alert.metrics_backlog_no_worker")))
    if segmentation_queue_status_counts.get("error"):
        alerts.append(AlertItem("warning", tui("snapshot.alert.segmentation_queue_errors", count=segmentation_queue_status_counts["error"])))
    if metrics_queue_status_counts.get("error"):
        alerts.append(AlertItem("warning", tui("snapshot.alert.metrics_queue_errors", count=metrics_queue_status_counts["error"])))
    if intake_stage.oldest_age_seconds and intake_stage.oldest_age_seconds > 900:
        alerts.append(AlertItem("warning", tui("snapshot.alert.intake_backlog_old")))
    if segmentation_stage.oldest_age_seconds and segmentation_stage.oldest_age_seconds > 1800:
        alerts.append(AlertItem("warning", tui("snapshot.alert.segmentation_old")))
    if metrics_stage.oldest_age_seconds and metrics_stage.oldest_age_seconds > 1800:
        alerts.append(AlertItem("warning", tui("snapshot.alert.metrics_old")))
    if not alerts:
        alerts.append(AlertItem("ok", tui("snapshot.alert.no_blocking")))
    return alerts


def _finalize_case(case: dict[str, Any]) -> CaseOverview:
    stage_key = _derive_stage_key(case)
    signal = _derive_case_signal(case, stage_key)
    queue_status_key = _display_queue_status_key(case, stage_key)
    updated_at = case["updated_at"]
    sort_timestamp = updated_at.timestamp() if updated_at is not None else 0.0
    return CaseOverview(
        case_id=case["case_id"],
        patient_name=case["patient_name"] or tui("snapshot.case.unknown"),
        origin="" if stage_key == "processed" else case["origin"],
        modality=case["modality"] or "-",
        accession_number=case["accession_number"] or "-",
        study_date=case["study_date"] or "-",
        stage_key=stage_key,
        stage_label=stage_label(stage_key),
        queue_status_key=queue_status_key,
        queue_status=queue_status_label(queue_status_key),
        signal=signal,
        updated_at=updated_at,
        prepare_elapsed=_display_duration(case["prepare_elapsed"]),
        segmentation_elapsed=_display_segmentation_elapsed(case),
        metrics_elapsed=_display_duration(case["metrics_elapsed"]),
        total_elapsed=_display_duration(case["total_elapsed"] or case["segmentation_elapsed"] or case["prepare_elapsed"]),
        selected_series=case["selected_series"],
        discarded_series=case["discarded_series"],
        path=case["path"],
        error=case["error"] or "",
        segmentation_status=case.get("segmentation_status", "") or "",
        sort_timestamp=sort_timestamp,
    )


def _derive_stage_key(case: dict[str, Any]) -> str:
    segmentation_queue_status = case["segmentation_queue_status"]
    metrics_queue_status = case["metrics_queue_status"]
    metrics_started = bool(case.get("metrics_started"))
    metrics_finished = bool(case.get("metrics_finished"))
    if case["has_results"] or metrics_queue_status == "done" or metrics_finished:
        return "processed"
    if metrics_queue_status in {"pending", "claimed"} or (metrics_started and not metrics_finished):
        return "metrics"
    if segmentation_queue_status == "claimed" or case["active_file"]:
        return "segmentation"
    if segmentation_queue_status == "pending" or case["pending_file"]:
        return "queued"
    if case.get("segmentation_status") == "ineligible":
        return "ineligible"
    if segmentation_queue_status == "error" or metrics_queue_status == "error" or case["failed_file"] or case["has_error_log"]:
        return "failed"
    if case["path"] is not None:
        return "prepared"
    return "intake"


def _derive_case_signal(case: dict[str, Any], stage_key: str) -> str:
    if stage_key == "ineligible":
        return tui("snapshot.case.signal.ineligible")
    if case["error"]:
        return _truncate(case["error"], 52)
    if case["segmentation_reused"]:
        reuse_reason = str(case.get("segmentation_reuse_reason", "") or "")
        if reuse_reason.startswith("prepare_duplicate_"):
            if stage_key == "processed":
                return tui("snapshot.case.signal.results_ready_prepare_duplicate")
            if stage_key == "metrics":
                return tui("snapshot.case.signal.metrics_running_prepare_duplicate")
            return tui("snapshot.case.signal.prepare_duplicate_skipped")
        if stage_key == "processed":
            return tui("snapshot.case.signal.results_ready_reused")
        if stage_key == "metrics":
            return tui("snapshot.case.signal.metrics_running_reused")
        return tui("snapshot.case.signal.segmentation_reused")
    if stage_key == "processed":
        return tui("snapshot.case.signal.results_ready")
    if stage_key == "metrics":
        return tui("snapshot.case.signal.metrics_running")
    if stage_key == "segmentation":
        return tui("snapshot.case.signal.segmentation_metrics")
    if stage_key == "queued":
        return tui("snapshot.case.signal.waiting_slot")
    if stage_key == "prepared":
        return tui("snapshot.case.signal.series_ready", count=case["selected_series"])
    return tui("snapshot.case.signal.awaiting_metadata")


def _display_queue_status_key(case: dict[str, Any], stage_key: str) -> str:
    if stage_key == "ineligible":
        return ""
    if stage_key in {"metrics", "processed"} and case["metrics_queue_status"]:
        return case["metrics_queue_status"]
    if case["segmentation_queue_status"]:
        return case["segmentation_queue_status"]
    return ""


def _empty_case(case_id: str) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "patient_name": "",
        "origin": "",
        "modality": "",
        "accession_number": "",
        "study_date": "",
        "segmentation_queue_status": "",
        "metrics_queue_status": "",
        "updated_at": None,
        "prepare_elapsed": "",
        "segmentation_elapsed": "",
        "metrics_elapsed": "",
        "total_elapsed": "",
        "metrics_started": False,
        "metrics_finished": False,
        "segmentation_status": "",
        "segmentation_reused": False,
        "segmentation_reuse_reason": "",
        "segmentation_original_elapsed": "",
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


def _normalize_case_origin(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized in {"P", "E"}:
        return normalized
    return ""


def _load_segmentation_queue(db_path: Path) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    query = """
        SELECT case_id, status, created_at, claimed_at, finished_at, error
        FROM segmentation_queue
        ORDER BY COALESCE(finished_at, claimed_at, created_at) DESC
    """
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(query).fetchall()]
    except sqlite3.DatabaseError:
        return []


def _load_metrics_queue(db_path: Path) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    query = """
        SELECT case_id, status, created_at, claimed_at, finished_at, error
        FROM metrics_queue
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
        WHERE COALESCE(ArtifactsPurged, 0) = 0
        ORDER BY ProcessedAt DESC
    """
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(query).fetchall()]
    except sqlite3.DatabaseError:
        return []


def _load_studies(studies_dir: Path, now: datetime) -> dict[str, dict[str, Any]]:
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
        segmentation_pipeline = pipeline.get("segmentation_pipeline", {})
        segmentation_reused = (
            isinstance(segmentation_pipeline, dict)
            and bool(segmentation_pipeline.get("reused_existing_outputs"))
        )
        segmentation_reuse_reason = ""
        if isinstance(segmentation_pipeline, dict):
            segmentation_reuse_reason = str(segmentation_pipeline.get("reuse_reason", "") or "")
        studies[case_id] = {
            "patient_name": _display_patient_name(payload.get("PatientName", "")),
            "origin": _normalize_case_origin(pipeline.get("prepare_input_origin", "")),
            "modality": payload.get("Modality", ""),
            "accession_number": payload.get("AccessionNumber", ""),
            "study_date": payload.get("StudyDate", ""),
            "prepare_elapsed": _resolve_elapsed_time(
                pipeline,
                now=now,
                elapsed_keys=("prepare_elapsed_time",),
                start_keys=("prepare_start_time",),
                end_keys=("prepare_end_time",),
            ),
            "segmentation_elapsed": _resolve_elapsed_time(
                pipeline,
                now=now,
                elapsed_keys=("segmentation_elapsed_time", "processing_elapsed_time", "elapsed_time"),
                start_keys=("segmentation_start_time", "start_time"),
                end_keys=("segmentation_end_time", "end_time"),
            ),
            "metrics_elapsed": _resolve_elapsed_time(
                pipeline,
                now=now,
                elapsed_keys=("metrics_elapsed_time",),
                start_keys=("metrics_start_time",),
                end_keys=("metrics_end_time",),
            ),
            "total_elapsed": _resolve_elapsed_time(
                pipeline,
                now=now,
                elapsed_keys=("pipeline_end_to_end_elapsed_time", "pipeline_active_elapsed_time"),
                start_keys=("prepare_start_time",),
                end_keys=("metrics_end_time", "segmentation_end_time", "end_time", "prepare_end_time"),
            ),
            "metrics_started": bool(pipeline.get("metrics_start_time")),
            "metrics_finished": bool(pipeline.get("metrics_end_time")),
            "segmentation_status": str(pipeline.get("segmentation_status", "") or ""),
            "segmentation_reused": segmentation_reused,
            "segmentation_reuse_reason": segmentation_reuse_reason,
            "segmentation_original_elapsed": str(
                pipeline.get("segmentation_original_elapsed_time", "") or ""
            ),
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


def _resolve_elapsed_time(
    pipeline: dict[str, Any],
    *,
    now: datetime,
    elapsed_keys: tuple[str, ...],
    start_keys: tuple[str, ...],
    end_keys: tuple[str, ...],
) -> str:
    for key in elapsed_keys:
        value = str(pipeline.get(key, "") or "").strip()
        if value:
            return value

    start_dt = _first_pipeline_datetime(pipeline, start_keys)
    if start_dt is None:
        return ""

    end_dt = _first_pipeline_datetime(pipeline, end_keys) or now
    if end_dt < start_dt:
        return ""
    return str(end_dt - start_dt)


def _first_pipeline_datetime(pipeline: dict[str, Any], keys: tuple[str, ...]) -> datetime | None:
    for key in keys:
        parsed = _parse_datetime(pipeline.get(key))
        if parsed is not None:
            return parsed
    return None


def _tail_text(path: Path, limit: int) -> str:
    try:
        content = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return _truncate(" ".join(content.split()), limit)


def _collect_files(
    directory: Path,
    *,
    suffix: str,
    include_claimed: bool = False,
    recursive: bool = False,
) -> list[Path]:
    if not directory.exists():
        return []
    iterator = directory.rglob("*") if recursive else directory.iterdir()
    files = [path for path in iterator if path.is_file() and path.name.endswith(suffix)]
    if not include_claimed:
        files = [path for path in files if not path.name.endswith(".working")]
    return sorted(files, key=lambda item: item.stat().st_mtime, reverse=True)


def _collect_runtime_items(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    items = [path for path in directory.iterdir() if not path.name.startswith(".")]
    return sorted(items, key=lambda item: item.stat().st_mtime, reverse=True)


def _scan_system_processes() -> dict[str, list[dict[str, str]]]:
    groups = {"intake": [], "prepare": [], "segmentation": [], "metrics": [], "space_manager": []}
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
                groups[slug].append(
                    {
                        "pid": pid,
                        "etime": _format_process_elapsed(etime),
                        "command": _truncate(command, 92),
                    }
                )
                break
    return groups


def _service_patterns() -> dict[str, tuple[str, ...]]:
    return {
        "intake": ("-m heimdallr.intake", "heimdallr/intake", "intake/gateway.py"),
        "prepare": ("-m heimdallr.prepare", "heimdallr/prepare", "prepare/worker.py"),
        "segmentation": ("-m heimdallr.segmentation", "heimdallr/segmentation"),
        "metrics": ("-m heimdallr.metrics", "heimdallr/metrics", "metrics/worker.py"),
        "space_manager": ("-m heimdallr.space_manager", "heimdallr/space_manager", "space_manager/worker.py"),
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


def _display_segmentation_elapsed(case: dict[str, Any]) -> str:
    source_value = case["segmentation_elapsed"]
    if case["segmentation_reused"] and case.get("segmentation_original_elapsed"):
        source_value = case["segmentation_original_elapsed"]
    rendered = _display_duration(source_value)
    if not case["segmentation_reused"]:
        return rendered
    reuse_reason = str(case.get("segmentation_reuse_reason", "") or "")
    if reuse_reason.startswith("prepare_duplicate_"):
        return tui("snapshot.case.elapsed.prepare_duplicate", value=rendered)
    return tui("snapshot.case.elapsed.reused", value=rendered)


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


def _newest_age_seconds(paths: list[Path], now: datetime) -> float | None:
    ages = []
    for path in paths:
        try:
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=LOCAL_TZ)
        except OSError:
            continue
        ages.append((now - mtime).total_seconds())
    return min(ages) if ages else None


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
        return no_data()
    minutes = int(seconds // 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m"


def _disk_free_bytes(path: Path) -> int | None:
    try:
        return int(shutil.disk_usage(path).free)
    except OSError:
        return None


def _bytes_human(value: int) -> str:
    suffixes = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(max(value, 0))
    for suffix in suffixes:
        if size < 1024.0 or suffix == suffixes[-1]:
            return f"{size:.1f}{suffix}"
        size /= 1024.0
    return f"{size:.1f}PB"


def _format_process_elapsed(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return raw

    days = 0
    clock = raw
    if "-" in raw:
        day_text, _, clock = raw.partition("-")
        try:
            days = int(day_text)
        except ValueError:
            return raw

    parts = clock.split(":")
    try:
        if len(parts) == 3:
            hours = int(parts[0])
            minutes = int(parts[1])
        elif len(parts) == 2:
            hours = 0
            minutes = int(parts[0])
        else:
            return raw
    except ValueError:
        return raw

    if days:
        total_hours = hours + days * 24
        display_days, rem_hours = divmod(total_hours, 24)
        return f"{display_days}d {rem_hours:02d}:{minutes:02d}"
    return f"{hours}:{minutes:02d}"


def _truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"
