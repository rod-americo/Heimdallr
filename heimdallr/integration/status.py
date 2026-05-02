"""External job status lookup helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from heimdallr.integration.submissions import EXTERNAL_SUBMISSION_SIDECAR_SUFFIX
from heimdallr.shared import settings, store
from heimdallr.shared.sqlite import connect as db_connect


def _load_json(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _delivery_rows(job_id: str) -> list[dict[str, Any]]:
    conn = db_connect()
    try:
        rows = store.get_integration_delivery_rows_for_job(conn, job_id)
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _delivery_summary(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    row = rows[0]
    return {
        "event_type": row.get("event_type"),
        "status": row.get("status"),
        "attempts": row.get("attempts"),
        "created_at": row.get("created_at"),
        "claimed_at": row.get("claimed_at"),
        "finished_at": row.get("finished_at"),
        "next_attempt_at": row.get("next_attempt_at"),
        "response_status": row.get("response_status"),
        "error": row.get("error"),
    }


def _status_from_delivery(rows: list[dict[str, Any]]) -> str | None:
    if not rows:
        return None
    row = rows[0]
    event_type = str(row.get("event_type") or "")
    delivery_status = str(row.get("status") or "")
    if event_type == "case.failed":
        if delivery_status == "done":
            return "failed"
        if delivery_status == "error":
            return "failure_delivery_error"
        return "failure_delivery_pending"
    if event_type == "case.completed":
        if delivery_status == "done":
            return "completed"
        if delivery_status == "error":
            return "completion_delivery_error"
        return "completion_delivery_pending"
    return None


def _find_sidecar(job_id: str) -> dict[str, Any] | None:
    candidate_dirs = [
        getattr(settings, "UPLOAD_EXTERNAL_DIR", None),
        getattr(settings, "UPLOAD_FROM_PREPARE_DIR", None),
        getattr(settings, "UPLOAD_DIR", None),
        getattr(settings, "UPLOAD_FAILED_DIR", None),
    ]
    seen: set[Path] = set()
    for directory in candidate_dirs:
        if directory is None:
            continue
        root = Path(directory)
        if root in seen or not root.exists():
            continue
        seen.add(root)
        for sidecar_path in sorted(root.glob(f"*{EXTERNAL_SUBMISSION_SIDECAR_SUFFIX}")):
            payload = _load_json(sidecar_path)
            if str(payload.get("job_id", "") or "") != str(job_id):
                continue
            failed = root == getattr(settings, "UPLOAD_FAILED_DIR", None)
            failure = payload.get("failure") if isinstance(payload.get("failure"), dict) else {}
            return {
                "job_id": job_id,
                "status": "failed" if failed or failure else "queued",
                "stage": failure.get("stage") or ("prepare" if failed else "upload"),
                "case_id": None,
                "study_instance_uid": None,
                "client_case_id": payload.get("client_case_id"),
                "source_system": payload.get("source_system"),
                "received_at": payload.get("received_at"),
                "error": failure.get("error"),
                "delivery": _delivery_summary(_delivery_rows(job_id)),
            }
    return None


def _pipeline_status(id_data: dict[str, Any]) -> tuple[str, str]:
    pipeline = id_data.get("Pipeline") if isinstance(id_data.get("Pipeline"), dict) else {}
    if pipeline.get("metrics_status") == "done":
        return "processing", "delivery"
    if pipeline.get("metrics_status") == "error" or pipeline.get("metrics_error"):
        return "failed", "metrics"
    if pipeline.get("segmentation_status") == "done":
        return "processing", "metrics"
    if pipeline.get("segmentation_status") == "error" or pipeline.get("segmentation_error"):
        return "failed", "segmentation"
    if pipeline.get("prepare_end_time"):
        return "processing", "segmentation"
    return "processing", "prepare"


def _find_study_job(job_id: str) -> dict[str, Any] | None:
    studies_dir = Path(settings.STUDIES_DIR)
    if not studies_dir.exists():
        return None
    for id_json_path in sorted(studies_dir.glob("*/metadata/id.json")):
        id_data = _load_json(id_json_path)
        external_delivery = id_data.get("ExternalDelivery")
        pipeline = id_data.get("Pipeline") if isinstance(id_data.get("Pipeline"), dict) else {}
        external_job_id = ""
        if isinstance(external_delivery, dict):
            external_job_id = str(external_delivery.get("job_id", "") or "")
        external_job_id = external_job_id or str(pipeline.get("external_job_id", "") or "")
        if external_job_id != str(job_id):
            continue
        delivery_rows = _delivery_rows(job_id)
        delivery_status = _status_from_delivery(delivery_rows)
        pipeline_status, stage = _pipeline_status(id_data)
        return {
            "job_id": job_id,
            "status": delivery_status or pipeline_status,
            "stage": stage,
            "case_id": id_data.get("CaseID") or id_json_path.parent.parent.name,
            "study_instance_uid": id_data.get("StudyInstanceUID"),
            "client_case_id": (
                external_delivery.get("client_case_id")
                if isinstance(external_delivery, dict)
                else pipeline.get("external_client_case_id")
            ),
            "source_system": external_delivery.get("source_system") if isinstance(external_delivery, dict) else None,
            "received_at": (
                external_delivery.get("received_at")
                if isinstance(external_delivery, dict)
                else pipeline.get("external_submission_received_at")
            ),
            "error": pipeline.get("metrics_error") or pipeline.get("segmentation_error"),
            "pipeline": pipeline,
            "delivery": _delivery_summary(delivery_rows),
        }
    return None


def _find_delivery_only(job_id: str) -> dict[str, Any] | None:
    rows = _delivery_rows(job_id)
    if not rows:
        return None
    row = rows[0]
    payload = {}
    try:
        payload = json.loads(row.get("payload_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        payload = {}
    return {
        "job_id": job_id,
        "status": _status_from_delivery(rows) or str(row.get("status") or "delivery_pending"),
        "stage": payload.get("failure_stage") or "delivery",
        "case_id": str(row.get("case_id") or "").strip() or None,
        "study_instance_uid": row.get("study_uid"),
        "client_case_id": row.get("client_case_id"),
        "source_system": row.get("source_system"),
        "received_at": payload.get("received_at"),
        "error": payload.get("error") or row.get("error"),
        "delivery": _delivery_summary(rows),
    }


def external_job_status(job_id: str) -> dict[str, Any] | None:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return None
    return (
        _find_study_job(normalized_job_id)
        or _find_sidecar(normalized_job_id)
        or _find_delivery_only(normalized_job_id)
    )
