"""Helpers for external submit-and-push delivery contracts."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

from heimdallr.shared import settings
from heimdallr.shared.spool import unclaim_path
from heimdallr.shared.spool import atomic_write_bytes


EXTERNAL_SUBMISSION_SIDECAR_SUFFIX = ".submission.json"

DEFAULT_REQUESTED_OUTPUTS = {
    "id_json": True,
    "metadata_json": True,
    "metrics_json": True,
    "overlays_png": True,
    "overlays_dicom": True,
    "report_pdf": True,
    "report_pdf_dicom": False,
    "artifact_instructions_pdf": True,
    "artifact_instructions_dicom": True,
    "artifacts_tree": True,
}

def normalize_requested_outputs(raw: dict[str, Any] | None) -> dict[str, bool]:
    normalized = dict(DEFAULT_REQUESTED_OUTPUTS)
    if not isinstance(raw, dict):
        return normalized
    for key in tuple(DEFAULT_REQUESTED_OUTPUTS):
        if key not in raw:
            continue
        value = raw[key]
        if isinstance(value, bool):
            normalized[key] = value
        else:
            normalized[key] = str(value).strip().lower() in {"1", "true", "yes", "on"}
    return normalized


def normalize_requested_metrics_modules(raw: Any) -> list[str]:
    if raw in (None, ""):
        return []

    items: list[str]
    if isinstance(raw, list):
        items = [str(item or "").strip() for item in raw]
    elif isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        items = [part.strip() for part in text.split(",")]
    else:
        raise ValueError("requested_metrics_modules must be a list or CSV string")

    normalized: list[str] = []
    seen: set[str] = set()
    for item in items:
        if not item or item in seen:
            continue
        normalized.append(item)
        seen.add(item)
    return normalized


def new_external_job_id() -> str:
    return str(uuid.uuid4())


def external_submission_sidecar_path(zip_path: Path) -> Path:
    logical_zip_path = unclaim_path(zip_path)
    return logical_zip_path.with_name(f"{logical_zip_path.name}{EXTERNAL_SUBMISSION_SIDECAR_SUFFIX}")


def write_external_submission_sidecar(zip_path: Path, payload: dict[str, Any]) -> Path:
    sidecar_path = external_submission_sidecar_path(zip_path)
    atomic_write_bytes(
        sidecar_path,
        json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8"),
    )
    return sidecar_path


def load_external_submission_sidecar(zip_path: Path) -> dict[str, Any]:
    sidecar_path = external_submission_sidecar_path(zip_path)
    try:
        raw = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def update_external_submission_sidecar(zip_path: Path, updates: dict[str, Any]) -> None:
    payload = load_external_submission_sidecar(zip_path)
    if not payload:
        return
    payload.update(updates)
    write_external_submission_sidecar(zip_path, payload)


def delete_external_submission_sidecar(zip_path: Path) -> None:
    sidecar_path = external_submission_sidecar_path(zip_path)
    if sidecar_path.exists():
        sidecar_path.unlink()


def move_external_submission_sidecar(source_zip_path: Path, destination_zip_path: Path) -> None:
    source_sidecar = external_submission_sidecar_path(source_zip_path)
    if not source_sidecar.exists():
        return
    destination_sidecar = external_submission_sidecar_path(destination_zip_path)
    destination_sidecar.parent.mkdir(parents=True, exist_ok=True)
    source_sidecar.replace(destination_sidecar)


def build_external_submission_payload(
    *,
    job_id: str,
    client_case_id: str,
    callback_url: str,
    source_system: str | None,
    requested_outputs: dict[str, Any] | None,
    requested_metrics_modules: Any = None,
) -> dict[str, Any]:
    return {
        "job_id": str(job_id),
        "client_case_id": str(client_case_id),
        "callback_url": str(callback_url),
        "source_system": str(source_system or "").strip() or None,
        "requested_outputs": normalize_requested_outputs(requested_outputs),
        "requested_metrics_modules": normalize_requested_metrics_modules(requested_metrics_modules),
        "received_at": settings.local_now().isoformat(),
    }
