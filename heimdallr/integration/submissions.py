"""Helpers for external submit-and-push delivery contracts."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

from heimdallr.shared import settings
from heimdallr.shared.i18n import normalize_locale
from heimdallr.shared.spool import unclaim_path
from heimdallr.shared.spool import atomic_write_bytes


EXTERNAL_SUBMISSION_SIDECAR_SUFFIX = ".submission.json"

DEFAULT_REQUESTED_OUTPUTS = {
    "id_json": True,
    "metadata_json": True,
    "metrics_json": True,
    "overlays_png": True,
    "overlays_dicom": True,
    "report_pdf": False,
    "report_pdf_dicom": False,
    "artifact_instructions_pdf": True,
    "artifact_instructions_dicom": True,
    "artifacts_tree": True,
}

ARTIFACT_DICOM_SECONDARY_CAPTURE_TRANSFER_SYNTAXES = {
    "original": "original",
    "uncompressed": "original",
    "none": "original",
    "explicit_vr_little_endian": "original",
    "1.2.840.10008.1.2.1": "original",
    "deflated": "deflated",
    "deflated_lossless": "deflated",
    "deflated_explicit_vr_little_endian": "deflated",
    "1.2.840.10008.1.2.1.99": "deflated",
    "jpeg_ls_lossless": "jpeg_ls_lossless",
    "jpegls_lossless": "jpeg_ls_lossless",
    "jpegls": "jpeg_ls_lossless",
    "1.2.840.10008.1.2.4.80": "jpeg_ls_lossless",
    "jpeg_2000_lossless": "jpeg_2000_lossless",
    "jpeg2000_lossless": "jpeg_2000_lossless",
    "jp2k_lossless": "jpeg_2000_lossless",
    "1.2.840.10008.1.2.4.90": "jpeg_2000_lossless",
    "rle_lossless": "rle_lossless",
    "rle": "rle_lossless",
    "1.2.840.10008.1.2.5": "rle_lossless",
}


def normalize_requested_outputs(raw: dict[str, Any] | None) -> dict[str, bool]:
    normalized = {key: False for key in DEFAULT_REQUESTED_OUTPUTS}
    if raw is None:
        return normalized

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


def normalize_artifact_locale(raw: Any) -> str | None:
    if raw in (None, ""):
        return None
    return normalize_locale(str(raw))


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


def normalize_series_selection_policy(raw: Any) -> dict[str, Any]:
    if raw in (None, ""):
        return {}
    if not isinstance(raw, dict):
        raise ValueError("series_selection_policy must be a JSON object")
    return raw


def normalize_artifact_dicom_policy(raw: Any) -> dict[str, Any]:
    if raw in (None, ""):
        return {}
    if not isinstance(raw, dict):
        raise ValueError("artifact_dicom_policy must be a JSON object")

    policy: dict[str, Any] = {}
    if "secondary_capture_transfer_syntax" not in raw:
        return policy

    value = (
        str(raw.get("secondary_capture_transfer_syntax") or "")
        .strip()
        .lower()
        .replace("-", "_")
        .replace(" ", "_")
    )
    if not value:
        return policy
    normalized = ARTIFACT_DICOM_SECONDARY_CAPTURE_TRANSFER_SYNTAXES.get(value)
    if normalized is None:
        allowed = ", ".join(
            [
                "original",
                "deflated",
                "jpeg_ls_lossless",
                "jpeg_2000_lossless",
                "rle_lossless",
            ]
        )
        raise ValueError(
            "artifact_dicom_policy.secondary_capture_transfer_syntax "
            f"must be one of: {allowed}"
        )
    policy["secondary_capture_transfer_syntax"] = normalized
    return policy


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
    artifact_locale: Any = None,
    series_selection_policy: Any = None,
    artifact_dicom_policy: Any = None,
) -> dict[str, Any]:
    return {
        "job_id": str(job_id),
        "client_case_id": str(client_case_id),
        "callback_url": str(callback_url),
        "source_system": str(source_system or "").strip() or None,
        "requested_outputs": normalize_requested_outputs(requested_outputs),
        "requested_metrics_modules": normalize_requested_metrics_modules(requested_metrics_modules),
        "artifact_locale": normalize_artifact_locale(artifact_locale),
        "series_selection_policy": normalize_series_selection_policy(series_selection_policy),
        "artifact_dicom_policy": normalize_artifact_dicom_policy(artifact_dicom_policy),
        "received_at": settings.local_now().isoformat(),
    }
