"""Configuration and routing helpers for DICOM egress."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from heimdallr.shared import settings


def load_dicom_egress_config() -> dict[str, Any]:
    path = Path(settings.DICOM_EGRESS_CONFIG_PATH)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid DICOM egress config JSON: {path}") from exc
    if not isinstance(raw, dict):
        raise RuntimeError(f"DICOM egress config must be a JSON object: {path}")
    return raw


def dicom_egress_local_ae_title(config: dict[str, Any]) -> str:
    value = str(config.get("local_ae_title", "")).strip()
    return value or settings.DICOM_AE_TITLE


def dicom_egress_retry_attempts(config: dict[str, Any]) -> int:
    return int(config.get("retry_attempts", 3))


def dicom_egress_retry_backoff_seconds(config: dict[str, Any]) -> int:
    return int(config.get("retry_backoff_seconds", 10))


def dicom_egress_connect_timeout_seconds(config: dict[str, Any]) -> int:
    return int(config.get("connect_timeout_seconds", 15))


def dicom_egress_dimse_timeout_seconds(config: dict[str, Any]) -> int:
    return int(config.get("dimse_timeout_seconds", 30))


def _destination_accepts_artifact(destination: dict[str, Any], artifact_type: str) -> bool:
    configured = destination.get("artifact_types")
    if configured is None:
        return True
    if not isinstance(configured, list):
        return False
    normalized = {str(item).strip() for item in configured if str(item).strip()}
    return artifact_type in normalized


def build_egress_queue_items(
    case_id: str,
    metadata: dict[str, Any],
    dicom_exports: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    config = load_dicom_egress_config()
    destinations = config.get("destinations", [])
    if not isinstance(destinations, list):
        raise RuntimeError("DICOM egress config field 'destinations' must be a list")

    pipeline = metadata.get("Pipeline", {}) if isinstance(metadata.get("Pipeline"), dict) else {}
    source_calling_aet = str(pipeline.get("intake_calling_aet", "") or "").strip() or None
    source_remote_ip = str(pipeline.get("intake_remote_ip", "") or "").strip() or None
    study_uid = str(metadata.get("StudyInstanceUID", "") or "").strip() or None

    queue_items: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for export in dicom_exports:
        artifact_path = str(export.get("path", "") or "").strip()
        artifact_type = str(
            export.get("kind")
            or export.get("artifact_type")
            or export.get("type")
            or "secondary_capture"
        ).strip()
        if not artifact_path or not artifact_type:
            continue

        for destination in destinations:
            if not isinstance(destination, dict):
                continue
            if not destination.get("enabled", True):
                continue
            if not _destination_accepts_artifact(destination, artifact_type):
                continue

            destination_name = str(destination.get("name", "") or "").strip()
            if not destination_name:
                continue
            if (artifact_path, destination_name) in seen:
                continue

            destination_host = str(destination.get("host", "") or "").strip()
            destination_port = int(destination.get("port", 0) or 0)
            destination_called_aet = str(destination.get("called_aet", "") or "").strip()
            if not destination_host or destination_port <= 0 or not destination_called_aet:
                continue

            seen.add((artifact_path, destination_name))
            queue_items.append(
                {
                    "case_id": case_id,
                    "study_uid": study_uid,
                    "artifact_path": artifact_path,
                    "artifact_type": artifact_type,
                    "destination_name": destination_name,
                    "destination_host": destination_host,
                    "destination_port": destination_port,
                    "destination_called_aet": destination_called_aet,
                    "source_calling_aet": source_calling_aet,
                    "source_remote_ip": source_remote_ip,
                }
            )
    return queue_items
