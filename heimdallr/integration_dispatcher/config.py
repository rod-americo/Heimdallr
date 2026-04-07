"""Configuration and queue-item builders for outbound integration events."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from heimdallr.shared import settings


def load_integration_dispatch_config() -> dict[str, Any]:
    path = Path(settings.INTEGRATION_DISPATCH_CONFIG_PATH)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid integration dispatch config JSON: {path}") from exc
    if not isinstance(raw, dict):
        raise RuntimeError(f"Integration dispatch config must be a JSON object: {path}")
    return raw


def integration_dispatch_enabled(config: dict[str, Any]) -> bool:
    return bool(config.get("enabled", True))


def integration_dispatch_retry_attempts(config: dict[str, Any]) -> int:
    return int(config.get("retry_attempts", 5))


def integration_dispatch_retry_backoff_seconds(config: dict[str, Any]) -> int:
    return int(config.get("retry_backoff_seconds", 30))


def _resolve_destination_headers(destination_name: str, destination: dict[str, Any]) -> dict[str, str]:
    headers = destination.get("headers", {})
    if headers is None:
        headers = {}
    if not isinstance(headers, dict):
        raise RuntimeError(
            f"Integration dispatch destination '{destination_name}' field 'headers' must be an object"
        )

    resolved = {
        str(key).strip(): str(value).strip()
        for key, value in headers.items()
        if str(key).strip() and str(value).strip()
    }

    headers_from_env = destination.get("headers_from_env", {})
    if headers_from_env is None:
        headers_from_env = {}
    if not isinstance(headers_from_env, dict):
        raise RuntimeError(
            f"Integration dispatch destination '{destination_name}' field 'headers_from_env' "
            "must be an object"
        )

    for header_name, env_name in headers_from_env.items():
        normalized_header_name = str(header_name).strip()
        normalized_env_name = str(env_name).strip()
        if not normalized_header_name or not normalized_env_name:
            continue
        env_value = os.getenv(normalized_env_name, "").strip()
        if env_value:
            resolved[normalized_header_name] = env_value
    return resolved


def _destination_accepts_event(destination: dict[str, Any], event_type: str) -> bool:
    configured = destination.get("events")
    if configured is None:
        return True
    if not isinstance(configured, list):
        return False
    normalized = {str(item).strip() for item in configured if str(item).strip()}
    return event_type in normalized


def build_dispatch_queue_items(
    *,
    event_type: str,
    event_version: int,
    event_key: str,
    case_id: str | None,
    study_uid: str | None,
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    config = load_integration_dispatch_config()
    if not integration_dispatch_enabled(config):
        return []

    destinations = config.get("destinations", [])
    if not isinstance(destinations, list):
        raise RuntimeError("Integration dispatch config field 'destinations' must be a list")

    queue_items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for destination in destinations:
        if not isinstance(destination, dict):
            continue
        if not destination.get("enabled", True):
            continue
        if not _destination_accepts_event(destination, event_type):
            continue

        destination_name = str(destination.get("name", "") or "").strip()
        destination_url = str(destination.get("url", "") or "").strip()
        http_method = str(destination.get("method", "POST") or "POST").upper()
        timeout_seconds = int(destination.get("timeout_seconds", 10) or 10)
        if not destination_name or not destination_url or destination_name in seen:
            continue
        if http_method != "POST":
            raise RuntimeError(
                f"Integration dispatch destination '{destination_name}' uses unsupported method '{http_method}'"
            )
        headers = _resolve_destination_headers(destination_name, destination)

        seen.add(destination_name)
        queue_items.append(
            {
                "event_type": event_type,
                "event_version": int(event_version),
                "event_key": event_key,
                "case_id": case_id,
                "study_uid": study_uid,
                "destination_name": destination_name,
                "destination_url": destination_url,
                "http_method": http_method,
                "timeout_seconds": timeout_seconds,
                "request_headers": headers or {},
                "payload": payload,
            }
        )
    return queue_items
