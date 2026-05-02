"""Configuration helpers for final package delivery callbacks."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from heimdallr.shared import settings


def load_integration_delivery_config() -> dict[str, Any]:
    path = Path(settings.INTEGRATION_DELIVERY_CONFIG_PATH)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid integration delivery config JSON: {path}") from exc
    if not isinstance(raw, dict):
        raise RuntimeError(f"Integration delivery config must be a JSON object: {path}")
    return raw


def integration_delivery_enabled(config: dict[str, Any]) -> bool:
    return bool(config.get("enabled", True))


def integration_delivery_retry_attempts(config: dict[str, Any]) -> int:
    return int(config.get("retry_attempts", 5))


def integration_delivery_retry_backoff_seconds(config: dict[str, Any]) -> int:
    return int(config.get("retry_backoff_seconds", 30))


def integration_delivery_timeout_seconds(config: dict[str, Any]) -> int:
    return int(config.get("timeout_seconds", 120))
