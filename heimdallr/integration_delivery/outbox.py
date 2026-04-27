"""Queue helpers for final package delivery to external submitters."""

from __future__ import annotations

from typing import Any

from heimdallr.integration_delivery.config import (
    integration_delivery_enabled,
    integration_delivery_timeout_seconds,
    load_integration_delivery_config,
)
from heimdallr.shared import store
from heimdallr.shared.external_delivery import normalize_requested_outputs
from heimdallr.shared.sqlite import connect as db_connect


def enqueue_case_delivery(
    *,
    case_id: str,
    study_uid: str | None,
    external_delivery: dict[str, Any],
) -> bool:
    config = load_integration_delivery_config()
    if not integration_delivery_enabled(config):
        return False

    job_id = str(external_delivery.get("job_id", "") or "").strip()
    callback_url = str(external_delivery.get("callback_url", "") or "").strip()
    if not job_id or not callback_url:
        return False

    conn = db_connect()
    try:
        store.enqueue_integration_delivery(
            conn,
            job_id=job_id,
            event_type="case.completed",
            event_version=1,
            case_id=case_id,
            study_uid=study_uid,
            client_case_id=str(external_delivery.get("client_case_id", "") or "").strip() or None,
            source_system=str(external_delivery.get("source_system", "") or "").strip() or None,
            callback_url=callback_url,
            http_method="POST",
            timeout_seconds=integration_delivery_timeout_seconds(config),
            requested_outputs=normalize_requested_outputs(external_delivery.get("requested_outputs")),
        )
    finally:
        conn.close()
    return True
