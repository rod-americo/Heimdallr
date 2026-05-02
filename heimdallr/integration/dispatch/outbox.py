"""Reusable SQLite outbox enqueue helpers for external integration events."""

from __future__ import annotations

from typing import Any

from heimdallr.integration.dispatch.config import build_dispatch_queue_items
from heimdallr.shared import store
from heimdallr.shared.sqlite import connect as db_connect


def enqueue_dispatches(
    *,
    event_type: str,
    event_version: int,
    event_key: str,
    case_id: str | None,
    study_uid: str | None,
    payload: dict[str, Any],
) -> int:
    queue_items = build_dispatch_queue_items(
        event_type=event_type,
        event_version=event_version,
        event_key=event_key,
        case_id=case_id,
        study_uid=study_uid,
        payload=payload,
    )
    if not queue_items:
        return 0

    conn = db_connect()
    try:
        for item in queue_items:
            store.enqueue_integration_dispatch(conn, **item)
    finally:
        conn.close()
    return len(queue_items)
