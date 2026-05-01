#!/usr/bin/env python3
"""Resident worker for outbound integration event delivery."""

from __future__ import annotations

import argparse
import json
import requests
import sys
import time

from heimdallr.integration_dispatcher.config import (
    integration_dispatch_retry_attempts,
    integration_dispatch_retry_backoff_seconds,
    load_integration_dispatch_config,
)
from heimdallr.shared import settings, store
from heimdallr.shared.sqlite import connect as db_connect

settings.configure_service_stdio()

SERVICE_NAME = "integration_dispatcher"
MODULE_NAME = "integration_dispatcher.worker"


def _log_event(
    level: str,
    event: str,
    message: str,
    **fields: object,
) -> None:
    payload = {
        "lvl": level,
        "svc": SERVICE_NAME,
        "mod": MODULE_NAME,
        "evt": event,
        "msg": message,
    }
    payload.update({key: value for key, value in fields.items() if value is not None})
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def ensure_integration_dispatch_queue_table() -> None:
    conn = db_connect()
    try:
        store.ensure_schema(conn)
    finally:
        conn.close()


def claim_next_pending_integration_dispatch_queue_item():
    conn = db_connect()
    try:
        return store.claim_next_pending_integration_dispatch_queue_item(conn)
    finally:
        conn.close()


def mark_integration_dispatch_queue_item_done(queue_id: int, *, response_status: int | None) -> None:
    conn = db_connect()
    try:
        store.mark_integration_dispatch_queue_item_done(
            conn,
            queue_id,
            response_status=response_status,
        )
    finally:
        conn.close()


def retry_integration_dispatch_queue_item(
    queue_id: int,
    error_message: str,
    *,
    backoff_seconds: int,
    response_status: int | None = None,
) -> None:
    conn = db_connect()
    try:
        store.retry_integration_dispatch_queue_item(
            conn,
            queue_id,
            error_message,
            backoff_seconds=backoff_seconds,
            response_status=response_status,
        )
    finally:
        conn.close()


def mark_integration_dispatch_queue_item_error(
    queue_id: int,
    error_message: str,
    *,
    response_status: int | None = None,
) -> None:
    conn = db_connect()
    try:
        store.mark_integration_dispatch_queue_item_error(
            conn,
            queue_id,
            error_message,
            response_status=response_status,
        )
    finally:
        conn.close()


def dispatch_integration_event(
    *,
    destination_url: str,
    http_method: str,
    timeout_seconds: int,
    request_headers_json: str,
    payload_json: str,
) -> requests.Response:
    headers = json.loads(request_headers_json) if request_headers_json else {}
    payload = json.loads(payload_json)
    response = requests.request(
        http_method,
        destination_url,
        json=payload,
        headers=headers,
        timeout=max(int(timeout_seconds), 1),
    )
    if 200 <= int(response.status_code) < 300:
        return response
    body_preview = (response.text or "").strip()[:500]
    raise RuntimeError(
        f"HTTP {response.status_code} from {destination_url}: {body_preview or 'empty response body'}"
    )


def run_dispatch_cycle() -> int:
    processed = 0
    while True:
        queue_item = claim_next_pending_integration_dispatch_queue_item()
        if not queue_item:
            return processed

        (
            queue_id,
            event_type,
            _event_version,
            event_key,
            case_id,
            _study_uid,
            destination_name,
            destination_url,
            http_method,
            timeout_seconds,
            request_headers_json,
            payload_json,
            attempts_before_claim,
        ) = queue_item

        _log_event(
            "INFO",
            "dispatch_claimed",
            "integration dispatch item claimed",
            queue_id=queue_id,
            event_type=event_type,
            event_key=event_key,
            case_id=case_id,
            destination=destination_name,
        )
        try:
            response = dispatch_integration_event(
                destination_url=destination_url,
                http_method=http_method,
                timeout_seconds=int(timeout_seconds),
                request_headers_json=request_headers_json,
                payload_json=payload_json,
            )
            mark_integration_dispatch_queue_item_done(
                queue_id,
                response_status=int(response.status_code),
            )
            processed += 1
            _log_event(
                "OK",
                "dispatch_done",
                "integration dispatch item delivered",
                queue_id=queue_id,
                event_type=event_type,
                event_key=event_key,
                case_id=case_id,
                destination=destination_name,
                response_status=int(response.status_code),
            )
        except Exception as exc:
            config = load_integration_dispatch_config()
            retry_attempts = integration_dispatch_retry_attempts(config)
            retry_backoff_seconds = integration_dispatch_retry_backoff_seconds(config)
            claimed_attempt = int(attempts_before_claim) + 1
            if claimed_attempt < max(retry_attempts, 1):
                retry_integration_dispatch_queue_item(
                    queue_id,
                    str(exc),
                    backoff_seconds=retry_backoff_seconds,
                )
                _log_event(
                    "WARN",
                    "dispatch_retry",
                    "integration dispatch item scheduled for retry",
                    queue_id=queue_id,
                    event_type=event_type,
                    event_key=event_key,
                    case_id=case_id,
                    destination=destination_name,
                    retry=claimed_attempt,
                    max_retries=retry_attempts,
                    backoff_seconds=retry_backoff_seconds,
                    err=str(exc),
                )
            else:
                mark_integration_dispatch_queue_item_error(queue_id, str(exc))
                _log_event(
                    "ERR",
                    "dispatch_fail",
                    "integration dispatch item failed",
                    queue_id=queue_id,
                    event_type=event_type,
                    event_key=event_key,
                    case_id=case_id,
                    destination=destination_name,
                    retry=claimed_attempt,
                    max_retries=retry_attempts,
                    err=str(exc),
                )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Heimdallr outbound integration dispatcher")
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Drain all currently pending dispatch queue items once and exit",
    )
    args = parser.parse_args(argv)

    _log_event(
        "INFO",
        "worker_start",
        "starting integration dispatch monitoring",
        config_path=str(settings.INTEGRATION_DISPATCH_CONFIG_PATH),
        scan_interval_seconds=settings.INTEGRATION_DISPATCH_SCAN_INTERVAL,
        run_once=args.run_once,
    )
    ensure_integration_dispatch_queue_table()

    if args.run_once:
        try:
            run_dispatch_cycle()
        except Exception as exc:
            _log_event(
                "ERR",
                "run_once_fail",
                "integration dispatch run-once failed",
                err=str(exc),
            )
            return 1
        return 0

    try:
        while True:
            try:
                processed = run_dispatch_cycle()
                if processed == 0:
                    time.sleep(settings.INTEGRATION_DISPATCH_SCAN_INTERVAL)
            except Exception as exc:
                _log_event(
                    "ERR",
                    "main_loop_fail",
                    "integration dispatch main loop failed",
                    err=str(exc),
                )
                time.sleep(settings.INTEGRATION_DISPATCH_SCAN_INTERVAL)
    except KeyboardInterrupt:
        _log_event(
            "INFO",
            "worker_stop",
            "stopping integration dispatch monitoring",
        )
        return 0
