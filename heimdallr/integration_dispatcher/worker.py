#!/usr/bin/env python3
"""Resident worker for outbound integration event delivery."""

from __future__ import annotations

import argparse
import json
import requests
import time

from heimdallr.integration_dispatcher.config import (
    integration_dispatch_retry_attempts,
    integration_dispatch_retry_backoff_seconds,
    load_integration_dispatch_config,
)
from heimdallr.shared import settings, store
from heimdallr.shared.sqlite import connect as db_connect

settings.configure_service_stdio()


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

        print(
            f"[Integration Dispatch] Claimed {event_type} "
            f"({event_key}) -> {destination_name} for {case_id or 'n/a'}"
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
            print(
                f"[Integration Dispatch] ✓ {event_type} "
                f"({event_key}) -> {destination_name} [{response.status_code}]"
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
                print(
                    f"[Integration Dispatch] Retry {claimed_attempt}/{retry_attempts} "
                    f"for {event_type} ({event_key}) -> {destination_name}: {exc}"
                )
            else:
                mark_integration_dispatch_queue_item_error(queue_id, str(exc))
                print(
                    f"[Integration Dispatch] Error for {event_type} "
                    f"({event_key}) -> {destination_name}: {exc}"
                )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Heimdallr outbound integration dispatcher")
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Drain all currently pending dispatch queue items once and exit",
    )
    args = parser.parse_args(argv)

    print("Starting integration dispatch monitoring...")
    print(f"  Config: {settings.INTEGRATION_DISPATCH_CONFIG_PATH}")
    print(f"  Scan interval: {settings.INTEGRATION_DISPATCH_SCAN_INTERVAL}s")
    ensure_integration_dispatch_queue_table()

    if args.run_once:
        try:
            run_dispatch_cycle()
        except Exception as exc:
            print(f"Error in integration dispatch run-once: {exc}")
            return 1
        return 0

    try:
        while True:
            try:
                processed = run_dispatch_cycle()
                if processed == 0:
                    time.sleep(settings.INTEGRATION_DISPATCH_SCAN_INTERVAL)
            except Exception as exc:
                print(f"Error in integration dispatch main loop: {exc}")
                time.sleep(settings.INTEGRATION_DISPATCH_SCAN_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopping integration dispatch monitoring...")
        return 0
