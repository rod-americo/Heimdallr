#!/usr/bin/env python3
"""Resident worker for outbound final package delivery."""

from __future__ import annotations

import argparse
import json
import shutil
import time

import requests

from heimdallr.integration_delivery.config import (
    integration_delivery_retry_attempts,
    integration_delivery_retry_backoff_seconds,
    load_integration_delivery_config,
)
from heimdallr.integration_delivery.package import build_delivery_package
from heimdallr.shared import settings, store
from heimdallr.shared.sqlite import connect as db_connect

settings.configure_service_stdio()


def ensure_integration_delivery_queue_table() -> None:
    conn = db_connect()
    try:
        store.ensure_schema(conn)
    finally:
        conn.close()


def claim_next_pending_integration_delivery_queue_item():
    conn = db_connect()
    try:
        return store.claim_next_pending_integration_delivery_queue_item(conn)
    finally:
        conn.close()


def mark_integration_delivery_queue_item_done(queue_id: int, *, response_status: int | None) -> None:
    conn = db_connect()
    try:
        store.mark_integration_delivery_queue_item_done(conn, queue_id, response_status=response_status)
    finally:
        conn.close()


def retry_integration_delivery_queue_item(
    queue_id: int,
    error_message: str,
    *,
    backoff_seconds: int,
    response_status: int | None = None,
) -> None:
    conn = db_connect()
    try:
        store.retry_integration_delivery_queue_item(
            conn,
            queue_id,
            error_message,
            backoff_seconds=backoff_seconds,
            response_status=response_status,
        )
    finally:
        conn.close()


def mark_integration_delivery_queue_item_error(
    queue_id: int,
    error_message: str,
    *,
    response_status: int | None = None,
) -> None:
    conn = db_connect()
    try:
        store.mark_integration_delivery_queue_item_error(
            conn,
            queue_id,
            error_message,
            response_status=response_status,
        )
    finally:
        conn.close()


def deliver_case_package(
    *,
    callback_url: str,
    http_method: str,
    timeout_seconds: int,
    manifest: dict,
    package_path: str,
) -> requests.Response:
    if str(http_method).upper() != "POST":
        raise RuntimeError(f"Unsupported delivery method: {http_method}")

    with open(package_path, "rb") as package_handle:
        response = requests.request(
            "POST",
            callback_url,
            files={
                "manifest": ("manifest.json", json.dumps(manifest, ensure_ascii=False), "application/json"),
                "package": (manifest["package_name"], package_handle, "application/zip"),
            },
            timeout=max(int(timeout_seconds), 1),
        )
    if 200 <= int(response.status_code) < 300:
        return response
    body_preview = (response.text or "").strip()[:500]
    raise RuntimeError(
        f"HTTP {response.status_code} from {callback_url}: {body_preview or 'empty response body'}"
    )


def run_delivery_cycle() -> int:
    processed = 0
    while True:
        queue_item = claim_next_pending_integration_delivery_queue_item()
        if not queue_item:
            return processed

        (
            queue_id,
            job_id,
            event_type,
            _event_version,
            case_id,
            study_uid,
            client_case_id,
            source_system,
            callback_url,
            http_method,
            timeout_seconds,
            requested_outputs_json,
            attempts_before_claim,
        ) = queue_item

        print(
            f"[Integration Delivery] Claimed {event_type} "
            f"({job_id}) -> {callback_url} for {case_id}"
        )
        temp_dir = None
        try:
            manifest, package_path = build_delivery_package(
                case_id=case_id,
                job_id=job_id,
                client_case_id=client_case_id,
                source_system=source_system,
                requested_outputs=json.loads(requested_outputs_json or "{}"),
            )
            temp_dir = package_path.parent
            response = deliver_case_package(
                callback_url=callback_url,
                http_method=http_method,
                timeout_seconds=int(timeout_seconds),
                manifest=manifest,
                package_path=str(package_path),
            )
            mark_integration_delivery_queue_item_done(queue_id, response_status=int(response.status_code))
            processed += 1
            print(
                f"[Integration Delivery] ✓ {event_type} "
                f"({job_id}) -> {callback_url} [{response.status_code}]"
            )
        except Exception as exc:
            config = load_integration_delivery_config()
            retry_attempts = integration_delivery_retry_attempts(config)
            retry_backoff_seconds = integration_delivery_retry_backoff_seconds(config)
            claimed_attempt = int(attempts_before_claim) + 1
            if claimed_attempt < max(retry_attempts, 1):
                retry_integration_delivery_queue_item(
                    queue_id,
                    str(exc),
                    backoff_seconds=retry_backoff_seconds,
                )
                print(
                    f"[Integration Delivery] Retry {claimed_attempt}/{retry_attempts} "
                    f"for {event_type} ({job_id}) -> {callback_url}: {exc}"
                )
            else:
                mark_integration_delivery_queue_item_error(queue_id, str(exc))
                print(
                    f"[Integration Delivery] Error for {event_type} "
                    f"({job_id}) -> {callback_url}: {exc}"
                )
        finally:
            if temp_dir is not None:
                shutil.rmtree(temp_dir, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Heimdallr outbound final delivery worker")
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Drain all currently pending final delivery queue items once and exit",
    )
    args = parser.parse_args(argv)

    print("Starting integration delivery monitoring...")
    print(f"  Config: {settings.INTEGRATION_DELIVERY_CONFIG_PATH}")
    print(f"  Scan interval: {settings.INTEGRATION_DELIVERY_SCAN_INTERVAL}s")
    ensure_integration_delivery_queue_table()

    if args.run_once:
        try:
            run_delivery_cycle()
        except Exception as exc:
            print(f"Error in integration delivery run-once: {exc}")
            return 1
        return 0

    try:
        while True:
            try:
                processed = run_delivery_cycle()
                if processed == 0:
                    time.sleep(settings.INTEGRATION_DELIVERY_SCAN_INTERVAL)
            except Exception as exc:
                print(f"Error in integration delivery main loop: {exc}")
                time.sleep(settings.INTEGRATION_DELIVERY_SCAN_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopping integration delivery monitoring...")
        return 0
