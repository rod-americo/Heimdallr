#!/usr/bin/env python3
"""Resident worker for reclaiming runtime study disk usage."""

from __future__ import annotations

import argparse
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from heimdallr.shared import settings, store
from heimdallr.shared.sqlite import connect as db_connect

settings.configure_service_stdio()
settings.ensure_directories()


@dataclass(frozen=True)
class DiskSnapshot:
    total_bytes: int
    used_bytes: int
    free_bytes: int

    @property
    def used_percent(self) -> float:
        if self.total_bytes <= 0:
            return 0.0
        return (self.used_bytes / self.total_bytes) * 100.0


def _disk_snapshot(path: Path) -> DiskSnapshot:
    usage = shutil.disk_usage(path)
    return DiskSnapshot(
        total_bytes=int(usage.total),
        used_bytes=int(usage.used),
        free_bytes=int(usage.free),
    )


def _bytes_human(value: int) -> str:
    suffixes = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(max(value, 0))
    for suffix in suffixes:
        if size < 1024.0 or suffix == suffixes[-1]:
            return f"{size:.1f}{suffix}"
        size /= 1024.0
    return f"{size:.1f}PB"


def list_purge_candidates(studies_dir: Path, protected_case_ids: set[str]) -> list[Path]:
    """Return purge-eligible case directories ordered oldest-first by mtime."""
    candidates: list[Path] = []
    for path in studies_dir.iterdir():
        if not path.is_dir():
            continue
        if path.name.startswith("."):
            continue
        if path.name in protected_case_ids:
            continue
        candidates.append(path)
    return sorted(candidates, key=lambda path: (path.stat().st_mtime, path.name))


def purge_case_directory(case_dir: Path) -> tuple[str, int]:
    """Delete a case directory and purge associated DB rows."""
    studies_root = settings.STUDIES_DIR.resolve()
    resolved_case_dir = case_dir.resolve()
    if resolved_case_dir.parent != studies_root:
        raise RuntimeError(f"Refusing to purge non-study path: {resolved_case_dir}")

    case_id = case_dir.name
    before_snapshot = _disk_snapshot(studies_root)
    shutil.rmtree(resolved_case_dir)

    conn = db_connect()
    try:
        store.purge_case_records(conn, case_id)
    finally:
        conn.close()

    after_snapshot = _disk_snapshot(studies_root)
    reclaimed_bytes = max(0, before_snapshot.used_bytes - after_snapshot.used_bytes)
    return case_id, reclaimed_bytes


def reclaim_space_once(
    *,
    studies_dir: Path,
    threshold_percent: float,
) -> list[dict[str, object]]:
    """Delete oldest case directories until disk usage drops below threshold."""
    studies_dir.mkdir(parents=True, exist_ok=True)
    before_snapshot = _disk_snapshot(studies_dir)
    if before_snapshot.used_percent < threshold_percent:
        return []

    conn = db_connect()
    try:
        protected_case_ids = store.list_protected_case_ids(conn)
    finally:
        conn.close()

    deletions: list[dict[str, object]] = []
    candidates = list_purge_candidates(studies_dir, protected_case_ids)
    current_snapshot = before_snapshot

    for case_dir in candidates:
        if current_snapshot.used_percent < threshold_percent:
            break
        case_id, reclaimed_bytes = purge_case_directory(case_dir)
        current_snapshot = _disk_snapshot(studies_dir)
        deletions.append(
            {
                "case_id": case_id,
                "reclaimed_bytes": reclaimed_bytes,
                "used_percent_after": round(current_snapshot.used_percent, 2),
            }
        )

    return deletions


def run_space_manager_once(*, log_below_threshold: bool = True) -> int:
    studies_dir = settings.STUDIES_DIR
    threshold_percent = settings.SPACE_MANAGER_USAGE_THRESHOLD_PERCENT
    snapshot = _disk_snapshot(studies_dir)
    if snapshot.used_percent < threshold_percent:
        if log_below_threshold:
            print(
                f"[Space Manager] Usage {snapshot.used_percent:.2f}% "
                f"({_bytes_human(snapshot.used_bytes)}/{_bytes_human(snapshot.total_bytes)})"
            )
            print(
                f"[Space Manager] Below threshold {threshold_percent:.2f}% "
                f"for {studies_dir}"
            )
        return 0

    print(
        f"[Space Manager] Threshold reached at {snapshot.used_percent:.2f}% "
        f"(limit {threshold_percent:.2f}%) for {studies_dir}"
    )
    deletions = reclaim_space_once(
        studies_dir=studies_dir,
        threshold_percent=threshold_percent,
    )
    if not deletions:
        print("[Space Manager] No purge-eligible cases found")
        return 0

    for deletion in deletions:
        print(
            f"[Space Manager] Purged {deletion['case_id']} "
            f"(reclaimed {_bytes_human(int(deletion['reclaimed_bytes']))}, "
            f"usage {float(deletion['used_percent_after']):.2f}%)"
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Heimdallr runtime studies space manager")
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Check usage once and reclaim space immediately if needed",
    )
    args = parser.parse_args(argv)

    print("Starting runtime space manager...")
    print(f"  Studies root: {settings.STUDIES_DIR}")
    print(f"  Usage threshold: {settings.SPACE_MANAGER_USAGE_THRESHOLD_PERCENT:.2f}%")
    print(f"  Scan interval: {settings.SPACE_MANAGER_SCAN_INTERVAL}s")

    if args.run_once:
        return run_space_manager_once(log_below_threshold=True)

    try:
        while True:
            run_space_manager_once(log_below_threshold=False)
            time.sleep(settings.SPACE_MANAGER_SCAN_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopping runtime space manager...")
        return 0
