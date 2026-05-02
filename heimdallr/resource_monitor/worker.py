#!/usr/bin/env python3
"""Resident RAM telemetry sampler for Heimdallr services."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path

from heimdallr.shared import settings, store
from heimdallr.shared.sqlite import connect as db_connect

settings.configure_service_stdio()
settings.ensure_directories()


@dataclass(frozen=True)
class MonitoredService:
    slug: str
    unit: str
    stage: str


@dataclass(frozen=True)
class ProcessSnapshot:
    pid: int
    ppid: int
    rss_kb: int
    hwm_kb: int
    pss_kb: int
    major_faults: int


def _mb_from_kb(value: int | None) -> float | None:
    if value is None:
        return None
    return round(float(value) / 1024.0, 2)


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _parse_status_value(raw_text: str, key: str) -> int | None:
    for line in raw_text.splitlines():
        if not line.startswith(f"{key}:"):
            continue
        _, _, tail = line.partition(":")
        parts = tail.strip().split()
        if not parts:
            return None
        try:
            return int(parts[0])
        except ValueError:
            return None
    return None


def _parse_proc_stat_major_faults(raw_text: str) -> int:
    marker = raw_text.rfind(")")
    if marker < 0:
        return 0
    parts = raw_text[marker + 2 :].split()
    if len(parts) < 10:
        return 0
    try:
        return int(parts[9])
    except ValueError:
        return 0


def _parse_smaps_rollup_pss(path: Path) -> int:
    raw_text = _read_text(path)
    if not raw_text:
        return 0
    return _parse_status_value(raw_text, "Pss") or 0


def _scan_process_table() -> dict[int, ProcessSnapshot]:
    snapshots: dict[int, ProcessSnapshot] = {}
    for proc_dir in Path("/proc").iterdir():
        if not proc_dir.name.isdigit():
            continue
        pid = int(proc_dir.name)
        status_text = _read_text(proc_dir / "status")
        stat_text = _read_text(proc_dir / "stat")
        if not status_text or not stat_text:
            continue
        ppid = _parse_status_value(status_text, "PPid") or 0
        rss_kb = _parse_status_value(status_text, "VmRSS") or 0
        hwm_kb = _parse_status_value(status_text, "VmHWM") or 0
        snapshots[pid] = ProcessSnapshot(
            pid=pid,
            ppid=ppid,
            rss_kb=rss_kb,
            hwm_kb=hwm_kb,
            pss_kb=_parse_smaps_rollup_pss(proc_dir / "smaps_rollup"),
            major_faults=_parse_proc_stat_major_faults(stat_text),
        )
    return snapshots


def _subtree_pids(root_pid: int, processes: dict[int, ProcessSnapshot]) -> list[int]:
    if root_pid <= 0 or root_pid not in processes:
        return []
    children: dict[int, list[int]] = defaultdict(list)
    for snapshot in processes.values():
        children[snapshot.ppid].append(snapshot.pid)
    ordered: list[int] = []
    queue: deque[int] = deque([root_pid])
    seen: set[int] = set()
    while queue:
        pid = queue.popleft()
        if pid in seen:
            continue
        seen.add(pid)
        ordered.append(pid)
        for child_pid in children.get(pid, []):
            queue.append(child_pid)
    return ordered


def _host_memory_snapshot() -> dict[str, float | None]:
    meminfo: dict[str, int] = {}
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            key, _, tail = line.partition(":")
            parts = tail.strip().split()
            if not parts:
                continue
            try:
                meminfo[key] = int(parts[0])
            except ValueError:
                continue
    except OSError:
        return {
            "total_mb": None,
            "available_mb": None,
            "swap_used_mb": None,
            "used_percent": None,
        }

    total_kb = meminfo.get("MemTotal", 0)
    available_kb = meminfo.get("MemAvailable", 0)
    swap_total_kb = meminfo.get("SwapTotal", 0)
    swap_free_kb = meminfo.get("SwapFree", 0)
    used_percent = None
    if total_kb > 0:
        used_percent = round(((total_kb - available_kb) / total_kb) * 100.0, 2)
    return {
        "total_mb": _mb_from_kb(total_kb),
        "available_mb": _mb_from_kb(available_kb),
        "swap_used_mb": _mb_from_kb(max(0, swap_total_kb - swap_free_kb)),
        "used_percent": used_percent,
    }


def _systemd_properties(unit: str) -> dict[str, str]:
    scope = "system"
    systemd_unit = unit
    if unit.startswith("user:"):
        scope = "user"
        systemd_unit = unit.removeprefix("user:").strip()
    if not systemd_unit:
        return {}
    command = ["systemctl"]
    if scope == "user":
        command.append("--user")
    command.extend(
        [
            "show",
            systemd_unit,
            "-p",
            "ActiveState",
            "-p",
            "ExecMainPID",
            "-p",
            "ControlGroup",
        ]
    )
    try:
        output = subprocess.check_output(
            command,
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    props: dict[str, str] = {}
    for line in output.splitlines():
        key, _, value = line.partition("=")
        if key:
            props[key] = value.strip()
    return props


def _cgroup_metrics(control_group: str | None) -> tuple[float | None, float | None]:
    if not control_group:
        return None, None
    relative = str(control_group).strip()
    if not relative:
        return None, None
    cgroup_dir = Path("/sys/fs/cgroup") / relative.lstrip("/")
    current_raw = _read_text(cgroup_dir / "memory.current")
    peak_raw = _read_text(cgroup_dir / "memory.peak")
    try:
        current_mb = round(int(current_raw) / (1024.0 * 1024.0), 2) if current_raw else None
    except ValueError:
        current_mb = None
    try:
        peak_mb = round(int(peak_raw) / (1024.0 * 1024.0), 2) if peak_raw else None
    except ValueError:
        peak_mb = None
    return current_mb, peak_mb


def _load_services() -> list[MonitoredService]:
    configured = settings.RESOURCE_MONITOR_CONFIG.get("services", [])
    services: list[MonitoredService] = []
    if not isinstance(configured, list):
        configured = []
    for item in configured:
        if not isinstance(item, dict):
            continue
        if not bool(item.get("enabled", True)):
            continue
        slug = str(item.get("slug", "") or "").strip()
        unit = str(item.get("unit", "") or "").strip()
        stage = str(item.get("stage", slug) or slug).strip()
        if not slug or not unit:
            continue
        services.append(MonitoredService(slug=slug, unit=unit, stage=stage or slug))
    return services


def _active_case_ids(conn, stage: str) -> list[str]:
    return store.list_resource_monitor_active_case_ids(conn, stage=stage)


def collect_resource_samples() -> list[dict[str, object]]:
    services = _load_services()
    if not services:
        return []
    processes = _scan_process_table()
    host_memory = _host_memory_snapshot()
    sampled_at = settings.local_timestamp()
    conn = db_connect()
    try:
        samples: list[dict[str, object]] = []
        for service in services:
            props = _systemd_properties(service.unit)
            active_state = props.get("ActiveState", "")
            try:
                main_pid = int(props.get("ExecMainPID", "0") or "0")
            except ValueError:
                main_pid = 0
            subtree = _subtree_pids(main_pid, processes)
            rss_kb = 0
            hwm_kb = 0
            pss_kb = 0
            major_faults = 0
            for pid in subtree:
                snapshot = processes.get(pid)
                if snapshot is None:
                    continue
                rss_kb += snapshot.rss_kb
                hwm_kb += snapshot.hwm_kb
                pss_kb += snapshot.pss_kb
                major_faults += snapshot.major_faults
            main_snapshot = processes.get(main_pid)
            cgroup_current_mb, cgroup_peak_mb = _cgroup_metrics(props.get("ControlGroup"))
            active_case_ids = _active_case_ids(conn, service.stage)
            samples.append(
                {
                    "sampled_at": sampled_at,
                    "service_slug": service.slug,
                    "service_unit": service.unit,
                    "stage": service.stage,
                    "main_pid": main_pid if main_pid > 0 else None,
                    "subtree_pids_json": json.dumps(subtree),
                    "active_case_ids_json": json.dumps(active_case_ids),
                    "rss_mb": _mb_from_kb(main_snapshot.rss_kb if main_snapshot else 0),
                    "peak_rss_mb": _mb_from_kb(main_snapshot.hwm_kb if main_snapshot else 0),
                    "subtree_rss_mb": _mb_from_kb(rss_kb),
                    "subtree_peak_rss_mb": _mb_from_kb(hwm_kb),
                    "subtree_pss_mb": _mb_from_kb(pss_kb),
                    "major_faults": major_faults,
                    "cgroup_memory_current_mb": cgroup_current_mb,
                    "cgroup_memory_peak_mb": cgroup_peak_mb,
                    "host_mem_total_mb": host_memory["total_mb"],
                    "host_mem_available_mb": host_memory["available_mb"],
                    "host_swap_used_mb": host_memory["swap_used_mb"],
                    "host_mem_used_percent": host_memory["used_percent"],
                    "notes_json": json.dumps(
                        {
                            "active_state": active_state,
                            "control_group": props.get("ControlGroup", ""),
                            "case_count": len(active_case_ids),
                        }
                    ),
                }
            )
        return samples
    finally:
        conn.close()


def run_resource_monitor_once(*, persist: bool = True) -> list[dict[str, object]]:
    samples = collect_resource_samples()
    if not samples:
        print("[Resource Monitor] No enabled services configured")
        return []
    for sample in samples:
        cases = json.loads(str(sample["active_case_ids_json"]))
        note = json.loads(str(sample["notes_json"]))
        print(
            "[Resource Monitor] "
            f"{sample['service_slug']} pid={sample['main_pid'] or '-'} "
            f"rss={sample['rss_mb'] or 0:.2f}MB "
            f"subtree_pss={sample['subtree_pss_mb'] or 0:.2f}MB "
            f"cgroup={sample['cgroup_memory_current_mb'] or 0:.2f}MB "
            f"swap={sample['host_swap_used_mb'] or 0:.2f}MB "
            f"cases={len(cases)} state={note.get('active_state', '')}"
        )
    if persist:
        conn = db_connect()
        try:
            store.insert_resource_monitor_samples(conn, samples)
        finally:
            conn.close()
    return samples


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Heimdallr resident RAM telemetry sampler")
    parser.add_argument("--run-once", action="store_true", help="Collect and persist one sample immediately")
    parser.add_argument("--no-persist", action="store_true", help="Collect samples without writing to SQLite")
    args = parser.parse_args(argv)

    print("Starting resource monitor...")
    print(f"  Config: {settings.RESOURCE_MONITOR_CONFIG_PATH}")
    print(f"  Scan interval: {settings.RESOURCE_MONITOR_SCAN_INTERVAL}s")

    if args.run_once:
        run_resource_monitor_once(persist=not args.no_persist)
        return 0

    try:
        while True:
            run_resource_monitor_once(persist=not args.no_persist)
            time.sleep(settings.RESOURCE_MONITOR_SCAN_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopping resource monitor...")
        return 0
