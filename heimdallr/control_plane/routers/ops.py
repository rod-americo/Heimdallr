"""Operational capacity routes for the control plane."""

from __future__ import annotations

import shutil
from typing import Any

from fastapi import APIRouter, Depends

from ...shared import settings
from ...shared.dependencies import get_db

router = APIRouter(prefix="/ops", tags=["ops"])


QUEUE_TABLES = {
    "segmentation": "segmentation_queue",
    "qc_segmentation": "qc_segmentation_queue",
    "metrics": "metrics_queue",
    "integration_delivery": "integration_delivery_queue",
    "dicom_egress": "dicom_egress_queue",
}


def _queue_summary(db: Any, table_name: str) -> dict[str, Any]:
    counts = {
        str(row["status"] or "unknown"): int(row["total"] or 0)
        for row in db.execute(
            f"""
            SELECT status, count(*) AS total
            FROM {table_name}
            GROUP BY status
            """
        ).fetchall()
    }
    oldest_pending = db.execute(
        f"""
        SELECT created_at
        FROM {table_name}
        WHERE status = 'pending'
        ORDER BY created_at ASC, id ASC
        LIMIT 1
        """
    ).fetchone()
    pending = int(counts.get("pending", 0))
    claimed = int(counts.get("claimed", 0))
    return {
        "pending": pending,
        "claimed": claimed,
        "active": pending + claimed,
        "done": int(counts.get("done", 0)),
        "error": int(counts.get("error", 0)),
        "statuses": counts,
        "oldest_pending_created_at": oldest_pending["created_at"] if oldest_pending else None,
    }


def _disk_summary() -> dict[str, Any]:
    usage = shutil.disk_usage(settings.RUNTIME_DIR)
    return {
        "path": str(settings.RUNTIME_DIR),
        "total_bytes": int(usage.total),
        "used_bytes": int(usage.used),
        "free_bytes": int(usage.free),
        "used_percent": round((usage.used / usage.total) * 100, 2) if usage.total else None,
    }


@router.get("/queues")
async def queue_capacity(db=Depends(get_db)):
    """Return non-identifying queue and capacity data for external feeders."""
    queues = {name: _queue_summary(db, table) for name, table in QUEUE_TABLES.items()}
    segmentation = queues["segmentation"]
    return {
        "status": "ok",
        "runtime": {
            "timezone": settings.TIMEZONE,
            "disk": _disk_summary(),
        },
        "capacity": {
            "prepare_max_parallel_cases": int(settings.PREPARE_MAX_PARALLEL_CASES),
            "segmentation_max_parallel_cases": int(settings.SEGMENTATION_MAX_PARALLEL_CASES),
            "metrics_max_parallel_cases": int(settings.METRICS_MAX_PARALLEL_CASES),
            # Backward-compatible alias; this has always meant segmentation capacity.
            "max_parallel_cases": int(settings.SEGMENTATION_MAX_PARALLEL_CASES),
            "segmentation_active": int(segmentation["active"]),
            "segmentation_pending": int(segmentation["pending"]),
            "segmentation_claimed": int(segmentation["claimed"]),
            "qc_evidence_host_default_enabled": bool(settings.QC_EVIDENCE_ENABLED),
            "qc_segmentation_active": int(queues["qc_segmentation"]["active"]),
        },
        "queues": queues,
    }
