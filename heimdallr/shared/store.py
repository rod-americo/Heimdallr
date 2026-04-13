"""Operational SQLite store for Heimdallr."""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import sqlite3
from typing import Any
from zoneinfo import ZoneInfo

from . import settings
from .sqlite import connect


_DICOM_METADATA_COLUMNS = {
    "StudyInstanceUID": "TEXT PRIMARY KEY",
    "PatientName": "TEXT",
    "PatientID": "TEXT",
    "PatientBirthDate": "TEXT",
    "ClinicalName": "TEXT",
    "AccessionNumber": "TEXT",
    "StudyDate": "TEXT",
    "Modality": "TEXT",
    "CallingAET": "TEXT",
    "RemoteIP": "TEXT",
    "IdJson": "TEXT",
    "JsonDump": "TEXT",
    "DicomMetadata": "TEXT",
    "CalculationResults": "TEXT",
    "PatientSex": "TEXT",
    "Weight": "REAL",
    "Height": "REAL",
    "SMI": "REAL",
    "SegmentationSeriesInstanceUID": "TEXT",
    "SegmentationSliceCount": "INTEGER",
    "SegmentationProfile": "TEXT",
    "SegmentationTasks": "TEXT",
    "SegmentationCompletedAt": "TIMESTAMP",
    "MetricsProfile": "TEXT",
    "MetricsCompletedAt": "TIMESTAMP",
    "ArtifactsPurged": "INTEGER DEFAULT 0",
    "ArtifactsPurgedAt": "TIMESTAMP",
    "ProcessedAt": "TIMESTAMP",
}

_DICOM_EGRESS_QUEUE_COLUMNS = {
    "artifact_digest": "TEXT",
}


LOCAL_TZ = ZoneInfo(settings.TIMEZONE)


def _now_local_timestamp() -> str:
    """Return current wall-clock time in the configured local timezone."""
    return datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _future_local_timestamp(seconds: int) -> str:
    """Return a future wall-clock time in the configured local timezone."""
    return (datetime.now(LOCAL_TZ) + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")


def _parse_local_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=LOCAL_TZ)
    text = str(value).strip()
    if not text:
        return None
    for parser in (
        datetime.fromisoformat,
        lambda raw: datetime.strptime(raw, "%Y-%m-%d %H:%M:%S"),
    ):
        try:
            parsed = parser(text)
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=LOCAL_TZ)
        except ValueError:
            continue
    return None


def _is_stale_claimed_at(value: Any, *, ttl_seconds: int) -> bool:
    claimed_at = _parse_local_timestamp(value)
    if claimed_at is None:
        return True
    return (datetime.now(LOCAL_TZ) - claimed_at).total_seconds() >= max(int(ttl_seconds), 1)


def ensure_schema(conn: sqlite3.Connection | None = None) -> None:
    owns_connection = conn is None
    if conn is None:
        conn = connect()
    cursor = conn.cursor()
    existing_tables = {
        row[0]
        for row in cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }
    if "processing_queue" in existing_tables and "segmentation_queue" not in existing_tables:
        try:
            cursor.execute("ALTER TABLE processing_queue RENAME TO segmentation_queue")
        except sqlite3.OperationalError as exc:
            if "no such table: processing_queue" not in str(exc).lower():
                raise
        existing_tables.discard("processing_queue")
        existing_tables.add("segmentation_queue")
    cursor.execute("DROP INDEX IF EXISTS idx_processing_queue_status_created")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dicom_metadata (
            StudyInstanceUID TEXT PRIMARY KEY,
            PatientName TEXT,
            PatientID TEXT,
            PatientBirthDate TEXT,
            ClinicalName TEXT,
            AccessionNumber TEXT,
            StudyDate TEXT,
            Modality TEXT,
            CallingAET TEXT,
            RemoteIP TEXT,
            IdJson TEXT,
            JsonDump TEXT,
            DicomMetadata TEXT,
            CalculationResults TEXT,
            PatientSex TEXT,
            Weight REAL,
            Height REAL,
            SMI REAL,
            SegmentationSeriesInstanceUID TEXT,
            SegmentationSliceCount INTEGER,
            SegmentationProfile TEXT,
            SegmentationTasks TEXT,
            SegmentationCompletedAt TIMESTAMP,
            ArtifactsPurged INTEGER DEFAULT 0,
            ArtifactsPurgedAt TIMESTAMP,
            ProcessedAt TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS segmentation_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            case_id TEXT NOT NULL UNIQUE,
            input_path TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TIMESTAMP,
            claimed_at TIMESTAMP,
            finished_at TIMESTAMP,
            error TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_segmentation_queue_status_created
        ON segmentation_queue(status, created_at)
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS metrics_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            case_id TEXT NOT NULL UNIQUE,
            input_path TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TIMESTAMP,
            claimed_at TIMESTAMP,
            finished_at TIMESTAMP,
            error TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_metrics_queue_status_created
        ON metrics_queue(status, created_at)
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dicom_egress_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            case_id TEXT NOT NULL,
            study_uid TEXT,
            artifact_path TEXT NOT NULL,
            artifact_type TEXT NOT NULL,
            destination_name TEXT NOT NULL,
            destination_host TEXT NOT NULL,
            destination_port INTEGER NOT NULL,
            destination_called_aet TEXT NOT NULL,
            source_calling_aet TEXT,
            source_remote_ip TEXT,
            artifact_digest TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP,
            claimed_at TIMESTAMP,
            finished_at TIMESTAMP,
            next_attempt_at TIMESTAMP,
            error TEXT,
            UNIQUE(case_id, artifact_path, destination_name)
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dicom_egress_queue_status_next_attempt
        ON dicom_egress_queue(status, next_attempt_at, created_at)
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS integration_dispatch_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            event_version INTEGER NOT NULL DEFAULT 1,
            event_key TEXT NOT NULL,
            case_id TEXT,
            study_uid TEXT,
            destination_name TEXT NOT NULL,
            destination_url TEXT NOT NULL,
            http_method TEXT NOT NULL DEFAULT 'POST',
            timeout_seconds INTEGER NOT NULL DEFAULT 10,
            request_headers TEXT,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP,
            claimed_at TIMESTAMP,
            finished_at TIMESTAMP,
            next_attempt_at TIMESTAMP,
            response_status INTEGER,
            error TEXT,
            UNIQUE(event_key, destination_name)
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_integration_dispatch_queue_status_next_attempt
        ON integration_dispatch_queue(status, next_attempt_at, created_at)
        """
    )
    _ensure_columns(cursor, "dicom_metadata", _DICOM_METADATA_COLUMNS)
    _ensure_columns(cursor, "dicom_egress_queue", _DICOM_EGRESS_QUEUE_COLUMNS)
    conn.commit()
    if owns_connection:
        conn.close()


def _ensure_columns(cursor: sqlite3.Cursor, table_name: str, columns: dict[str, str]) -> None:
    existing = {
        row[1]
        for row in cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    for name, definition in columns.items():
        if name in existing:
            continue
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {name} {definition}")


def upsert_study_metadata(conn: sqlite3.Connection, metadata: dict[str, Any]) -> None:
    ensure_schema(conn)
    processed_at = _now_local_timestamp()
    conn.execute(
        """
        INSERT INTO dicom_metadata
        (
            StudyInstanceUID,
            PatientName,
            PatientID,
            PatientBirthDate,
            ClinicalName,
            AccessionNumber,
            StudyDate,
            PatientSex,
            Modality,
            CallingAET,
            RemoteIP,
            JsonDump,
            ProcessedAt
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(StudyInstanceUID) DO UPDATE SET
            PatientName = excluded.PatientName,
            PatientID = excluded.PatientID,
            PatientBirthDate = excluded.PatientBirthDate,
            ClinicalName = excluded.ClinicalName,
            AccessionNumber = excluded.AccessionNumber,
            StudyDate = excluded.StudyDate,
            PatientSex = excluded.PatientSex,
            Modality = excluded.Modality,
            CallingAET = COALESCE(NULLIF(excluded.CallingAET, ''), dicom_metadata.CallingAET),
            RemoteIP = COALESCE(NULLIF(excluded.RemoteIP, ''), dicom_metadata.RemoteIP),
            JsonDump = excluded.JsonDump,
            ArtifactsPurged = 0,
            ArtifactsPurgedAt = NULL,
            ProcessedAt = excluded.ProcessedAt
        """,
        (
            metadata["StudyInstanceUID"],
            metadata["PatientName"],
            metadata.get("PatientID", ""),
            metadata.get("PatientBirthDate", ""),
            metadata.get("ClinicalName", ""),
            metadata["AccessionNumber"],
            metadata.get("StudyDate", ""),
            metadata.get("PatientSex", ""),
            metadata["Modality"],
            metadata.get("CallingAET", ""),
            metadata.get("RemoteIP", ""),
            json.dumps(metadata),
            processed_at,
        ),
    )
    conn.commit()


def upsert_intake_metadata(
    conn: sqlite3.Connection,
    study_uid: str,
    *,
    calling_aet: str | None,
    remote_ip: str | None,
) -> None:
    ensure_schema(conn)
    conn.execute(
        """
        INSERT INTO dicom_metadata (StudyInstanceUID, CallingAET, RemoteIP)
        VALUES (?, NULLIF(?, ''), NULLIF(?, ''))
        ON CONFLICT(StudyInstanceUID) DO UPDATE SET
            CallingAET = COALESCE(NULLIF(excluded.CallingAET, ''), dicom_metadata.CallingAET),
            RemoteIP = COALESCE(NULLIF(excluded.RemoteIP, ''), dicom_metadata.RemoteIP)
        """,
        (study_uid, calling_aet or "", remote_ip or ""),
    )
    conn.commit()


def enqueue_segmentation_case(conn: sqlite3.Connection, case_id: str, input_path: str) -> None:
    ensure_schema(conn)
    created_at = _now_local_timestamp()
    existing = conn.execute(
        """
        SELECT status, claimed_at
        FROM segmentation_queue
        WHERE case_id = ?
        """,
        (str(case_id),),
    ).fetchone()
    if existing and existing["status"] == "claimed" and not _is_stale_claimed_at(
        existing["claimed_at"],
        ttl_seconds=settings.SEGMENTATION_CLAIM_TTL_SECONDS,
    ):
        conn.execute(
            """
            UPDATE segmentation_queue
            SET input_path = ?
            WHERE case_id = ?
            """,
            (str(input_path), str(case_id)),
        )
        conn.commit()
        return

    conn.execute("DELETE FROM metrics_queue WHERE case_id = ?", (str(case_id),))
    conn.execute(
        """
        DELETE FROM dicom_egress_queue
        WHERE case_id = ?
          AND status != 'done'
        """,
        (str(case_id),),
    )
    conn.execute(
        """
        INSERT INTO segmentation_queue (case_id, input_path, status, created_at)
        VALUES (?, ?, 'pending', ?)
        ON CONFLICT(case_id) DO UPDATE SET
            input_path = excluded.input_path,
            status = 'pending',
            created_at = excluded.created_at,
            claimed_at = NULL,
            finished_at = NULL,
            error = NULL
        """,
        (str(case_id), str(input_path), created_at),
    )
    conn.commit()


def enqueue_case_for_metrics(conn: sqlite3.Connection, case_id: str, input_path: str) -> None:
    ensure_schema(conn)
    created_at = _now_local_timestamp()
    existing = conn.execute(
        """
        SELECT status, claimed_at
        FROM metrics_queue
        WHERE case_id = ?
        """,
        (str(case_id),),
    ).fetchone()
    if existing and existing["status"] == "claimed" and not _is_stale_claimed_at(
        existing["claimed_at"],
        ttl_seconds=settings.METRICS_CLAIM_TTL_SECONDS,
    ):
        conn.execute(
            """
            UPDATE metrics_queue
            SET input_path = ?
            WHERE case_id = ?
            """,
            (str(input_path), str(case_id)),
        )
        conn.commit()
        return

    conn.execute(
        """
        INSERT INTO metrics_queue (case_id, input_path, status, created_at)
        VALUES (?, ?, 'pending', ?)
        ON CONFLICT(case_id) DO UPDATE SET
            input_path = excluded.input_path,
            status = 'pending',
            created_at = excluded.created_at,
            claimed_at = NULL,
            finished_at = NULL,
            error = NULL
        """,
        (str(case_id), str(input_path), created_at),
    )
    conn.commit()


def enqueue_dicom_export(
    conn: sqlite3.Connection,
    *,
    case_id: str,
    study_uid: str | None,
    artifact_path: str,
    artifact_type: str,
    destination_name: str,
    destination_host: str,
    destination_port: int,
    destination_called_aet: str,
    source_calling_aet: str | None,
    source_remote_ip: str | None,
    artifact_digest: str | None = None,
) -> None:
    ensure_schema(conn)
    created_at = _now_local_timestamp()
    conn.execute(
        """
        INSERT INTO dicom_egress_queue (
            case_id,
            study_uid,
            artifact_path,
            artifact_type,
            destination_name,
            destination_host,
            destination_port,
            destination_called_aet,
            source_calling_aet,
            source_remote_ip,
            artifact_digest,
            status,
            attempts,
            created_at,
            claimed_at,
            finished_at,
            next_attempt_at,
            error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, NULL, NULL, ?, NULL)
        ON CONFLICT(case_id, artifact_path, destination_name) DO UPDATE SET
            study_uid = excluded.study_uid,
            artifact_type = excluded.artifact_type,
            destination_host = excluded.destination_host,
            destination_port = excluded.destination_port,
            destination_called_aet = excluded.destination_called_aet,
            source_calling_aet = excluded.source_calling_aet,
            source_remote_ip = excluded.source_remote_ip,
            artifact_digest = excluded.artifact_digest,
            status = CASE
                WHEN COALESCE(dicom_egress_queue.artifact_digest, '') = COALESCE(excluded.artifact_digest, '')
                     AND dicom_egress_queue.status IN ('pending', 'claimed', 'done')
                THEN dicom_egress_queue.status
                ELSE 'pending'
            END,
            attempts = CASE
                WHEN COALESCE(dicom_egress_queue.artifact_digest, '') = COALESCE(excluded.artifact_digest, '')
                     AND dicom_egress_queue.status IN ('pending', 'claimed', 'done')
                THEN dicom_egress_queue.attempts
                ELSE 0
            END,
            created_at = CASE
                WHEN COALESCE(dicom_egress_queue.artifact_digest, '') = COALESCE(excluded.artifact_digest, '')
                     AND dicom_egress_queue.status IN ('pending', 'claimed', 'done')
                THEN dicom_egress_queue.created_at
                ELSE excluded.created_at
            END,
            claimed_at = CASE
                WHEN COALESCE(dicom_egress_queue.artifact_digest, '') = COALESCE(excluded.artifact_digest, '')
                     AND dicom_egress_queue.status IN ('claimed', 'done')
                THEN dicom_egress_queue.claimed_at
                ELSE NULL
            END,
            finished_at = CASE
                WHEN COALESCE(dicom_egress_queue.artifact_digest, '') = COALESCE(excluded.artifact_digest, '')
                     AND dicom_egress_queue.status = 'done'
                THEN dicom_egress_queue.finished_at
                ELSE NULL
            END,
            next_attempt_at = CASE
                WHEN COALESCE(dicom_egress_queue.artifact_digest, '') = COALESCE(excluded.artifact_digest, '')
                     AND dicom_egress_queue.status IN ('pending', 'claimed', 'done')
                THEN dicom_egress_queue.next_attempt_at
                ELSE excluded.next_attempt_at
            END,
            error = CASE
                WHEN COALESCE(dicom_egress_queue.artifact_digest, '') = COALESCE(excluded.artifact_digest, '')
                     AND dicom_egress_queue.status IN ('pending', 'claimed', 'done')
                THEN dicom_egress_queue.error
                ELSE NULL
            END
        """,
        (
            str(case_id),
            study_uid,
            str(artifact_path),
            str(artifact_type),
            str(destination_name),
            str(destination_host),
            int(destination_port),
            str(destination_called_aet),
            source_calling_aet or None,
            source_remote_ip or None,
            artifact_digest or None,
            created_at,
            created_at,
        ),
    )
    conn.commit()


def enqueue_integration_dispatch(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    event_version: int,
    event_key: str,
    case_id: str | None,
    study_uid: str | None,
    destination_name: str,
    destination_url: str,
    http_method: str,
    timeout_seconds: int,
    request_headers: dict[str, Any] | None,
    payload: dict[str, Any],
) -> None:
    ensure_schema(conn)
    created_at = _now_local_timestamp()
    conn.execute(
        """
        INSERT INTO integration_dispatch_queue (
            event_type,
            event_version,
            event_key,
            case_id,
            study_uid,
            destination_name,
            destination_url,
            http_method,
            timeout_seconds,
            request_headers,
            payload_json,
            status,
            attempts,
            created_at,
            claimed_at,
            finished_at,
            next_attempt_at,
            response_status,
            error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, NULL, NULL, ?, NULL, NULL)
        ON CONFLICT(event_key, destination_name) DO UPDATE SET
            event_type = excluded.event_type,
            event_version = excluded.event_version,
            case_id = excluded.case_id,
            study_uid = excluded.study_uid,
            destination_url = excluded.destination_url,
            http_method = excluded.http_method,
            timeout_seconds = excluded.timeout_seconds,
            request_headers = excluded.request_headers,
            payload_json = excluded.payload_json,
            status = 'pending',
            attempts = 0,
            created_at = excluded.created_at,
            claimed_at = NULL,
            finished_at = NULL,
            next_attempt_at = excluded.next_attempt_at,
            response_status = NULL,
            error = NULL
        """,
        (
            str(event_type),
            int(event_version),
            str(event_key),
            str(case_id) if case_id is not None else None,
            str(study_uid) if study_uid is not None else None,
            str(destination_name),
            str(destination_url),
            str(http_method).upper(),
            int(timeout_seconds),
            json.dumps(request_headers or {}),
            json.dumps(payload),
            created_at,
            created_at,
        ),
    )
    conn.commit()


def reset_claimed_segmentation_queue_items(conn: sqlite3.Connection) -> int:
    ensure_schema(conn)
    cursor = conn.execute(
        """
        UPDATE segmentation_queue
        SET status = 'pending',
            claimed_at = NULL,
            finished_at = NULL,
            error = NULL
        WHERE status = 'claimed'
        """
    )
    conn.commit()
    return int(cursor.rowcount or 0)


def reset_claimed_metrics_queue_items(conn: sqlite3.Connection) -> int:
    ensure_schema(conn)
    cursor = conn.execute(
        """
        UPDATE metrics_queue
        SET status = 'pending',
            claimed_at = NULL,
            finished_at = NULL,
            error = NULL
        WHERE status = 'claimed'
        """
    )
    conn.commit()
    return int(cursor.rowcount or 0)


def requeue_stale_claimed_segmentation_items(conn: sqlite3.Connection, *, ttl_seconds: int) -> int:
    ensure_schema(conn)
    rows = conn.execute(
        """
        SELECT id, claimed_at
        FROM segmentation_queue
        WHERE status = 'claimed'
        """
    ).fetchall()
    stale_ids = [
        int(row["id"])
        for row in rows
        if _is_stale_claimed_at(row["claimed_at"], ttl_seconds=ttl_seconds)
    ]
    if not stale_ids:
        return 0
    conn.executemany(
        """
        UPDATE segmentation_queue
        SET status = 'pending',
            claimed_at = NULL,
            finished_at = NULL,
            error = NULL
        WHERE id = ?
        """,
        [(queue_id,) for queue_id in stale_ids],
    )
    conn.commit()
    return len(stale_ids)


def requeue_stale_claimed_metrics_items(conn: sqlite3.Connection, *, ttl_seconds: int) -> int:
    ensure_schema(conn)
    rows = conn.execute(
        """
        SELECT id, claimed_at
        FROM metrics_queue
        WHERE status = 'claimed'
        """
    ).fetchall()
    stale_ids = [
        int(row["id"])
        for row in rows
        if _is_stale_claimed_at(row["claimed_at"], ttl_seconds=ttl_seconds)
    ]
    if not stale_ids:
        return 0
    conn.executemany(
        """
        UPDATE metrics_queue
        SET status = 'pending',
            claimed_at = NULL,
            finished_at = NULL,
            error = NULL
        WHERE id = ?
        """,
        [(queue_id,) for queue_id in stale_ids],
    )
    conn.commit()
    return len(stale_ids)


def touch_segmentation_queue_item_claim(conn: sqlite3.Connection, queue_id: int) -> bool:
    ensure_schema(conn)
    cursor = conn.execute(
        """
        UPDATE segmentation_queue
        SET claimed_at = ?
        WHERE id = ?
          AND status = 'claimed'
        """,
        (_now_local_timestamp(), int(queue_id)),
    )
    conn.commit()
    return cursor.rowcount == 1


def touch_metrics_queue_item_claim(conn: sqlite3.Connection, queue_id: int) -> bool:
    ensure_schema(conn)
    cursor = conn.execute(
        """
        UPDATE metrics_queue
        SET claimed_at = ?
        WHERE id = ?
          AND status = 'claimed'
        """,
        (_now_local_timestamp(), int(queue_id)),
    )
    conn.commit()
    return cursor.rowcount == 1


def claim_next_pending_segmentation_queue_item(conn: sqlite3.Connection) -> tuple[int, str, str] | None:
    ensure_schema(conn)
    requeue_stale_claimed_segmentation_items(
        conn,
        ttl_seconds=settings.SEGMENTATION_CLAIM_TTL_SECONDS,
    )
    claimed_at = _now_local_timestamp()
    cursor = conn.cursor()
    cursor.execute("BEGIN IMMEDIATE")
    cursor.execute(
        """
        SELECT id, case_id, input_path
        FROM segmentation_queue
        WHERE status = 'pending'
        ORDER BY created_at ASC, id ASC
        LIMIT 1
        """
    )
    row = cursor.fetchone()
    if not row:
        conn.commit()
        return None

    queue_id, case_id, input_path = row
    cursor.execute(
        """
        UPDATE segmentation_queue
        SET status = 'claimed',
            claimed_at = ?,
            error = NULL
        WHERE id = ? AND status = 'pending'
        """,
        (claimed_at, queue_id),
    )
    if cursor.rowcount != 1:
        conn.rollback()
        return None
    conn.commit()
    return queue_id, case_id, input_path


def claim_next_pending_metrics_queue_item(conn: sqlite3.Connection) -> tuple[int, str, str] | None:
    ensure_schema(conn)
    requeue_stale_claimed_metrics_items(
        conn,
        ttl_seconds=settings.METRICS_CLAIM_TTL_SECONDS,
    )
    claimed_at = _now_local_timestamp()
    cursor = conn.cursor()
    cursor.execute("BEGIN IMMEDIATE")
    cursor.execute(
        """
        SELECT id, case_id, input_path
        FROM metrics_queue
        WHERE status = 'pending'
        ORDER BY created_at ASC, id ASC
        LIMIT 1
        """
    )
    row = cursor.fetchone()
    if not row:
        conn.commit()
        return None

    queue_id, case_id, input_path = row
    cursor.execute(
        """
        UPDATE metrics_queue
        SET status = 'claimed',
            claimed_at = ?,
            error = NULL
        WHERE id = ? AND status = 'pending'
        """,
        (claimed_at, queue_id),
    )
    if cursor.rowcount != 1:
        conn.rollback()
        return None
    conn.commit()
    return queue_id, case_id, input_path


def claim_next_pending_dicom_egress_queue_item(
    conn: sqlite3.Connection,
) -> tuple[int, str, str | None, str, str, str, str, int, str, int] | None:
    ensure_schema(conn)
    claimed_at = _now_local_timestamp()
    cursor = conn.cursor()
    cursor.execute("BEGIN IMMEDIATE")
    cursor.execute(
        """
        SELECT
            id,
            case_id,
            study_uid,
            artifact_path,
            artifact_type,
            destination_name,
            destination_host,
            destination_port,
            destination_called_aet,
            attempts
        FROM dicom_egress_queue
        WHERE status = 'pending'
          AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
        ORDER BY next_attempt_at ASC, created_at ASC, id ASC
        LIMIT 1
        """,
        (claimed_at,),
    )
    row = cursor.fetchone()
    if not row:
        conn.commit()
        return None

    queue_id = row[0]
    cursor.execute(
        """
        UPDATE dicom_egress_queue
        SET status = 'claimed',
            claimed_at = ?,
            attempts = attempts + 1,
            error = NULL
        WHERE id = ? AND status = 'pending'
        """,
        (claimed_at, queue_id),
    )
    if cursor.rowcount != 1:
        conn.rollback()
        return None
    conn.commit()
    return row


def claim_next_pending_integration_dispatch_queue_item(
    conn: sqlite3.Connection,
) -> tuple[int, str, int, str, str | None, str | None, str, str, str, int, str, str, int] | None:
    ensure_schema(conn)
    claimed_at = _now_local_timestamp()
    cursor = conn.cursor()
    cursor.execute("BEGIN IMMEDIATE")
    cursor.execute(
        """
        SELECT
            id,
            event_type,
            event_version,
            event_key,
            case_id,
            study_uid,
            destination_name,
            destination_url,
            http_method,
            timeout_seconds,
            request_headers,
            payload_json,
            attempts
        FROM integration_dispatch_queue
        WHERE status = 'pending'
          AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
        ORDER BY next_attempt_at ASC, created_at ASC, id ASC
        LIMIT 1
        """,
        (claimed_at,),
    )
    row = cursor.fetchone()
    if not row:
        conn.commit()
        return None

    queue_id = row[0]
    cursor.execute(
        """
        UPDATE integration_dispatch_queue
        SET status = 'claimed',
            claimed_at = ?,
            attempts = attempts + 1,
            response_status = NULL,
            error = NULL
        WHERE id = ? AND status = 'pending'
        """,
        (claimed_at, queue_id),
    )
    if cursor.rowcount != 1:
        conn.rollback()
        return None
    conn.commit()
    return row


def mark_segmentation_queue_item_done(conn: sqlite3.Connection, queue_id: int) -> None:
    finished_at = _now_local_timestamp()
    conn.execute(
        """
        UPDATE segmentation_queue
        SET status = 'done',
            finished_at = ?,
            error = NULL
        WHERE id = ?
        """,
        (finished_at, queue_id),
    )
    conn.commit()


def mark_metrics_queue_item_done(conn: sqlite3.Connection, queue_id: int) -> None:
    finished_at = _now_local_timestamp()
    conn.execute(
        """
        UPDATE metrics_queue
        SET status = 'done',
            finished_at = ?,
            error = NULL
        WHERE id = ?
        """,
        (finished_at, queue_id),
    )
    conn.commit()


def mark_dicom_egress_queue_item_done(conn: sqlite3.Connection, queue_id: int) -> None:
    finished_at = _now_local_timestamp()
    conn.execute(
        """
        UPDATE dicom_egress_queue
        SET status = 'done',
            finished_at = ?,
            next_attempt_at = NULL,
            error = NULL
        WHERE id = ?
        """,
        (finished_at, queue_id),
    )
    conn.commit()


def mark_integration_dispatch_queue_item_done(
    conn: sqlite3.Connection,
    queue_id: int,
    *,
    response_status: int | None,
) -> None:
    finished_at = _now_local_timestamp()
    conn.execute(
        """
        UPDATE integration_dispatch_queue
        SET status = 'done',
            finished_at = ?,
            next_attempt_at = NULL,
            response_status = ?,
            error = NULL
        WHERE id = ?
        """,
        (finished_at, int(response_status) if response_status is not None else None, queue_id),
    )
    conn.commit()


def mark_segmentation_queue_item_error(conn: sqlite3.Connection, queue_id: int, error_message: Any) -> None:
    finished_at = _now_local_timestamp()
    conn.execute(
        """
        UPDATE segmentation_queue
        SET status = 'error',
            finished_at = ?,
            error = ?
        WHERE id = ?
        """,
        (finished_at, str(error_message)[:2000], queue_id),
    )
    conn.commit()


def mark_metrics_queue_item_error(conn: sqlite3.Connection, queue_id: int, error_message: Any) -> None:
    finished_at = _now_local_timestamp()
    conn.execute(
        """
        UPDATE metrics_queue
        SET status = 'error',
            finished_at = ?,
            error = ?
        WHERE id = ?
        """,
        (finished_at, str(error_message)[:2000], queue_id),
    )
    conn.commit()


def retry_dicom_egress_queue_item(
    conn: sqlite3.Connection,
    queue_id: int,
    error_message: Any,
    *,
    backoff_seconds: int,
) -> None:
    conn.execute(
        """
        UPDATE dicom_egress_queue
        SET status = 'pending',
            claimed_at = NULL,
            finished_at = NULL,
            next_attempt_at = ?,
            error = ?
        WHERE id = ?
        """,
        (
            _future_local_timestamp(backoff_seconds),
            str(error_message)[:2000],
            queue_id,
        ),
    )
    conn.commit()


def retry_integration_dispatch_queue_item(
    conn: sqlite3.Connection,
    queue_id: int,
    error_message: Any,
    *,
    backoff_seconds: int,
    response_status: int | None = None,
) -> None:
    conn.execute(
        """
        UPDATE integration_dispatch_queue
        SET status = 'pending',
            claimed_at = NULL,
            finished_at = NULL,
            next_attempt_at = ?,
            response_status = ?,
            error = ?
        WHERE id = ?
        """,
        (
            _future_local_timestamp(backoff_seconds),
            int(response_status) if response_status is not None else None,
            str(error_message)[:2000],
            queue_id,
        ),
    )
    conn.commit()


def mark_dicom_egress_queue_item_error(
    conn: sqlite3.Connection,
    queue_id: int,
    error_message: Any,
) -> None:
    finished_at = _now_local_timestamp()
    conn.execute(
        """
        UPDATE dicom_egress_queue
        SET status = 'error',
            finished_at = ?,
            next_attempt_at = NULL,
            error = ?
        WHERE id = ?
        """,
        (finished_at, str(error_message)[:2000], queue_id),
    )
    conn.commit()


def mark_integration_dispatch_queue_item_error(
    conn: sqlite3.Connection,
    queue_id: int,
    error_message: Any,
    *,
    response_status: int | None = None,
) -> None:
    finished_at = _now_local_timestamp()
    conn.execute(
        """
        UPDATE integration_dispatch_queue
        SET status = 'error',
            finished_at = ?,
            next_attempt_at = NULL,
            response_status = ?,
            error = ?
        WHERE id = ?
        """,
        (
            finished_at,
            int(response_status) if response_status is not None else None,
            str(error_message)[:2000],
            queue_id,
        ),
    )
    conn.commit()


def update_full_dicom_metadata(conn: sqlite3.Connection, study_uid: str, full_meta: dict[str, Any]) -> None:
    conn.execute(
        "UPDATE dicom_metadata SET DicomMetadata = ? WHERE StudyInstanceUID = ?",
        (json.dumps(full_meta), study_uid),
    )
    conn.commit()


def update_id_json(conn: sqlite3.Connection, study_uid: str, metadata: dict[str, Any]) -> None:
    conn.execute(
        "UPDATE dicom_metadata SET IdJson = ?, Weight = ?, Height = ? WHERE StudyInstanceUID = ?",
        (json.dumps(metadata), metadata.get("Weight"), metadata.get("Height"), study_uid),
    )
    conn.commit()


def get_recorded_segmentation_signature(conn: sqlite3.Connection, study_uid: str) -> sqlite3.Row | None:
    ensure_schema(conn)
    return conn.execute(
        """
        SELECT
            SegmentationSeriesInstanceUID,
            SegmentationSliceCount,
            SegmentationProfile,
            SegmentationTasks,
            SegmentationCompletedAt
        FROM dicom_metadata
        WHERE StudyInstanceUID = ?
        """,
        (study_uid,),
    ).fetchone()


def get_pipeline_completion_state(conn: sqlite3.Connection, study_uid: str) -> sqlite3.Row | None:
    ensure_schema(conn)
    return conn.execute(
        """
        SELECT
            SegmentationSeriesInstanceUID,
            SegmentationSliceCount,
            SegmentationProfile,
            SegmentationTasks,
            SegmentationCompletedAt,
            MetricsProfile,
            MetricsCompletedAt,
            ArtifactsPurged
        FROM dicom_metadata
        WHERE StudyInstanceUID = ?
        """,
        (study_uid,),
    ).fetchone()


def update_segmentation_signature(
    conn: sqlite3.Connection,
    study_uid: str,
    *,
    series_instance_uid: str | None,
    slice_count: int | None,
    profile_name: str,
    task_names: list[str],
) -> None:
    ensure_schema(conn)
    conn.execute(
        """
        UPDATE dicom_metadata
        SET SegmentationSeriesInstanceUID = ?,
            SegmentationSliceCount = ?,
            SegmentationProfile = ?,
            SegmentationTasks = ?,
            SegmentationCompletedAt = ?
        WHERE StudyInstanceUID = ?
        """,
        (
            series_instance_uid,
            int(slice_count) if slice_count is not None else None,
            profile_name,
            json.dumps(task_names),
            _now_local_timestamp(),
            study_uid,
        ),
    )
    conn.commit()


def update_metrics_completion(
    conn: sqlite3.Connection,
    study_uid: str,
    *,
    profile_name: str,
) -> None:
    ensure_schema(conn)
    conn.execute(
        """
        UPDATE dicom_metadata
        SET MetricsProfile = ?,
            MetricsCompletedAt = ?
        WHERE StudyInstanceUID = ?
        """,
        (
            str(profile_name),
            _now_local_timestamp(),
            study_uid,
        ),
    )
    conn.commit()


def update_study_biometrics(
    conn: sqlite3.Connection,
    study_uid: str,
    *,
    weight: float | None,
    height: float | None,
) -> None:
    conn.execute(
        "UPDATE dicom_metadata SET Weight = ?, Height = ? WHERE StudyInstanceUID = ?",
        (weight, height, study_uid),
    )
    conn.commit()


def update_calculation_results(conn: sqlite3.Connection, study_uid: str, results: dict[str, Any]) -> None:
    conn.execute(
        "UPDATE dicom_metadata SET CalculationResults = ? WHERE StudyInstanceUID = ?",
        (json.dumps(results), study_uid),
    )
    conn.commit()


def list_patient_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    ensure_schema(conn)
    return conn.execute("SELECT * FROM dicom_metadata ORDER BY StudyDate DESC").fetchall()


def find_case_row_by_case_id(conn: sqlite3.Connection, case_id: str) -> sqlite3.Row | None:
    ensure_schema(conn)
    try:
        row = conn.execute(
            """
            SELECT
                StudyInstanceUID,
                IdJson,
                PatientID,
                PatientBirthDate,
                Weight,
                Height,
                PatientSex,
                CalculationResults,
                SMI,
                ArtifactsPurged,
                ArtifactsPurgedAt
            FROM dicom_metadata
            WHERE json_extract(IdJson, '$.CaseID') = ?
            """,
            (case_id,),
        ).fetchone()
        if row:
            return row
    except Exception:
        pass

    for row in conn.execute(
        """
        SELECT
                StudyInstanceUID,
                IdJson,
                PatientID,
                PatientBirthDate,
                Weight,
                Height,
            PatientSex,
            CalculationResults,
            SMI,
            ArtifactsPurged,
            ArtifactsPurgedAt
        FROM dicom_metadata
        """
    ).fetchall():
        metadata = json.loads(row["IdJson"]) if row["IdJson"] else {}
        if metadata.get("CaseID") == case_id:
            return row
    return None


def list_protected_case_ids(conn: sqlite3.Connection) -> set[str]:
    """Return case IDs that should not be purged while still active in queues."""
    ensure_schema(conn)
    protected: set[str] = set()
    for table_name in ("segmentation_queue", "metrics_queue", "dicom_egress_queue"):
        rows = conn.execute(
            f"""
            SELECT DISTINCT case_id
            FROM {table_name}
            WHERE status IN ('pending', 'claimed')
            """
        ).fetchall()
        protected.update(str(row["case_id"]) for row in rows if row["case_id"])
    return protected


def case_has_incomplete_dicom_egress(conn: sqlite3.Connection, case_id: str) -> bool:
    ensure_schema(conn)
    row = conn.execute(
        """
        SELECT 1
        FROM dicom_egress_queue
        WHERE case_id = ?
          AND status != 'done'
        LIMIT 1
        """,
        (case_id,),
    ).fetchone()
    return row is not None


def case_has_incomplete_metrics(conn: sqlite3.Connection, case_id: str) -> bool:
    ensure_schema(conn)
    row = conn.execute(
        """
        SELECT 1
        FROM metrics_queue
        WHERE case_id = ?
          AND status != 'done'
        LIMIT 1
        """,
        (case_id,),
    ).fetchone()
    return row is not None


def get_case_metrics_queue_statuses(conn: sqlite3.Connection, case_id: str) -> set[str]:
    ensure_schema(conn)
    rows = conn.execute(
        """
        SELECT DISTINCT status
        FROM metrics_queue
        WHERE case_id = ?
        """,
        (case_id,),
    ).fetchall()
    return {str(row[0]) for row in rows if row and row[0]}


def get_case_dicom_egress_statuses(conn: sqlite3.Connection, case_id: str) -> set[str]:
    ensure_schema(conn)
    rows = conn.execute(
        """
        SELECT DISTINCT status
        FROM dicom_egress_queue
        WHERE case_id = ?
        """,
        (case_id,),
    ).fetchall()
    return {str(row[0]) for row in rows if row and row[0]}


def purge_case_records(conn: sqlite3.Connection, case_id: str) -> str | None:
    """Delete queue rows and mark the metadata row as purged for a case."""
    ensure_schema(conn)
    study_uid = None

    case_row = find_case_row_by_case_id(conn, case_id)
    if case_row:
        study_uid = case_row["StudyInstanceUID"]
    else:
        fallback = conn.execute(
            "SELECT StudyInstanceUID FROM dicom_metadata WHERE ClinicalName = ?",
            (case_id,),
        ).fetchone()
        if fallback:
            study_uid = fallback["StudyInstanceUID"]

    conn.execute("DELETE FROM segmentation_queue WHERE case_id = ?", (case_id,))
    conn.execute("DELETE FROM metrics_queue WHERE case_id = ?", (case_id,))
    conn.execute("DELETE FROM dicom_egress_queue WHERE case_id = ?", (case_id,))
    if study_uid:
        conn.execute(
            """
            UPDATE dicom_metadata
            SET ArtifactsPurged = 1,
                ArtifactsPurgedAt = ?
            WHERE StudyInstanceUID = ?
            """,
            (_now_local_timestamp(), study_uid),
        )
    conn.commit()
    return study_uid
