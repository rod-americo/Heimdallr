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
    "ProcessedAt": "TIMESTAMP",
}


LOCAL_TZ = ZoneInfo(settings.TIMEZONE)


def _now_local_timestamp() -> str:
    """Return current wall-clock time in the configured local timezone."""
    return datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _future_local_timestamp(seconds: int) -> str:
    """Return a future wall-clock time in the configured local timezone."""
    return (datetime.now(LOCAL_TZ) + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")


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
        cursor.execute("ALTER TABLE processing_queue RENAME TO segmentation_queue")
        existing_tables.discard("processing_queue")
        existing_tables.add("segmentation_queue")
    cursor.execute("DROP INDEX IF EXISTS idx_processing_queue_status_created")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dicom_metadata (
            StudyInstanceUID TEXT PRIMARY KEY,
            PatientName TEXT,
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
    _ensure_columns(cursor, "dicom_metadata", _DICOM_METADATA_COLUMNS)
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(StudyInstanceUID) DO UPDATE SET
            PatientName = excluded.PatientName,
            ClinicalName = excluded.ClinicalName,
            AccessionNumber = excluded.AccessionNumber,
            StudyDate = excluded.StudyDate,
            PatientSex = excluded.PatientSex,
            Modality = excluded.Modality,
            CallingAET = COALESCE(NULLIF(excluded.CallingAET, ''), dicom_metadata.CallingAET),
            RemoteIP = COALESCE(NULLIF(excluded.RemoteIP, ''), dicom_metadata.RemoteIP),
            JsonDump = excluded.JsonDump,
            ProcessedAt = excluded.ProcessedAt
        """,
        (
            metadata["StudyInstanceUID"],
            metadata["PatientName"],
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
            status,
            attempts,
            created_at,
            claimed_at,
            finished_at,
            next_attempt_at,
            error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, NULL, NULL, ?, NULL)
        ON CONFLICT(case_id, artifact_path, destination_name) DO UPDATE SET
            study_uid = excluded.study_uid,
            artifact_type = excluded.artifact_type,
            destination_host = excluded.destination_host,
            destination_port = excluded.destination_port,
            destination_called_aet = excluded.destination_called_aet,
            source_calling_aet = excluded.source_calling_aet,
            source_remote_ip = excluded.source_remote_ip,
            status = 'pending',
            attempts = 0,
            created_at = excluded.created_at,
            claimed_at = NULL,
            finished_at = NULL,
            next_attempt_at = excluded.next_attempt_at,
            error = NULL
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
            created_at,
            created_at,
        ),
    )
    conn.commit()


def claim_next_pending_segmentation_queue_item(conn: sqlite3.Connection) -> tuple[int, str, str] | None:
    ensure_schema(conn)
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
            SELECT StudyInstanceUID, IdJson, Weight, Height, PatientSex, CalculationResults, SMI
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
        "SELECT StudyInstanceUID, IdJson, Weight, Height, PatientSex, CalculationResults, SMI FROM dicom_metadata"
    ).fetchall():
        metadata = json.loads(row["IdJson"]) if row["IdJson"] else {}
        if metadata.get("CaseID") == case_id:
            return row
    return None
