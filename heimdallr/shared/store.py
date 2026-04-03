"""Operational SQLite store for Heimdallr."""

from __future__ import annotations

from datetime import datetime
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


def ensure_schema(conn: sqlite3.Connection | None = None) -> None:
    owns_connection = conn is None
    if conn is None:
        conn = connect()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dicom_metadata (
            StudyInstanceUID TEXT PRIMARY KEY,
            PatientName TEXT,
            ClinicalName TEXT,
            AccessionNumber TEXT,
            StudyDate TEXT,
            Modality TEXT,
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
        CREATE TABLE IF NOT EXISTS processing_queue (
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
        CREATE INDEX IF NOT EXISTS idx_processing_queue_status_created
        ON processing_queue(status, created_at)
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
        INSERT OR REPLACE INTO dicom_metadata
        (StudyInstanceUID, PatientName, ClinicalName, AccessionNumber, StudyDate, PatientSex, Modality, JsonDump, ProcessedAt)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            metadata["StudyInstanceUID"],
            metadata["PatientName"],
            metadata.get("ClinicalName", ""),
            metadata["AccessionNumber"],
            metadata.get("StudyDate", ""),
            metadata.get("PatientSex", ""),
            metadata["Modality"],
            json.dumps(metadata),
            processed_at,
        ),
    )
    conn.commit()


def enqueue_case(conn: sqlite3.Connection, case_id: str, input_path: str) -> None:
    ensure_schema(conn)
    created_at = _now_local_timestamp()
    conn.execute(
        """
        INSERT INTO processing_queue (case_id, input_path, status, created_at)
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


def claim_next_pending_queue_item(conn: sqlite3.Connection) -> tuple[int, str, str] | None:
    ensure_schema(conn)
    claimed_at = _now_local_timestamp()
    cursor = conn.cursor()
    cursor.execute("BEGIN IMMEDIATE")
    cursor.execute(
        """
        SELECT id, case_id, input_path
        FROM processing_queue
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
        UPDATE processing_queue
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


def mark_queue_item_done(conn: sqlite3.Connection, queue_id: int) -> None:
    finished_at = _now_local_timestamp()
    conn.execute(
        """
        UPDATE processing_queue
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


def mark_queue_item_error(conn: sqlite3.Connection, queue_id: int, error_message: Any) -> None:
    finished_at = _now_local_timestamp()
    conn.execute(
        """
        UPDATE processing_queue
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
