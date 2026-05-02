import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from heimdallr.shared import settings
from heimdallr.tui.snapshot import (
    RuntimeLayout,
    _derive_stage_key,
    _format_process_elapsed,
    build_snapshot,
)


class TestTuiSnapshot(unittest.TestCase):
    def test_format_process_elapsed_shortens_ps_etime(self):
        self.assertEqual(_format_process_elapsed("1-21:10:49"), "1d 21:10")
        self.assertEqual(_format_process_elapsed("17:52:00"), "17:52")
        self.assertEqual(_format_process_elapsed("05:09"), "0:05")

    def test_derive_stage_key_prefers_active_segmentation_over_stale_metrics_error(self):
        stage = _derive_stage_key(
            {
                "segmentation_queue_status": "claimed",
                "metrics_queue_status": "error",
                "metrics_started": False,
                "metrics_finished": False,
                "has_results": False,
                "failed_file": False,
                "has_error_log": False,
                "active_file": False,
                "pending_file": False,
                "path": None,
            }
        )

        self.assertEqual(stage, "segmentation")

    def test_derive_stage_key_marks_ineligible_before_failed(self):
        stage = _derive_stage_key(
            {
                "segmentation_queue_status": "error",
                "metrics_queue_status": "",
                "metrics_started": False,
                "metrics_finished": False,
                "has_results": False,
                "failed_file": False,
                "has_error_log": True,
                "active_file": False,
                "pending_file": False,
                "path": object(),
                "segmentation_status": "ineligible",
            }
        )

        self.assertEqual(stage, "ineligible")

    def test_build_snapshot_marks_reused_segmentation_in_signal_and_elapsed(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            runtime = base / "runtime"
            uploads = runtime / "intake" / "uploads"
            uploads_failed = runtime / "intake" / "uploads_failed"
            dicom_incoming = runtime / "intake" / "dicom" / "incoming"
            dicom_failed = runtime / "intake" / "dicom" / "failed"
            pending = runtime / "queue" / "pending"
            active = runtime / "queue" / "active"
            failed = runtime / "queue" / "failed"
            studies = runtime / "studies"
            for path in (uploads, uploads_failed, dicom_incoming, dicom_failed, pending, active, failed, studies):
                path.mkdir(parents=True, exist_ok=True)

            case_dir = studies / "ReuseCase_20260413_1"
            (case_dir / "metadata").mkdir(parents=True, exist_ok=True)
            (case_dir / "metadata" / "id.json").write_text(
                json.dumps(
                    {
                        "CaseID": "ReuseCase_20260413_1",
                        "PatientName": "Reuse Example",
                        "AccessionNumber": "123",
                        "Modality": "CT",
                        "StudyDate": "20260413",
                        "Pipeline": {
                            "segmentation_elapsed_time": "0:00:08.000000",
                            "segmentation_original_elapsed_time": "0:03:21",
                            "metrics_start_time": "2026-04-13T12:07:40-03:00",
                            "metrics_end_time": "2026-04-13T12:07:57-03:00",
                            "metrics_elapsed_time": "0:00:17.000000",
                            "segmentation_pipeline": {
                                "reused_existing_outputs": True,
                                "reuse_reason": "sqlite_signature_match",
                            },
                        },
                        "AvailableSeries": [{}],
                    }
                ),
                encoding="utf-8",
            )
            (case_dir / "metadata" / "resultados.json").write_text("{}", encoding="utf-8")

            snapshot = build_snapshot(
                layout=RuntimeLayout(
                    runtime_dir=runtime,
                    intake_dir=runtime / "intake",
                    uploads_dir=uploads,
                    uploads_failed_dir=uploads_failed,
                    dicom_incoming_dir=dicom_incoming,
                    dicom_failed_dir=dicom_failed,
                    pending_dir=pending,
                    active_dir=active,
                    failed_dir=failed,
                    studies_dir=studies,
                ),
                db_path=base / "missing.db",
            )

            case = next(item for item in snapshot.cases if item.case_id == "ReuseCase_20260413_1")
            self.assertEqual(case.stage_key, "processed")
            self.assertEqual(case.segmentation_elapsed, "reused (0:03:21)")
            self.assertEqual(case.signal, "results ready from reused segmentation")

    def test_build_snapshot_marks_prepare_duplicate_skip_separately(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            runtime = base / "runtime"
            uploads = runtime / "intake" / "uploads"
            uploads_failed = runtime / "intake" / "uploads_failed"
            dicom_incoming = runtime / "intake" / "dicom" / "incoming"
            dicom_failed = runtime / "intake" / "dicom" / "failed"
            pending = runtime / "queue" / "pending"
            active = runtime / "queue" / "active"
            failed = runtime / "queue" / "failed"
            studies = runtime / "studies"
            for path in (uploads, uploads_failed, dicom_incoming, dicom_failed, pending, active, failed, studies):
                path.mkdir(parents=True, exist_ok=True)

            case_dir = studies / "DupCase_20260413_1"
            (case_dir / "metadata").mkdir(parents=True, exist_ok=True)
            (case_dir / "metadata" / "id.json").write_text(
                json.dumps(
                    {
                        "CaseID": "DupCase_20260413_1",
                        "PatientName": "Dup Example",
                        "AccessionNumber": "456",
                        "Modality": "CT",
                        "StudyDate": "20260413",
                        "Pipeline": {
                            "segmentation_elapsed_time": "0:00:05.000000",
                            "segmentation_original_elapsed_time": "0:04:12",
                            "segmentation_pipeline": {
                                "reused_existing_outputs": True,
                                "reuse_reason": "prepare_duplicate_complete",
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            (case_dir / "metadata" / "resultados.json").write_text("{}", encoding="utf-8")

            snapshot = build_snapshot(
                layout=RuntimeLayout(
                    runtime_dir=runtime,
                    intake_dir=runtime / "intake",
                    uploads_dir=uploads,
                    uploads_failed_dir=uploads_failed,
                    dicom_incoming_dir=dicom_incoming,
                    dicom_failed_dir=dicom_failed,
                    pending_dir=pending,
                    active_dir=active,
                    failed_dir=failed,
                    studies_dir=studies,
                ),
                db_path=base / "missing.db",
            )

            case = next(item for item in snapshot.cases if item.case_id == "DupCase_20260413_1")
            self.assertEqual(case.segmentation_elapsed, "duplicate (0:04:12)")
            self.assertEqual(case.signal, "results ready from prepare-level duplicate skip")

    def test_build_snapshot_marks_ineligible_case_from_pipeline_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            runtime = base / "runtime"
            uploads = runtime / "intake" / "uploads"
            uploads_failed = runtime / "intake" / "uploads_failed"
            dicom_incoming = runtime / "intake" / "dicom" / "incoming"
            dicom_failed = runtime / "intake" / "dicom" / "failed"
            pending = runtime / "queue" / "pending"
            active = runtime / "queue" / "active"
            failed = runtime / "queue" / "failed"
            studies = runtime / "studies"
            for path in (uploads, uploads_failed, dicom_incoming, dicom_failed, pending, active, failed, studies):
                path.mkdir(parents=True, exist_ok=True)

            case_dir = studies / "IneligibleCase_20260420_1"
            (case_dir / "metadata").mkdir(parents=True, exist_ok=True)
            (case_dir / "logs").mkdir(parents=True, exist_ok=True)
            (case_dir / "metadata" / "id.json").write_text(
                json.dumps(
                    {
                        "CaseID": "IneligibleCase_20260420_1",
                        "PatientName": "Ineligible Example",
                        "AccessionNumber": "789",
                        "Modality": "CT",
                        "StudyDate": "20260420",
                        "Pipeline": {
                            "segmentation_status": "ineligible",
                        },
                    }
                ),
                encoding="utf-8",
            )
            (case_dir / "logs" / "error.log").write_text("No eligible series found", encoding="utf-8")

            snapshot = build_snapshot(
                layout=RuntimeLayout(
                    runtime_dir=runtime,
                    intake_dir=runtime / "intake",
                    uploads_dir=uploads,
                    uploads_failed_dir=uploads_failed,
                    dicom_incoming_dir=dicom_incoming,
                    dicom_failed_dir=dicom_failed,
                    pending_dir=pending,
                    active_dir=active,
                    failed_dir=failed,
                    studies_dir=studies,
                ),
                db_path=base / "missing.db",
            )

            case = next(item for item in snapshot.cases if item.case_id == "IneligibleCase_20260420_1")
            self.assertEqual(case.stage_key, "ineligible")
            self.assertEqual(case.segmentation_status, "ineligible")
            self.assertEqual(case.signal, "series ineligible for the profile")

    def test_build_snapshot_classifies_pipeline_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            runtime = base / "runtime"
            uploads = runtime / "intake" / "uploads"
            uploads_failed = runtime / "intake" / "uploads_failed"
            dicom_incoming = runtime / "intake" / "dicom" / "incoming"
            dicom_failed = runtime / "intake" / "dicom" / "failed"
            pending = runtime / "queue" / "pending"
            active = runtime / "queue" / "active"
            failed = runtime / "queue" / "failed"
            studies = runtime / "studies"
            for path in (uploads, uploads_failed, dicom_incoming, dicom_failed, pending, active, failed, studies):
                path.mkdir(parents=True, exist_ok=True)

            (uploads / "fresh_case.zip").write_bytes(b"zip")
            (uploads_failed / "broken_case.zip").write_bytes(b"zip")
            (pending / "QueuedCase_20260401_1.nii.gz").write_bytes(b"nifti")
            (failed / "FailedCase_20260401_2.nii.gz").write_bytes(b"nifti")

            processed_case = studies / "ProcessedCase_20260401_3"
            (processed_case / "metadata").mkdir(parents=True, exist_ok=True)
            (processed_case / "logs").mkdir(parents=True, exist_ok=True)
            (processed_case / "metadata" / "id.json").write_text(
                json.dumps(
                    {
                        "CaseID": "ProcessedCase_20260401_3",
                        "PatientName": "Alice Example",
                        "AccessionNumber": "3",
                        "Modality": "CT",
                        "StudyDate": "20260401",
                        "Pipeline": {
                            "prepare_elapsed_time": "0:00:08.000000",
                            "segmentation_elapsed_time": "0:02:03.000000",
                            "elapsed_time": "0:02:11.000000",
                            "prepare_input_origin": "P",
                        },
                        "AvailableSeries": [{}, {}],
                        "DiscardedSeries": [{}],
                    }
                ),
                encoding="utf-8",
            )
            (processed_case / "metadata" / "resultados.json").write_text("{}", encoding="utf-8")

            failed_case = studies / "FailedCase_20260401_2"
            (failed_case / "metadata").mkdir(parents=True, exist_ok=True)
            (failed_case / "logs").mkdir(parents=True, exist_ok=True)
            (failed_case / "metadata" / "id.json").write_text(
                json.dumps(
                    {
                        "CaseID": "FailedCase_20260401_2",
                        "PatientName": "Bob Example",
                        "AccessionNumber": "2",
                        "Modality": "MR",
                        "StudyDate": "20260401",
                        "Pipeline": {
                            "prepare_elapsed_time": "0:00:04.000000",
                        },
                        "AvailableSeries": [{}],
                    }
                ),
                encoding="utf-8",
            )
            (failed_case / "logs" / "error.log").write_text("segmentation crashed", encoding="utf-8")

            db_path = base / "dicom.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE segmentation_queue (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        case_id TEXT NOT NULL UNIQUE,
                        input_path TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at TEXT,
                        claimed_at TEXT,
                        finished_at TEXT,
                        error TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE dicom_metadata (
                        StudyInstanceUID TEXT PRIMARY KEY,
                        PatientName TEXT,
                        ClinicalName TEXT,
                        AccessionNumber TEXT,
                        StudyDate TEXT,
                        Modality TEXT,
                        IdJson TEXT,
                        CalculationResults TEXT,
                        ProcessedAt TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE metrics_queue (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        case_id TEXT NOT NULL UNIQUE,
                        input_path TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at TEXT,
                        claimed_at TEXT,
                        finished_at TEXT,
                        error TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO segmentation_queue(case_id, input_path, status, created_at, error)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("QueuedCase_20260401_1", str(pending / "QueuedCase_20260401_1.nii.gz"), "pending", "2026-04-02 09:00:00", ""),
                )
                conn.execute(
                    """
                    INSERT INTO segmentation_queue(case_id, input_path, status, created_at, finished_at, error)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("FailedCase_20260401_2", str(failed / "FailedCase_20260401_2.nii.gz"), "error", "2026-04-02 08:00:00", "2026-04-02 08:10:00", "segmentation crashed"),
                )
                conn.execute(
                    """
                    INSERT INTO dicom_metadata(
                        StudyInstanceUID, PatientName, ClinicalName, AccessionNumber, StudyDate,
                        Modality, IdJson, CalculationResults, ProcessedAt
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "1.2.3",
                        "Alice Example",
                        "ProcessedCase_20260401_3",
                        "3",
                        "20260401",
                        "CT",
                        json.dumps({"CaseID": "ProcessedCase_20260401_3"}),
                        "{}",
                        "2026-04-02 08:30:00",
                    ),
                )
                conn.commit()

            layout = RuntimeLayout(
                runtime_dir=runtime,
                intake_dir=runtime / "intake",
                uploads_dir=uploads,
                uploads_failed_dir=uploads_failed,
                dicom_incoming_dir=dicom_incoming,
                dicom_failed_dir=dicom_failed,
                pending_dir=pending,
                active_dir=active,
                failed_dir=failed,
                studies_dir=studies,
            )

            snapshot = build_snapshot(layout=layout, db_path=db_path)

            self.assertEqual(snapshot.total_cases, 3)
            self.assertEqual(snapshot.processed_cases, 1)
            self.assertEqual(snapshot.failed_cases, 1)
            self.assertGreaterEqual(snapshot.backlog_cases, 1)
            self.assertEqual([stage.slug for stage in snapshot.stages], ["intake", "prepare", "segmentation", "metrics"])
            self.assertEqual(snapshot.stages[0].queued, 1)
            self.assertEqual(snapshot.stages[2].failed, 1)
            self.assertTrue(any(case.case_id == "ProcessedCase_20260401_3" and case.stage_key == "processed" for case in snapshot.cases))
            self.assertTrue(any(case.case_id == "FailedCase_20260401_2" and case.stage_key == "failed" for case in snapshot.cases))
            self.assertTrue(any(alert.level == "warning" for alert in snapshot.alerts))
            processed = next(item for item in snapshot.cases if item.case_id == "ProcessedCase_20260401_3")
            self.assertEqual(processed.origin, "")

    def test_build_snapshot_computes_live_segmentation_elapsed_from_start_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            runtime = base / "runtime"
            uploads = runtime / "intake" / "uploads"
            uploads_failed = runtime / "intake" / "uploads_failed"
            dicom_incoming = runtime / "intake" / "dicom" / "incoming"
            dicom_failed = runtime / "intake" / "dicom" / "failed"
            pending = runtime / "queue" / "pending"
            active = runtime / "queue" / "active"
            failed = runtime / "queue" / "failed"
            studies = runtime / "studies"
            for path in (uploads, uploads_failed, dicom_incoming, dicom_failed, pending, active, failed, studies):
                path.mkdir(parents=True, exist_ok=True)
            (active / "LiveCase_20260401_9.nii.gz").write_bytes(b"nifti")

            case_dir = studies / "LiveCase_20260401_9"
            (case_dir / "metadata").mkdir(parents=True, exist_ok=True)
            local_tz = ZoneInfo(settings.TIMEZONE)
            prepare_start = datetime.now(local_tz) - timedelta(minutes=3, seconds=5)
            prepare_end = prepare_start + timedelta(seconds=40)
            segmentation_start = prepare_end
            (case_dir / "metadata" / "id.json").write_text(
                json.dumps(
                    {
                        "CaseID": "LiveCase_20260401_9",
                        "PatientName": "Carol Example",
                        "AccessionNumber": "9",
                        "Modality": "CT",
                        "StudyDate": "20260401",
                        "Pipeline": {
                            "prepare_start_time": prepare_start.isoformat(),
                            "prepare_end_time": prepare_end.isoformat(),
                            "prepare_elapsed_time": "0:00:40.000000",
                            "start_time": segmentation_start.isoformat(),
                            "segmentation_start_time": segmentation_start.isoformat(),
                            "prepare_input_origin": "E",
                        },
                        "AvailableSeries": [{}],
                    }
                ),
                encoding="utf-8",
            )

            layout = RuntimeLayout(
                runtime_dir=runtime,
                intake_dir=runtime / "intake",
                uploads_dir=uploads,
                uploads_failed_dir=uploads_failed,
                dicom_incoming_dir=dicom_incoming,
                dicom_failed_dir=dicom_failed,
                pending_dir=pending,
                active_dir=active,
                failed_dir=failed,
                studies_dir=studies,
            )

            snapshot = build_snapshot(layout=layout, db_path=base / "missing.db")
            case = next(item for item in snapshot.cases if item.case_id == "LiveCase_20260401_9")

            self.assertEqual(case.stage_key, "segmentation")
            self.assertEqual(case.origin, "E")
            self.assertEqual(case.prepare_elapsed, "0:00:40")
            self.assertNotEqual(case.segmentation_elapsed, "-")
            self.assertRegex(case.segmentation_elapsed, r"^\d+:\d{2}:\d{2}$")
            self.assertNotEqual(case.total_elapsed, "-")
            self.assertRegex(case.total_elapsed, r"^\d+:\d{2}:\d{2}$")

    def test_build_snapshot_uses_fixed_elapsed_for_failed_stages_with_end_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            runtime = base / "runtime"
            uploads = runtime / "intake" / "uploads"
            uploads_failed = runtime / "intake" / "uploads_failed"
            dicom_incoming = runtime / "intake" / "dicom" / "incoming"
            dicom_failed = runtime / "intake" / "dicom" / "failed"
            pending = runtime / "queue" / "pending"
            active = runtime / "queue" / "active"
            failed = runtime / "queue" / "failed"
            studies = runtime / "studies"
            for path in (uploads, uploads_failed, dicom_incoming, dicom_failed, pending, active, failed, studies):
                path.mkdir(parents=True, exist_ok=True)

            seg_case = studies / "SegError_20260410_1"
            (seg_case / "metadata").mkdir(parents=True, exist_ok=True)
            (seg_case / "logs").mkdir(parents=True, exist_ok=True)
            (seg_case / "metadata" / "id.json").write_text(
                json.dumps(
                    {
                        "CaseID": "SegError_20260410_1",
                        "PatientName": "Seg Example",
                        "Pipeline": {
                            "segmentation_start_time": "2026-04-10T17:00:00-03:00",
                            "segmentation_end_time": "2026-04-10T17:01:15-03:00",
                            "segmentation_elapsed_time": "0:01:15",
                            "segmentation_status": "error",
                        },
                    }
                ),
                encoding="utf-8",
            )
            (seg_case / "logs" / "error.log").write_text("segmentation failed", encoding="utf-8")

            metrics_case = studies / "MetricsError_20260410_2"
            (metrics_case / "metadata").mkdir(parents=True, exist_ok=True)
            (metrics_case / "logs").mkdir(parents=True, exist_ok=True)
            (metrics_case / "metadata" / "id.json").write_text(
                json.dumps(
                    {
                        "CaseID": "MetricsError_20260410_2",
                        "PatientName": "Metrics Example",
                        "Pipeline": {
                            "segmentation_start_time": "2026-04-10T16:55:00-03:00",
                            "segmentation_end_time": "2026-04-10T16:56:00-03:00",
                            "segmentation_elapsed_time": "0:01:00",
                            "metrics_start_time": "2026-04-10T17:00:00-03:00",
                            "metrics_end_time": "2026-04-10T17:01:15-03:00",
                            "metrics_elapsed_time": "0:01:15",
                            "metrics_status": "error",
                        },
                    }
                ),
                encoding="utf-8",
            )
            (metrics_case / "logs" / "error.log").write_text("metrics failed", encoding="utf-8")

            db_path = base / "dicom.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE segmentation_queue (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        case_id TEXT NOT NULL UNIQUE,
                        input_path TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at TEXT,
                        claimed_at TEXT,
                        finished_at TEXT,
                        error TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE metrics_queue (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        case_id TEXT NOT NULL UNIQUE,
                        input_path TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at TEXT,
                        claimed_at TEXT,
                        finished_at TEXT,
                        error TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO segmentation_queue(case_id, input_path, status, created_at, finished_at, error)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("SegError_20260410_1", str(active / "SegError_20260410_1.nii.gz"), "error", "2026-04-10 17:00:00", "2026-04-10 17:01:15", "segmentation failed"),
                )
                conn.execute(
                    """
                    INSERT INTO segmentation_queue(case_id, input_path, status, created_at, finished_at, error)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("MetricsError_20260410_2", str(active / "MetricsError_20260410_2.nii.gz"), "done", "2026-04-10 16:55:00", "2026-04-10 16:56:00", ""),
                )
                conn.execute(
                    """
                    INSERT INTO metrics_queue(case_id, input_path, status, created_at, finished_at, error)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("MetricsError_20260410_2", str(active / "MetricsError_20260410_2.nii.gz"), "error", "2026-04-10 17:00:00", "2026-04-10 17:01:15", "metrics failed"),
                )
                conn.commit()

            layout = RuntimeLayout(
                runtime_dir=runtime,
                intake_dir=runtime / "intake",
                uploads_dir=uploads,
                uploads_failed_dir=uploads_failed,
                dicom_incoming_dir=dicom_incoming,
                dicom_failed_dir=dicom_failed,
                pending_dir=pending,
                active_dir=active,
                failed_dir=failed,
                studies_dir=studies,
            )

            snapshot = build_snapshot(layout=layout, db_path=db_path)
            seg_item = next(item for item in snapshot.cases if item.case_id == "SegError_20260410_1")
            metrics_item = next(item for item in snapshot.cases if item.case_id == "MetricsError_20260410_2")

            self.assertEqual(seg_item.stage_key, "failed")
            self.assertEqual(seg_item.segmentation_elapsed, "0:01:15")
            self.assertEqual(metrics_item.stage_key, "failed")
            self.assertEqual(metrics_item.metrics_elapsed, "0:01:15")

    def test_build_snapshot_treats_retained_intake_failures_as_notes_not_alerts(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            runtime = base / "runtime"
            uploads = runtime / "intake" / "uploads"
            uploads_failed = runtime / "intake" / "uploads_failed"
            dicom_incoming = runtime / "intake" / "dicom" / "incoming"
            dicom_failed = runtime / "intake" / "dicom" / "failed"
            pending = runtime / "queue" / "pending"
            active = runtime / "queue" / "active"
            failed = runtime / "queue" / "failed"
            studies = runtime / "studies"
            for path in (uploads, uploads_failed, dicom_incoming, dicom_failed, pending, active, failed, studies):
                path.mkdir(parents=True, exist_ok=True)

            (uploads_failed / "old_case.zip").write_bytes(b"zip")
            (dicom_failed / "old_study").mkdir(parents=True, exist_ok=True)

            layout = RuntimeLayout(
                runtime_dir=runtime,
                intake_dir=runtime / "intake",
                uploads_dir=uploads,
                uploads_failed_dir=uploads_failed,
                dicom_incoming_dir=dicom_incoming,
                dicom_failed_dir=dicom_failed,
                pending_dir=pending,
                active_dir=active,
                failed_dir=failed,
                studies_dir=studies,
            )

            snapshot = build_snapshot(layout=layout, db_path=base / "missing.db")
            intake_stage = next(stage for stage in snapshot.stages if stage.slug == "intake")
            prepare_stage = next(stage for stage in snapshot.stages if stage.slug == "prepare")

            self.assertEqual(intake_stage.state, "flow")
            self.assertEqual(intake_stage.failed, 0)
            self.assertIn("2 intake failures retained", intake_stage.notes)
            self.assertEqual(prepare_stage.state, "flow")
            self.assertEqual(prepare_stage.failed, 0)
            self.assertEqual(len(snapshot.alerts), 1)
            self.assertEqual(snapshot.alerts[0].level, "ok")

    def test_build_snapshot_shows_intake_idle_window_notes(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            runtime = base / "runtime"
            uploads = runtime / "intake" / "uploads"
            uploads_failed = runtime / "intake" / "uploads_failed"
            dicom_incoming = runtime / "intake" / "dicom" / "incoming"
            dicom_failed = runtime / "intake" / "dicom" / "failed"
            pending = runtime / "queue" / "pending"
            active = runtime / "queue" / "active"
            failed = runtime / "queue" / "failed"
            studies = runtime / "studies"
            for path in (uploads, uploads_failed, dicom_incoming, dicom_failed, pending, active, failed, studies):
                path.mkdir(parents=True, exist_ok=True)

            incoming_file = dicom_incoming / "study_partial"
            incoming_file.write_text("partial", encoding="utf-8")
            stale_ts = datetime.now(ZoneInfo(settings.TIMEZONE)).timestamp() - 120
            os.utime(incoming_file, (stale_ts, stale_ts))

            layout = RuntimeLayout(
                runtime_dir=runtime,
                intake_dir=runtime / "intake",
                uploads_dir=uploads,
                uploads_failed_dir=uploads_failed,
                dicom_incoming_dir=dicom_incoming,
                dicom_failed_dir=dicom_failed,
                pending_dir=pending,
                active_dir=active,
                failed_dir=failed,
                studies_dir=studies,
            )

            with patch.object(settings, "DICOM_IDLE_SECONDS", 600):
                with patch(
                    "heimdallr.tui.snapshot._scan_system_processes",
                    return_value={
                        "intake": [{"pid": "111", "etime": "1:23", "command": "python -m heimdallr.intake"}],
                        "prepare": [],
                        "segmentation": [],
                        "metrics": [],
                        "space_manager": [{"pid": "222", "etime": "2:34", "command": "python -m heimdallr.space_manager"}],
                    },
                ):
                    with patch("heimdallr.tui.snapshot._disk_free_bytes", return_value=64 * 1024**3):
                        snapshot = build_snapshot(layout=layout, db_path=base / "missing.db")

            intake_stage = next(stage for stage in snapshot.stages if stage.slug == "intake")
            intake_service = next(service for service in snapshot.services if service.slug == "intake")
            space_service = next(service for service in snapshot.services if service.slug == "space_manager")
            self.assertIn("handoff after 10m without new images", intake_stage.notes)
            self.assertTrue(
                any(
                    note.startswith("estimated quiet-window remaining:")
                    for note in intake_stage.notes
                )
            )
            self.assertTrue(intake_service.details[0].startswith("PID 111 • up 1:23 • last image 2m ago"))
            self.assertRegex(intake_service.details[0], r"handoff in [78]m$")
            self.assertEqual(
                space_service.details[0],
                "PID 222 • up 2:34 • free 64.0GB",
            )


if __name__ == "__main__":
    unittest.main()
