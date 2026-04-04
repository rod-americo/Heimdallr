import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from heimdallr.shared import settings
from heimdallr.tui.snapshot import RuntimeLayout, build_snapshot


class TestTuiSnapshot(unittest.TestCase):
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
            self.assertEqual(case.prepare_elapsed, "0:00:40")
            self.assertNotEqual(case.segmentation_elapsed, "-")
            self.assertRegex(case.segmentation_elapsed, r"^\d+:\d{2}:\d{2}$")
            self.assertNotEqual(case.total_elapsed, "-")
            self.assertRegex(case.total_elapsed, r"^\d+:\d{2}:\d{2}$")


if __name__ == "__main__":
    unittest.main()
