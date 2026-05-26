import unittest
from io import StringIO
from datetime import datetime
from pathlib import Path
import sqlite3
import tempfile
from zoneinfo import ZoneInfo

from rich.console import Console

from heimdallr.tui.simple import (
    SimpleQueueTui,
    _backlog_cases,
    _key_digit,
    _pipeline_elapsed,
    _processed_cases,
    cancel_case_from_pipeline,
    prioritize_case_in_pipeline,
)
from heimdallr.shared import store
from heimdallr.tui.snapshot import CaseOverview, DashboardSnapshot, StageMetrics


LOCAL_TZ = ZoneInfo("America/Sao_Paulo")


def case_overview(
    case_id: str,
    *,
    stage_key: str,
    queue_status_key: str = "",
    accession_number: str = "",
    sort_timestamp: float = 1.0,
) -> CaseOverview:
    return CaseOverview(
        case_id=case_id,
        patient_name="Visible Patient Name",
        origin="api",
        modality="CT",
        accession_number=accession_number,
        study_date="20260525",
        stage_key=stage_key,
        stage_label=stage_key,
        queue_status_key=queue_status_key,
        queue_status=queue_status_key,
        signal="ready",
        updated_at=datetime(2026, 5, 25, 12, 0, tzinfo=LOCAL_TZ),
        prepare_elapsed="0:00:01",
        segmentation_elapsed="0:00:02",
        metrics_elapsed="0:00:03",
        total_elapsed="0:00:06",
        selected_series=1,
        discarded_series=0,
        path=Path("runtime/studies") / case_id,
        error="",
        segmentation_status="",
        sort_timestamp=sort_timestamp,
    )


def snapshot() -> DashboardSnapshot:
    return DashboardSnapshot(
        generated_at=datetime(2026, 5, 25, 12, 0, tzinfo=LOCAL_TZ),
        services=[],
        stages=[
            StageMetrics("segmentation", "Segmentation", "flow", 1, 0, 1, 0, None),
            StageMetrics("metrics", "Metrics", "flow", 0, 1, 1, 0, None),
        ],
        cases=[
            case_overview("QueuedCase_20260525_1", stage_key="queued", queue_status_key="pending", accession_number="123"),
            case_overview("ProcessedCase_20260525_2", stage_key="processed", queue_status_key="done", accession_number="456"),
        ],
        alerts=[],
        total_cases=2,
        processed_cases=1,
        backlog_cases=1,
        failed_cases=0,
        avg_prepare_seconds=None,
        avg_segmentation_seconds=None,
        avg_metrics_seconds=None,
    )


class TestSimpleQueueTui(unittest.TestCase):
    def test_case_groups_separate_backlog_from_processed(self):
        current = snapshot()

        self.assertEqual([case.case_id for case in _backlog_cases(current)], ["QueuedCase_20260525_1"])
        self.assertEqual([case.case_id for case in _processed_cases(current)], ["ProcessedCase_20260525_2"])

    def test_case_groups_are_newest_first(self):
        current = DashboardSnapshot(
            generated_at=datetime(2026, 5, 25, 12, 0, tzinfo=LOCAL_TZ),
            services=[],
            stages=[],
            cases=[
                case_overview("ProcessedOld", stage_key="processed", sort_timestamp=10.0),
                case_overview("ProcessedNew", stage_key="processed", sort_timestamp=30.0),
                case_overview("QueuedOld", stage_key="queued", queue_status_key="pending", sort_timestamp=20.0),
                case_overview("QueuedNew", stage_key="queued", queue_status_key="pending", sort_timestamp=40.0),
            ],
            alerts=[],
            total_cases=4,
            processed_cases=2,
            backlog_cases=2,
            failed_cases=0,
            avg_prepare_seconds=None,
            avg_segmentation_seconds=None,
            avg_metrics_seconds=None,
        )

        self.assertEqual([case.case_id for case in _processed_cases(current)], ["ProcessedNew", "ProcessedOld"])
        self.assertEqual([case.case_id for case in _backlog_cases(current)], ["QueuedNew", "QueuedOld"])

    def test_ineligible_cases_do_not_appear_in_backlog(self):
        current = DashboardSnapshot(
            generated_at=datetime(2026, 5, 25, 12, 0, tzinfo=LOCAL_TZ),
            services=[],
            stages=[],
            cases=[
                case_overview("QueuedCase", stage_key="queued", queue_status_key="pending"),
                case_overview("IneligibleCase", stage_key="ineligible"),
            ],
            alerts=[],
            total_cases=2,
            processed_cases=0,
            backlog_cases=1,
            failed_cases=0,
            avg_prepare_seconds=None,
            avg_segmentation_seconds=None,
            avg_metrics_seconds=None,
        )

        self.assertEqual([case.case_id for case in _backlog_cases(current)], ["QueuedCase"])

    def test_prepare_cases_appear_in_backlog(self):
        current = DashboardSnapshot(
            generated_at=datetime(2026, 5, 25, 12, 0, tzinfo=LOCAL_TZ),
            services=[],
            stages=[],
            cases=[
                case_overview("PrepareCase", stage_key="prepare"),
            ],
            alerts=[],
            total_cases=1,
            processed_cases=0,
            backlog_cases=1,
            failed_cases=0,
            avg_prepare_seconds=None,
            avg_segmentation_seconds=None,
            avg_metrics_seconds=None,
        )

        self.assertEqual([case.case_id for case in _backlog_cases(current)], ["PrepareCase"])

    def test_render_uses_accessions_without_patient_names(self):
        app = SimpleQueueTui(limit=10)
        current = snapshot()
        app.load = lambda: current  # type: ignore[method-assign]

        console = Console(file=StringIO(), record=True, width=180, height=50)
        console.print(app.render())
        rendered = console.export_text()

        self.assertIn("01", rendered)
        self.assertIn("123", rendered)
        self.assertIn("456", rendered)
        self.assertIn("Pipeline", rendered)
        self.assertNotIn("Visible Patient Name", rendered)

    def test_cancel_case_from_pipeline_marks_pending_items_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                store.ensure_schema(conn)
                conn.execute(
                    """
                    INSERT INTO segmentation_queue (case_id, input_path, status, created_at)
                    VALUES ('CaseA', '/tmp/case-a.nii.gz', 'pending', '2026-05-25 12:00:00')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO metrics_queue (case_id, input_path, status, created_at)
                    VALUES ('CaseA', '/tmp/case-a.nii.gz', 'claimed', '2026-05-25 12:00:00')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO dicom_egress_queue (
                        case_id,
                        artifact_path,
                        artifact_type,
                        destination_name,
                        destination_host,
                        destination_port,
                        destination_called_aet,
                        status,
                        created_at
                    )
                    VALUES (
                        'CaseA',
                        '/tmp/artifact.dcm',
                        'secondary_capture',
                        'local',
                        '127.0.0.1',
                        104,
                        'OSIRIX',
                        'pending',
                        '2026-05-25 12:00:00'
                    )
                    """
                )
                conn.commit()
            finally:
                conn.close()

            self.assertEqual(cancel_case_from_pipeline(db_path, "CaseA"), 3)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                for table in ("segmentation_queue", "metrics_queue", "dicom_egress_queue"):
                    row = conn.execute(f"SELECT status, error FROM {table} WHERE case_id = 'CaseA'").fetchone()
                    self.assertEqual(row["status"], "error")
                    self.assertEqual(row["error"], store.PIPELINE_CANCEL_MESSAGE)
            finally:
                conn.close()

    def test_cancel_case_from_pipeline_marks_prepared_case_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"

            self.assertEqual(cancel_case_from_pipeline(db_path, "PreparedCase"), 1)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                row = conn.execute(
                    "SELECT status, input_path, error FROM segmentation_queue WHERE case_id = 'PreparedCase'"
                ).fetchone()
                self.assertEqual(row["status"], "error")
                self.assertTrue(str(row["input_path"]).endswith("PreparedCase"))
                self.assertEqual(row["error"], store.PIPELINE_CANCEL_MESSAGE)
            finally:
                conn.close()

    def test_prioritize_case_moves_pending_queue_item_next(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                store.ensure_schema(conn)
                conn.execute(
                    """
                    INSERT INTO segmentation_queue (case_id, input_path, status, created_at)
                    VALUES ('CaseA', '/tmp/case-a.nii.gz', 'pending', '2026-05-25 12:00:00')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO segmentation_queue (case_id, input_path, status, created_at)
                    VALUES ('CaseB', '/tmp/case-b.nii.gz', 'pending', '2026-05-25 12:10:00')
                    """
                )
                conn.commit()
            finally:
                conn.close()

    def test_prioritize_case_creates_pending_row_for_prepared_case(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"

            self.assertEqual(prioritize_case_in_pipeline(db_path, "PreparedCase"), 1)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                claimed = store.claim_next_pending_segmentation_queue_item(conn)
                self.assertIsNotNone(claimed)
                self.assertEqual(claimed[1], "PreparedCase")
                self.assertTrue(str(claimed[2]).endswith("PreparedCase"))
            finally:
                conn.close()

    def test_prioritize_case_does_not_requeue_claimed_item(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                store.ensure_schema(conn)
                conn.execute(
                    """
                    INSERT INTO segmentation_queue (case_id, input_path, status, created_at, claimed_at)
                    VALUES (
                        'ActiveCase',
                        '/tmp/active-case',
                        'claimed',
                        '2026-05-25 12:00:00',
                        '2026-05-25 12:01:00'
                    )
                    """
                )
                conn.commit()
            finally:
                conn.close()

            self.assertEqual(prioritize_case_in_pipeline(db_path, "ActiveCase"), 0)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                row = conn.execute("SELECT status FROM segmentation_queue WHERE case_id = 'ActiveCase'").fetchone()
                self.assertEqual(row["status"], "claimed")
            finally:
                conn.close()

            self.assertEqual(prioritize_case_in_pipeline(db_path, "CaseB"), 1)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                claimed = store.claim_next_pending_segmentation_queue_item(conn)
                self.assertIsNotNone(claimed)
                self.assertEqual(claimed[1], "CaseB")
            finally:
                conn.close()

    def test_key_digit_accepts_named_digit_keys(self):
        self.assertEqual(_key_digit("1", ""), "1")
        self.assertEqual(_key_digit("digit1", ""), "1")
        self.assertEqual(_key_digit("number10", ""), "10")
        self.assertEqual(_key_digit("a", "4"), "4")
        self.assertIsNone(_key_digit("a", ""))

    def test_pipeline_elapsed_sums_active_stage_durations(self):
        case = case_overview("CaseA", stage_key="processed")
        case.prepare_elapsed = "0:01:00"
        case.segmentation_elapsed = "0:02:30"
        case.metrics_elapsed = "0:00:15"

        self.assertEqual(_pipeline_elapsed(case), "0:03:45")


if __name__ == "__main__":
    unittest.main()
