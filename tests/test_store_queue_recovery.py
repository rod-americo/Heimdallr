import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from heimdallr.shared import store


def _connect_row_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


class TestStoreQueueRecovery(unittest.TestCase):
    def test_register_study_handoff_suppresses_prepared_duplicate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = _connect_row_db(db_path)
            store.ensure_schema(conn)

            accepted, row = store.register_study_handoff(
                conn,
                study_uid="1.2.3",
                manifest_digest="abc",
                instance_count=100,
                calling_aet="SRC",
                remote_ip="10.0.0.1",
            )
            self.assertTrue(accepted)
            self.assertEqual(row["status"], "pending_prepare")

            store.update_study_handoff_state(
                conn,
                study_uid="1.2.3",
                manifest_digest="abc",
                case_id="CaseA",
                status="prepared",
                last_error=None,
            )

            accepted, row = store.register_study_handoff(
                conn,
                study_uid="1.2.3",
                manifest_digest="abc",
                instance_count=100,
                calling_aet="SRC",
                remote_ip="10.0.0.1",
            )
            self.assertFalse(accepted)
            self.assertEqual(row["status"], "prepared")
            self.assertEqual(row["duplicate_count"], 1)
            conn.close()

    def test_register_study_handoff_allows_retry_after_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = _connect_row_db(db_path)
            store.ensure_schema(conn)

            store.register_study_handoff(
                conn,
                study_uid="1.2.3",
                manifest_digest="abc",
                instance_count=100,
                calling_aet="SRC",
                remote_ip="10.0.0.1",
            )
            store.update_study_handoff_state(
                conn,
                study_uid="1.2.3",
                manifest_digest="abc",
                case_id="CaseA",
                status="error",
                last_error="prepare failed",
            )

            accepted, row = store.register_study_handoff(
                conn,
                study_uid="1.2.3",
                manifest_digest="abc",
                instance_count=101,
                calling_aet="SRC",
                remote_ip="10.0.0.1",
            )
            self.assertTrue(accepted)
            self.assertEqual(row["status"], "pending_prepare")
            self.assertEqual(row["instance_count"], 101)
            self.assertIsNone(row["last_error"])
            conn.close()

    def test_enqueue_segmentation_case_preserves_fresh_claimed_row(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = _connect_row_db(db_path)
            store.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO segmentation_queue (case_id, input_path, status, created_at, claimed_at)
                VALUES (?, ?, 'claimed', '2026-04-12 10:00:00', '2099-04-12 10:00:05')
                """,
                ("CaseFresh", "/tmp/original"),
            )

            with patch("heimdallr.shared.store.settings.SEGMENTATION_CLAIM_TTL_SECONDS", 900):
                store.enqueue_segmentation_case(conn, "CaseFresh", "/tmp/new-path")

            row = conn.execute(
                "SELECT status, input_path, claimed_at FROM segmentation_queue WHERE case_id = ?",
                ("CaseFresh",),
            ).fetchone()
            self.assertEqual(row["status"], "claimed")
            self.assertEqual(row["input_path"], "/tmp/new-path")
            self.assertEqual(row["claimed_at"], "2099-04-12 10:00:05")
            conn.close()

    def test_enqueue_case_for_metrics_preserves_fresh_claimed_row(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = _connect_row_db(db_path)
            store.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO metrics_queue (case_id, input_path, status, created_at, claimed_at)
                VALUES (?, ?, 'claimed', '2026-04-12 10:00:00', '2099-04-12 10:00:05')
                """,
                ("CaseFreshMetrics", "/tmp/original"),
            )

            with patch("heimdallr.shared.store.settings.METRICS_CLAIM_TTL_SECONDS", 900):
                store.enqueue_case_for_metrics(conn, "CaseFreshMetrics", "/tmp/new-path")

            row = conn.execute(
                "SELECT status, input_path, claimed_at FROM metrics_queue WHERE case_id = ?",
                ("CaseFreshMetrics",),
            ).fetchone()
            self.assertEqual(row["status"], "claimed")
            self.assertEqual(row["input_path"], "/tmp/new-path")
            self.assertEqual(row["claimed_at"], "2099-04-12 10:00:05")
            conn.close()

    def test_enqueue_segmentation_case_clears_downstream_non_done_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = _connect_row_db(db_path)
            store.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO metrics_queue (case_id, input_path, status, created_at, error)
                VALUES (?, ?, 'error', '2026-04-12 10:00:00', 'old metrics error')
                """,
                ("CaseA", "/tmp/case-a",),
            )
            conn.execute(
                """
                INSERT INTO dicom_egress_queue (
                    case_id, study_uid, artifact_path, artifact_type, destination_name,
                    destination_host, destination_port, destination_called_aet,
                    source_calling_aet, source_remote_ip, artifact_digest,
                    status, attempts, created_at, claimed_at, finished_at, next_attempt_at, error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'error', 1, '2026-04-12 10:00:00', NULL, NULL, NULL, 'old egress error')
                """,
                ("CaseA", "1.2.3", "artifact.dcm", "secondary_capture", "PACS", "127.0.0.1", 104, "PACS", None, None, "abc"),
            )
            conn.execute(
                """
                INSERT INTO dicom_egress_queue (
                    case_id, study_uid, artifact_path, artifact_type, destination_name,
                    destination_host, destination_port, destination_called_aet,
                    source_calling_aet, source_remote_ip, artifact_digest,
                    status, attempts, created_at, claimed_at, finished_at, next_attempt_at, error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'done', 1, '2026-04-12 09:00:00', '2026-04-12 09:00:01', '2026-04-12 09:00:02', NULL, NULL)
                """,
                ("CaseA", "1.2.3", "artifact-done.dcm", "secondary_capture", "PACS", "127.0.0.1", 104, "PACS", None, None, "done"),
            )

            store.enqueue_segmentation_case(conn, "CaseA", "/tmp/case-a")

            seg_row = conn.execute(
                "SELECT status, input_path FROM segmentation_queue WHERE case_id = ?",
                ("CaseA",),
            ).fetchone()
            self.assertEqual(seg_row["status"], "pending")
            self.assertEqual(seg_row["input_path"], "/tmp/case-a")
            self.assertIsNone(
                conn.execute("SELECT 1 FROM metrics_queue WHERE case_id = ?", ("CaseA",)).fetchone()
            )
            egress_rows = conn.execute(
                "SELECT artifact_path, status FROM dicom_egress_queue WHERE case_id = ? ORDER BY artifact_path",
                ("CaseA",),
            ).fetchall()
            self.assertEqual([(row["artifact_path"], row["status"]) for row in egress_rows], [("artifact-done.dcm", "done")])
            conn.close()

    def test_reset_claimed_queue_items_moves_rows_back_to_pending(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = _connect_row_db(db_path)
            store.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO segmentation_queue (case_id, input_path, status, created_at, claimed_at)
                VALUES (?, ?, 'claimed', '2026-04-12 10:00:00', '2026-04-12 10:00:05')
                """,
                ("CaseSeg", "/tmp/seg"),
            )
            conn.execute(
                """
                INSERT INTO metrics_queue (case_id, input_path, status, created_at, claimed_at)
                VALUES (?, ?, 'claimed', '2026-04-12 10:00:00', '2026-04-12 10:00:05')
                """,
                ("CaseMet", "/tmp/met"),
            )

            self.assertEqual(store.reset_claimed_segmentation_queue_items(conn), 1)
            self.assertEqual(store.reset_claimed_metrics_queue_items(conn), 1)

            seg_row = conn.execute(
                "SELECT status, claimed_at, finished_at, error FROM segmentation_queue WHERE case_id = ?",
                ("CaseSeg",),
            ).fetchone()
            met_row = conn.execute(
                "SELECT status, claimed_at, finished_at, error FROM metrics_queue WHERE case_id = ?",
                ("CaseMet",),
            ).fetchone()
            self.assertEqual(seg_row["status"], "pending")
            self.assertIsNone(seg_row["claimed_at"])
            self.assertIsNone(seg_row["finished_at"])
            self.assertIsNone(seg_row["error"])
            self.assertEqual(met_row["status"], "pending")
            self.assertIsNone(met_row["claimed_at"])
            self.assertIsNone(met_row["finished_at"])
            self.assertIsNone(met_row["error"])
            conn.close()

    def test_claim_next_pending_segmentation_queue_item_reclaims_stale_claim(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = _connect_row_db(db_path)
            store.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO segmentation_queue (case_id, input_path, status, created_at, claimed_at)
                VALUES (?, ?, 'claimed', '2026-04-12 10:00:00', '2026-04-12 10:00:05')
                """,
                ("CaseSeg", "/tmp/seg"),
            )

            with patch("heimdallr.shared.store.settings.SEGMENTATION_CLAIM_TTL_SECONDS", 1):
                claimed = store.claim_next_pending_segmentation_queue_item(conn)

            self.assertIsNotNone(claimed)
            self.assertEqual(claimed[1], "CaseSeg")
            row = conn.execute(
                "SELECT status, claimed_at, error FROM segmentation_queue WHERE case_id = ?",
                ("CaseSeg",),
            ).fetchone()
            self.assertEqual(row["status"], "claimed")
            self.assertIsNotNone(row["claimed_at"])
            self.assertIsNone(row["error"])
            conn.close()

    def test_claim_next_pending_metrics_queue_item_reclaims_stale_claim(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = _connect_row_db(db_path)
            store.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO metrics_queue (case_id, input_path, status, created_at, claimed_at)
                VALUES (?, ?, 'claimed', '2026-04-12 10:00:00', '2026-04-12 10:00:05')
                """,
                ("CaseMet", "/tmp/met"),
            )

            with patch("heimdallr.shared.store.settings.METRICS_CLAIM_TTL_SECONDS", 1):
                claimed = store.claim_next_pending_metrics_queue_item(conn)

            self.assertIsNotNone(claimed)
            self.assertEqual(claimed[1], "CaseMet")
            row = conn.execute(
                "SELECT status, claimed_at, error FROM metrics_queue WHERE case_id = ?",
                ("CaseMet",),
            ).fetchone()
            self.assertEqual(row["status"], "claimed")
            self.assertIsNotNone(row["claimed_at"])
            self.assertIsNone(row["error"])
            conn.close()


if __name__ == "__main__":
    unittest.main()
