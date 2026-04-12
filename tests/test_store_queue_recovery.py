import sqlite3
import tempfile
import unittest
from pathlib import Path

from heimdallr.shared import store


def _connect_row_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


class TestStoreQueueRecovery(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
