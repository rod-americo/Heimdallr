import json
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
                INSERT INTO segmentation_queue (case_id, input_path, status, created_at, claimed_at, claim_heartbeat_at)
                VALUES (?, ?, 'claimed', '2026-04-12 10:00:00', '2026-04-12 10:00:05', '2099-04-12 10:10:05')
                """,
                ("CaseFresh", "/tmp/original"),
            )

            with patch("heimdallr.shared.store.settings.SEGMENTATION_CLAIM_TTL_SECONDS", 900):
                store.enqueue_segmentation_case(conn, "CaseFresh", "/tmp/new-path")

            row = conn.execute(
                "SELECT status, input_path, claimed_at, claim_heartbeat_at FROM segmentation_queue WHERE case_id = ?",
                ("CaseFresh",),
            ).fetchone()
            self.assertEqual(row["status"], "claimed")
            self.assertEqual(row["input_path"], "/tmp/new-path")
            self.assertEqual(row["claimed_at"], "2026-04-12 10:00:05")
            self.assertEqual(row["claim_heartbeat_at"], "2099-04-12 10:10:05")
            conn.close()

    def test_enqueue_case_for_metrics_preserves_fresh_claimed_row(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = _connect_row_db(db_path)
            store.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO metrics_queue (case_id, input_path, status, created_at, claimed_at, claim_heartbeat_at)
                VALUES (?, ?, 'claimed', '2026-04-12 10:00:00', '2026-04-12 10:00:05', '2099-04-12 10:10:05')
                """,
                ("CaseFreshMetrics", "/tmp/original"),
            )

            with patch("heimdallr.shared.store.settings.METRICS_CLAIM_TTL_SECONDS", 900):
                store.enqueue_case_for_metrics(conn, "CaseFreshMetrics", "/tmp/new-path")

            row = conn.execute(
                "SELECT status, input_path, claimed_at, claim_heartbeat_at FROM metrics_queue WHERE case_id = ?",
                ("CaseFreshMetrics",),
            ).fetchone()
            self.assertEqual(row["status"], "claimed")
            self.assertEqual(row["input_path"], "/tmp/new-path")
            self.assertEqual(row["claimed_at"], "2026-04-12 10:00:05")
            self.assertEqual(row["claim_heartbeat_at"], "2099-04-12 10:10:05")
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
                INSERT INTO segmentation_queue (case_id, input_path, status, created_at, claimed_at, claim_heartbeat_at)
                VALUES (?, ?, 'claimed', '2026-04-12 10:00:00', '2026-04-12 10:00:05', '2026-04-12 10:00:06')
                """,
                ("CaseSeg", "/tmp/seg"),
            )
            conn.execute(
                """
                INSERT INTO metrics_queue (case_id, input_path, status, created_at, claimed_at, claim_heartbeat_at)
                VALUES (?, ?, 'claimed', '2026-04-12 10:00:00', '2026-04-12 10:00:05', '2026-04-12 10:00:06')
                """,
                ("CaseMet", "/tmp/met"),
            )

            self.assertEqual(store.reset_claimed_segmentation_queue_items(conn), 1)
            self.assertEqual(store.reset_claimed_metrics_queue_items(conn), 1)

            seg_row = conn.execute(
                "SELECT status, claimed_at, claim_heartbeat_at, finished_at, error FROM segmentation_queue WHERE case_id = ?",
                ("CaseSeg",),
            ).fetchone()
            met_row = conn.execute(
                "SELECT status, claimed_at, claim_heartbeat_at, finished_at, error FROM metrics_queue WHERE case_id = ?",
                ("CaseMet",),
            ).fetchone()
            self.assertEqual(seg_row["status"], "pending")
            self.assertIsNone(seg_row["claimed_at"])
            self.assertIsNone(seg_row["claim_heartbeat_at"])
            self.assertIsNone(seg_row["finished_at"])
            self.assertIsNone(seg_row["error"])
            self.assertEqual(met_row["status"], "pending")
            self.assertIsNone(met_row["claimed_at"])
            self.assertIsNone(met_row["claim_heartbeat_at"])
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
                INSERT INTO segmentation_queue (case_id, input_path, status, created_at, claimed_at, claim_heartbeat_at)
                VALUES (?, ?, 'claimed', '2026-04-12 10:00:00', '2099-04-12 10:00:00', '2026-04-12 10:00:05')
                """,
                ("CaseSeg", "/tmp/seg"),
            )

            with patch("heimdallr.shared.store.settings.SEGMENTATION_CLAIM_TTL_SECONDS", 1):
                claimed = store.claim_next_pending_segmentation_queue_item(conn)

            self.assertIsNotNone(claimed)
            self.assertEqual(claimed[1], "CaseSeg")
            row = conn.execute(
                "SELECT status, claimed_at, claim_heartbeat_at, error, attempts FROM segmentation_queue WHERE case_id = ?",
                ("CaseSeg",),
            ).fetchone()
            self.assertEqual(row["status"], "claimed")
            self.assertIsNotNone(row["claimed_at"])
            self.assertEqual(row["claimed_at"], row["claim_heartbeat_at"])
            self.assertIsNone(row["error"])
            self.assertEqual(row["attempts"], 1)
            conn.close()

    def test_touch_segmentation_queue_item_claim_updates_heartbeat_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = _connect_row_db(db_path)
            store.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO segmentation_queue (case_id, input_path, status, created_at, claimed_at, claim_heartbeat_at)
                VALUES (?, ?, 'claimed', '2026-04-12 10:00:00', '2026-04-12 10:00:05', '2026-04-12 10:00:05')
                """,
                ("CaseHeartbeat", "/tmp/seg"),
            )

            with patch("heimdallr.shared.store._now_local_timestamp", return_value="2026-04-12 10:03:00"):
                touched = store.touch_segmentation_queue_item_claim(conn, 1)

            self.assertTrue(touched)
            row = conn.execute(
                "SELECT claimed_at, claim_heartbeat_at FROM segmentation_queue WHERE case_id = ?",
                ("CaseHeartbeat",),
            ).fetchone()
            self.assertEqual(row["claimed_at"], "2026-04-12 10:00:05")
            self.assertEqual(row["claim_heartbeat_at"], "2026-04-12 10:03:00")
            conn.close()

    def test_retry_segmentation_queue_item_requeues_once_before_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = _connect_row_db(db_path)
            store.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO segmentation_queue (case_id, input_path, status, created_at, attempts)
                VALUES (?, ?, 'claimed', '2026-04-12 10:00:00', 1)
                """,
                ("CaseRetry", "/tmp/retry"),
            )

            requeued = store.retry_segmentation_queue_item(
                conn,
                1,
                "worker shutdown",
                max_attempts=2,
            )
            self.assertTrue(requeued)
            row = conn.execute(
                "SELECT status, claimed_at, finished_at, error, attempts FROM segmentation_queue WHERE case_id = ?",
                ("CaseRetry",),
            ).fetchone()
            self.assertEqual(row["status"], "pending")
            self.assertIsNone(row["claimed_at"])
            self.assertIsNone(row["finished_at"])
            self.assertEqual(row["error"], "worker shutdown")
            self.assertEqual(row["attempts"], 1)

            conn.execute(
                "UPDATE segmentation_queue SET status = 'claimed', attempts = 2 WHERE case_id = ?",
                ("CaseRetry",),
            )
            requeued = store.retry_segmentation_queue_item(
                conn,
                1,
                "worker shutdown",
                max_attempts=2,
            )
            self.assertFalse(requeued)
            conn.close()

    def test_update_calculation_results_materializes_bone_health_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = _connect_row_db(db_path)
            store.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO dicom_metadata (StudyInstanceUID, PatientName, AccessionNumber, StudyDate, Modality)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("1.2.3", "Alice Example", "123", "20260502", "CT"),
            )

            store.update_calculation_results(
                conn,
                "1.2.3",
                {
                    "metrics": {
                        "bone_health_l1_hu": {
                            "status": "done",
                            "measurement": {
                                "l1_trabecular_hu_mean": 142.5,
                                "classification": "osteopenia",
                                "qc": {"bone_health_qc_pass": True},
                            },
                        }
                    }
                },
            )

            row = conn.execute(
                """
                SELECT BoneHealthL1TrabecularHuMean, BoneHealthL1Classification, BoneHealthL1QcPass
                FROM dicom_metadata
                WHERE StudyInstanceUID = ?
                """,
                ("1.2.3",),
            ).fetchone()
            self.assertEqual(row["BoneHealthL1TrabecularHuMean"], 142.5)
            self.assertEqual(row["BoneHealthL1Classification"], "osteopenia")
            self.assertEqual(row["BoneHealthL1QcPass"], 1)
            conn.close()

    def test_backfill_materialized_calculation_results_populates_legacy_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            conn = _connect_row_db(db_path)
            store.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO dicom_metadata (
                    StudyInstanceUID, PatientName, AccessionNumber, StudyDate, Modality, CalculationResults
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "1.2.4",
                    "Bob Example",
                    "124",
                    "20260502",
                    "CT",
                    json.dumps(
                        {
                            "metrics": {
                                "bone_health_l1_hu": {
                                    "status": "done",
                                    "measurement": {
                                        "l1_trabecular_hu_mean": 88.0,
                                        "classification": "osteoporosis",
                                        "qc": {"bone_health_qc_pass": False},
                                    },
                                }
                            }
                        }
                    ),
                ),
            )

            updated = store.backfill_materialized_calculation_results(conn)
            self.assertEqual(updated, 1)
            row = conn.execute(
                """
                SELECT BoneHealthL1TrabecularHuMean, BoneHealthL1Classification, BoneHealthL1QcPass
                FROM dicom_metadata
                WHERE StudyInstanceUID = ?
                """,
                ("1.2.4",),
            ).fetchone()
            self.assertEqual(row["BoneHealthL1TrabecularHuMean"], 88.0)
            self.assertEqual(row["BoneHealthL1Classification"], "osteoporosis")
            self.assertEqual(row["BoneHealthL1QcPass"], 0)
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
