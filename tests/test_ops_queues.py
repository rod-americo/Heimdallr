import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from heimdallr.control_plane.app import create_app
from heimdallr.shared import settings, store


class TestOpsQueues(unittest.TestCase):
    def test_queue_capacity_reports_counts_without_case_ids(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dicom.db"
            runtime_dir = Path(tmpdir) / "runtime"
            runtime_dir.mkdir()
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                store.ensure_schema(conn)
                conn.execute(
                    """
                    INSERT INTO segmentation_queue (case_id, input_path, status, created_at)
                    VALUES
                        ('CasePending', '/tmp/case-pending', 'pending', '2026-05-23 00:01:00'),
                        ('CaseClaimed', '/tmp/case-claimed', 'claimed', '2026-05-23 00:02:00')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO metrics_queue (case_id, input_path, status, created_at)
                    VALUES ('CaseMetrics', '/tmp/case-metrics', 'error', '2026-05-23 00:03:00')
                    """
                )
                conn.commit()
            finally:
                conn.close()

            with patch.object(settings, "DB_PATH", db_path), patch.object(settings, "RUNTIME_DIR", runtime_dir):
                client = TestClient(create_app())
                response = client.get("/ops/queues")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["capacity"]["segmentation_active"], 2)
        self.assertEqual(body["queues"]["segmentation"]["pending"], 1)
        self.assertEqual(body["queues"]["segmentation"]["claimed"], 1)
        self.assertEqual(body["queues"]["metrics"]["error"], 1)
        self.assertEqual(body["queues"]["segmentation"]["oldest_pending_created_at"], "2026-05-23 00:01:00")
        self.assertNotIn("CasePending", str(body))


if __name__ == "__main__":
    unittest.main()
