import sqlite3
import tempfile
import unittest
import os
from pathlib import Path
from unittest.mock import patch

from heimdallr.shared import store
from heimdallr.space_manager import worker


def _connect_row_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


class TestSpaceManager(unittest.TestCase):
    def test_list_purge_candidates_skips_protected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            studies_dir = Path(tmpdir)
            oldest = studies_dir / "CaseOld"
            protected = studies_dir / "CaseBusy"
            newest = studies_dir / "CaseNew"
            oldest.mkdir()
            protected.mkdir()
            newest.mkdir()

            oldest.touch()
            protected.touch()
            newest.touch()
            os.utime(oldest, (1, 1))
            os.utime(protected, (2, 2))
            os.utime(newest, (3, 3))

            candidates = worker.list_purge_candidates(studies_dir, {"CaseBusy"})
            self.assertEqual([path.name for path in candidates], ["CaseOld", "CaseNew"])

    def test_reclaim_space_once_purges_oldest_and_db_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            studies_dir = Path(tmpdir) / "studies"
            studies_dir.mkdir(parents=True, exist_ok=True)
            db_path = Path(tmpdir) / "dicom.db"
            old_case = studies_dir / "CaseOld_20260101_1"
            busy_case = studies_dir / "CaseBusy_20260102_2"
            new_case = studies_dir / "CaseNew_20260103_3"
            for case_dir in (old_case, busy_case, new_case):
                (case_dir / "metadata").mkdir(parents=True, exist_ok=True)

            old_case.touch()
            busy_case.touch()
            new_case.touch()
            os.utime(old_case, (1, 1))
            os.utime(busy_case, (2, 2))
            os.utime(new_case, (3, 3))

            conn = _connect_row_db(db_path)
            store.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO dicom_metadata (StudyInstanceUID, ClinicalName, IdJson)
                VALUES (?, ?, ?)
                """,
                ("1.2.3", old_case.name, '{"CaseID": "CaseOld_20260101_1"}'),
            )
            conn.execute(
                """
                INSERT INTO segmentation_queue (case_id, input_path, status, created_at)
                VALUES (?, ?, 'claimed', '2026-04-06 15:00:00')
                """,
                (busy_case.name, str(busy_case)),
            )
            conn.commit()

            snapshots = iter(
                [
                    worker.DiskSnapshot(100, 85, 15),
                    worker.DiskSnapshot(100, 85, 15),
                    worker.DiskSnapshot(100, 75, 25),
                    worker.DiskSnapshot(100, 75, 25),
                ]
            )

            with (
                patch.object(worker.settings, "STUDIES_DIR", studies_dir),
                patch(
                    "heimdallr.space_manager.worker.db_connect",
                    side_effect=lambda: _connect_row_db(db_path),
                ),
                patch("heimdallr.space_manager.worker._disk_snapshot", side_effect=lambda _path: next(snapshots)),
            ):
                deletions = worker.reclaim_space_once(
                    studies_dir=studies_dir,
                    threshold_percent=80.0,
                )

            conn = _connect_row_db(db_path)
            self.assertEqual([item["case_id"] for item in deletions], [old_case.name])
            self.assertFalse(old_case.exists())
            self.assertTrue(busy_case.exists())
            self.assertTrue(new_case.exists())
            metadata_row = conn.execute(
                "SELECT ArtifactsPurged, ArtifactsPurgedAt FROM dicom_metadata WHERE StudyInstanceUID = ?",
                ("1.2.3",),
            ).fetchone()
            self.assertIsNotNone(metadata_row)
            self.assertEqual(metadata_row["ArtifactsPurged"], 1)
            self.assertTrue(metadata_row["ArtifactsPurgedAt"])
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM segmentation_queue WHERE case_id = ?", (busy_case.name,)).fetchone()[0],
                1,
            )
            conn.close()


if __name__ == "__main__":
    unittest.main()
