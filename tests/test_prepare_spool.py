import tempfile
import unittest
import zipfile
import sqlite3
from pathlib import Path
from unittest.mock import patch

from heimdallr.prepare import worker
from heimdallr.shared import store
from heimdallr.shared.spool import CLAIM_SUFFIX


class TestPrepareSpoolOrder(unittest.TestCase):
    def test_iter_claimable_uploads_prioritizes_from_prepare_then_external_in_fifo(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            upload_root = Path(tmpdir)
            from_prepare_dir = upload_root / "from_prepare"
            external_dir = upload_root / "external"
            from_prepare_dir.mkdir(parents=True)
            external_dir.mkdir(parents=True)

            from_prepare_claimed = from_prepare_dir / f"study_20260407100000_a.zip{CLAIM_SUFFIX}"
            from_prepare_ready = from_prepare_dir / "study_20260407100500_b.zip"
            external_claimed = external_dir / f"study_20260407100100_c.zip{CLAIM_SUFFIX}"
            external_ready = external_dir / "study_20260407100600_d.zip"

            for path in (from_prepare_claimed, from_prepare_ready, external_claimed, external_ready):
                path.write_bytes(b"zip")

            with patch.object(worker.settings, "UPLOAD_DIR", upload_root):
                with patch.object(worker.settings, "UPLOAD_FROM_PREPARE_DIR", from_prepare_dir):
                    with patch.object(worker.settings, "UPLOAD_EXTERNAL_DIR", external_dir):
                        with patch.object(worker, "is_spooled_zip_stable", return_value=True):
                            paths = list(worker.iter_claimable_uploads())

            self.assertEqual(
                [str(p.relative_to(upload_root)) for p in paths],
                [
                    str(from_prepare_claimed.relative_to(upload_root)),
                    f"{from_prepare_ready.relative_to(upload_root)}{CLAIM_SUFFIX}",
                    str(external_claimed.relative_to(upload_root)),
                    f"{external_ready.relative_to(upload_root)}{CLAIM_SUFFIX}",
                ],
            )

    def test_iter_claimable_uploads_keeps_legacy_root_as_last_resort(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            upload_root = Path(tmpdir)
            from_prepare_dir = upload_root / "from_prepare"
            external_dir = upload_root / "external"
            from_prepare_dir.mkdir(parents=True)
            external_dir.mkdir(parents=True)
            legacy_ready = upload_root / "study_20260407101000_legacy.zip"
            legacy_ready.write_bytes(b"zip")

            with patch.object(worker.settings, "UPLOAD_DIR", upload_root):
                with patch.object(worker.settings, "UPLOAD_FROM_PREPARE_DIR", from_prepare_dir):
                    with patch.object(worker.settings, "UPLOAD_EXTERNAL_DIR", external_dir):
                        with patch.object(worker, "is_spooled_zip_stable", return_value=True):
                            paths = list(worker.iter_claimable_uploads())

            self.assertEqual(
                [p.name for p in paths],
                [f"{legacy_ready.name}{CLAIM_SUFFIX}"],
            )

    def test_process_spooled_zip_marks_manifest_error_on_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime = Path(tmpdir)
            db_path = runtime / "dicom.db"
            failed_dir = runtime / "failed"
            failed_dir.mkdir(parents=True, exist_ok=True)
            zip_path = runtime / f"study.zip{CLAIM_SUFFIX}"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr(
                    worker.INTAKE_MANIFEST_NAME,
                    (
                        '{"study_uid":"1.2.3",'
                        '"manifest_digest":"digest-1",'
                        '"instance_count":42}'
                    ),
                )

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                store.register_study_handoff(
                    conn,
                    study_uid="1.2.3",
                    manifest_digest="digest-1",
                    instance_count=42,
                    calling_aet="SRC",
                    remote_ip="10.0.0.1",
                )
            finally:
                conn.close()

            with patch.object(worker.settings, "DB_PATH", db_path):
                with patch.object(worker.settings, "UPLOAD_FAILED_DIR", failed_dir):
                    with patch.object(worker, "process_zip", side_effect=RuntimeError("boom")):
                        ok = worker.process_spooled_zip(zip_path)

            self.assertFalse(ok)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                row = store.get_study_handoff_state(conn, "1.2.3", "digest-1")
            finally:
                conn.close()
            self.assertEqual(row["status"], "error")
            self.assertEqual(row["last_error"], "boom")
            self.assertTrue((failed_dir / "study.zip").exists())


if __name__ == "__main__":
    unittest.main()
