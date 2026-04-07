import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from heimdallr.prepare import worker
from heimdallr.shared.spool import CLAIM_SUFFIX


class TestPrepareSpoolOrder(unittest.TestCase):
    def test_iter_claimable_uploads_uses_lifo_for_claimed_and_ready_zips(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            upload_dir = Path(tmpdir)

            claimed_old = upload_dir / f"study_20260407100000_a.zip{CLAIM_SUFFIX}"
            claimed_new = upload_dir / f"study_20260407100500_b.zip{CLAIM_SUFFIX}"
            ready_old = upload_dir / "study_20260407101000_c.zip"
            ready_new = upload_dir / "study_20260407101500_d.zip"

            for path in (claimed_old, claimed_new, ready_old, ready_new):
                path.write_bytes(b"zip")

            with patch.object(worker.settings, "UPLOAD_DIR", upload_dir):
                with patch.object(worker, "is_spooled_zip_stable", return_value=True):
                    paths = list(worker.iter_claimable_uploads())

            self.assertEqual(
                [p.name for p in paths],
                [
                    claimed_new.name,
                    claimed_old.name,
                    f"{ready_new.name}{CLAIM_SUFFIX}",
                    f"{ready_old.name}{CLAIM_SUFFIX}",
                ],
            )


if __name__ == "__main__":
    unittest.main()
