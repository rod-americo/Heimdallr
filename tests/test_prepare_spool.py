import tempfile
import unittest
import zipfile
import sqlite3
from pathlib import Path
from unittest.mock import patch
from pydicom.dataset import Dataset

from heimdallr.prepare import worker
from heimdallr.shared import store
from heimdallr.shared.spool import CLAIM_SUFFIX


class TestPrepareSpoolOrder(unittest.TestCase):
    def test_update_global_biometrics_from_dataset_fills_missing_values(self):
        global_meta = {"Height": None, "Weight": None}

        first = Dataset()
        first.PatientName = "Example^Patient"
        worker.update_global_biometrics_from_dataset(global_meta, first)
        self.assertIsNone(global_meta["Height"])
        self.assertIsNone(global_meta["Weight"])

        second = Dataset()
        second.PatientWeight = "72"
        worker.update_global_biometrics_from_dataset(global_meta, second)
        self.assertEqual(global_meta["Weight"], 72.0)
        self.assertIsNone(global_meta["Height"])

        third = Dataset()
        third.PatientSize = "1.68"
        worker.update_global_biometrics_from_dataset(global_meta, third)
        self.assertEqual(global_meta["Weight"], 72.0)
        self.assertEqual(global_meta["Height"], 1.68)

    def test_update_global_biometrics_from_dataset_preserves_existing_values(self):
        global_meta = {"Height": 1.70, "Weight": 80.0}
        ds = Dataset()
        ds.PatientWeight = "65"
        ds.PatientSize = "1.55"

        worker.update_global_biometrics_from_dataset(global_meta, ds)

        self.assertEqual(global_meta["Weight"], 80.0)
        self.assertEqual(global_meta["Height"], 1.70)

    def test_compute_series_geometry_summary_uses_projected_positions(self):
        series_data = {
            "files": ["a", "b", "c", "d"],
            "SliceThicknessValues": [1.0, 1.0, 1.0, 1.0],
            "SpacingBetweenSlicesValues": [1.0, 1.0, 1.0, 1.0],
            "GeometryPositions": [0.0, 1.0, 2.0, 3.0],
        }

        summary = worker.compute_series_geometry_summary(series_data)

        self.assertEqual(summary["CoverageMm"], 3.0)
        self.assertEqual(summary["ZSpacingMm"], 1.0)
        self.assertEqual(summary["SliceThicknessMm"], 1.0)
        self.assertEqual(summary["GeometrySlicePositions"], 4)
        self.assertEqual(summary["GeometryConfidence"], "position")

    def test_compute_series_geometry_summary_keeps_estimate_separate(self):
        series_data = {
            "files": ["a", "b", "c"],
            "SliceThicknessValues": [2.5, 2.5, 2.5],
            "SpacingBetweenSlicesValues": [],
            "GeometryPositions": [],
        }

        summary = worker.compute_series_geometry_summary(series_data)

        self.assertNotIn("CoverageMm", summary)
        self.assertEqual(summary["EstimatedCoverageMm"], 5.0)
        self.assertEqual(summary["GeometryConfidence"], "estimated")
        self.assertIn("missing_image_position_patient", summary["GeometryWarnings"])

    def test_persist_source_dicom_series_groups_instances_by_series(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_a = root / "input_a"
            source_b = root / "input_b"
            source_a.mkdir()
            source_b.mkdir()
            first = source_a / "image"
            second = source_b / "image"
            first.write_bytes(b"first")
            second.write_bytes(b"second")
            destination = root / "study" / "source" / "dicom" / "series"

            persisted = worker.persist_source_dicom_series(
                {
                    "1.2.3": {
                        "SeriesNumber": "4",
                        "Modality": "CT",
                        "SeriesDescriptionOriginal": "Body 2.0",
                        "files": [first, second],
                    }
                },
                destination,
            )

            series_dir = persisted["1.2.3"]["path"]
            self.assertEqual(persisted["1.2.3"]["count"], 2)
            self.assertEqual((series_dir / "instance_000001.dcm").read_bytes(), b"first")
            self.assertEqual((series_dir / "instance_000002.dcm").read_bytes(), b"second")
            self.assertTrue(str(series_dir).startswith(str(destination)))

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

    def test_pipeline_upload_zip_detection_only_matches_upload_spool(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime = Path(tmpdir)
            upload_root = runtime / "uploads"
            from_prepare_dir = upload_root / "from_prepare"
            external_dir = upload_root / "external"
            test_dataset_dir = runtime / "test_datasets" / "abdomen_complete"
            nested_external_dir = external_dir / "nested"
            for path in (upload_root, from_prepare_dir, external_dir, test_dataset_dir, nested_external_dir):
                path.mkdir(parents=True)

            from_prepare_zip = from_prepare_dir / "study_a.zip"
            external_claimed_zip = external_dir / f"study_b.zip{CLAIM_SUFFIX}"
            legacy_zip = upload_root / "study_c.zip"
            test_dataset_zip = test_dataset_dir / "study_d.zip"
            nested_external_zip = nested_external_dir / "study_e.zip"

            with patch.object(worker.settings, "UPLOAD_DIR", upload_root):
                with patch.object(worker.settings, "UPLOAD_FROM_PREPARE_DIR", from_prepare_dir):
                    with patch.object(worker.settings, "UPLOAD_EXTERNAL_DIR", external_dir):
                        self.assertTrue(worker._is_pipeline_upload_zip(from_prepare_zip))
                        self.assertTrue(worker._is_pipeline_upload_zip(external_claimed_zip))
                        self.assertTrue(worker._is_pipeline_upload_zip(legacy_zip))
                        self.assertFalse(worker._is_pipeline_upload_zip(test_dataset_zip))
                        self.assertFalse(worker._is_pipeline_upload_zip(nested_external_zip))

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
