import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from heimdallr.segmentation.worker import should_reuse_existing_segmentation
from heimdallr.shared import store


class TestSegmentationReuse(unittest.TestCase):
    def test_reuses_when_sqlite_signature_matches_and_outputs_exist(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_output = Path(tmpdir) / "case"
            total_dir = case_output / "artifacts" / "total"
            tissue_dir = case_output / "artifacts" / "tissue_types"
            total_dir.mkdir(parents=True, exist_ok=True)
            tissue_dir.mkdir(parents=True, exist_ok=True)
            (total_dir / "mask.nii.gz").write_bytes(b"1")
            (tissue_dir / "mask.nii.gz").write_bytes(b"1")

            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            try:
                store.ensure_schema(conn)
                store.upsert_study_metadata(
                    conn,
                    {
                        "StudyInstanceUID": "1.2.3",
                        "PatientName": "Alice Example",
                        "ClinicalName": "AliceE_20260407_1",
                        "AccessionNumber": "1",
                        "StudyDate": "20260407",
                        "Modality": "CT",
                    },
                )
                store.update_segmentation_signature(
                    conn,
                    "1.2.3",
                    series_instance_uid="1.2.3.4.5",
                    slice_count=476,
                    profile_name="ct_native_segmentation_only",
                    task_names=["total", "tissue_types"],
                )

                with patch("heimdallr.segmentation.worker.db_connect", return_value=conn):
                    reused = should_reuse_existing_segmentation(
                        "1.2.3",
                        case_output,
                        {
                            "SelectedSeriesInstanceUID": "1.2.3.4.5",
                            "SliceCount": 476,
                        },
                        "ct_native_segmentation_only",
                        [
                            {"name": "total", "output_dir": "artifacts/total"},
                            {"name": "tissue_types", "output_dir": "artifacts/tissue_types"},
                        ],
                    )
            finally:
                conn.close()

        self.assertTrue(reused)

    def test_does_not_reuse_when_slice_count_changes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_output = Path(tmpdir) / "case"
            total_dir = case_output / "artifacts" / "total"
            tissue_dir = case_output / "artifacts" / "tissue_types"
            total_dir.mkdir(parents=True, exist_ok=True)
            tissue_dir.mkdir(parents=True, exist_ok=True)
            (total_dir / "mask.nii.gz").write_bytes(b"1")
            (tissue_dir / "mask.nii.gz").write_bytes(b"1")

            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            try:
                store.ensure_schema(conn)
                store.upsert_study_metadata(
                    conn,
                    {
                        "StudyInstanceUID": "1.2.3",
                        "PatientName": "Alice Example",
                        "ClinicalName": "AliceE_20260407_1",
                        "AccessionNumber": "1",
                        "StudyDate": "20260407",
                        "Modality": "CT",
                    },
                )
                store.update_segmentation_signature(
                    conn,
                    "1.2.3",
                    series_instance_uid="1.2.3.4.5",
                    slice_count=476,
                    profile_name="ct_native_segmentation_only",
                    task_names=["total", "tissue_types"],
                )

                with patch("heimdallr.segmentation.worker.db_connect", return_value=conn):
                    reused = should_reuse_existing_segmentation(
                        "1.2.3",
                        case_output,
                        {
                            "SelectedSeriesInstanceUID": "1.2.3.4.5",
                            "SliceCount": 477,
                        },
                        "ct_native_segmentation_only",
                        [
                            {"name": "total", "output_dir": "artifacts/total"},
                            {"name": "tissue_types", "output_dir": "artifacts/tissue_types"},
                        ],
                    )
            finally:
                conn.close()

        self.assertFalse(reused)


if __name__ == "__main__":
    unittest.main()
