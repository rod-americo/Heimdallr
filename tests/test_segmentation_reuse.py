import json
import gzip
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from heimdallr.segmentation.worker import (
    _record_segmentation_pipeline_state,
    should_reuse_existing_segmentation,
)
from heimdallr.shared import store


class TestSegmentationReuse(unittest.TestCase):
    def test_record_segmentation_pipeline_state_closes_failed_stage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            id_json_path = Path(tmpdir) / "id.json"
            id_json_path.write_text(
                json.dumps(
                    {
                        "CaseID": "case-1",
                        "StudyInstanceUID": "1.2.3",
                        "Pipeline": {
                            "prepare_elapsed_time": "0:00:40",
                            "prepare_start_time": "2026-04-10T16:59:00-03:00",
                            "start_time": "2026-04-10T17:00:00-03:00",
                            "segmentation_start_time": "2026-04-10T17:00:00-03:00",
                        },
                    }
                ),
                encoding="utf-8",
            )

            conn = MagicMock()
            with (
                patch("heimdallr.segmentation.worker.study_id_json", return_value=id_json_path),
                patch("heimdallr.segmentation.worker.db_connect", return_value=conn),
                patch("heimdallr.segmentation.worker.store.update_id_json"),
            ):
                _record_segmentation_pipeline_state(
                    "case-1",
                    status="error",
                    end_dt=datetime.fromisoformat("2026-04-10T17:01:15-03:00"),
                    error="segmentation failed",
                )

            payload = json.loads(id_json_path.read_text(encoding="utf-8"))
            pipeline = payload["Pipeline"]
            self.assertEqual(pipeline["segmentation_status"], "error")
            self.assertEqual(pipeline["segmentation_error"], "segmentation failed")
            self.assertEqual(pipeline["segmentation_end_time"], "2026-04-10T17:01:15-03:00")
            self.assertEqual(pipeline["segmentation_elapsed_time"], "0:01:15")
            self.assertEqual(pipeline["pipeline_active_elapsed_time"], "0:01:55")

    def test_reuses_when_sqlite_signature_matches_and_outputs_exist(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_output = Path(tmpdir) / "case"
            total_dir = case_output / "artifacts" / "total"
            tissue_dir = case_output / "artifacts" / "tissue_types"
            total_dir.mkdir(parents=True, exist_ok=True)
            tissue_dir.mkdir(parents=True, exist_ok=True)
            (total_dir / "mask.nii.gz").write_bytes(gzip.compress(b"1"))
            (tissue_dir / "mask.nii.gz").write_bytes(gzip.compress(b"1"))

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
            (total_dir / "mask.nii.gz").write_bytes(gzip.compress(b"1"))
            (tissue_dir / "mask.nii.gz").write_bytes(gzip.compress(b"1"))

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

    def test_does_not_reuse_when_existing_nifti_is_truncated(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_output = Path(tmpdir) / "case"
            total_dir = case_output / "artifacts" / "total"
            tissue_dir = case_output / "artifacts" / "tissue_types"
            total_dir.mkdir(parents=True, exist_ok=True)
            tissue_dir.mkdir(parents=True, exist_ok=True)
            with gzip.open(total_dir / "mask.nii.gz", "wb") as handle:
                handle.write(b"ok")
            (tissue_dir / "mask.nii.gz").write_bytes(gzip.compress(b"partial")[:-4])

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

        self.assertFalse(reused)


if __name__ == "__main__":
    unittest.main()
