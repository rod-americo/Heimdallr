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
    WorkerShutdownRequestedError,
    resolve_segmentation_plan,
    segment_case,
    should_reuse_existing_segmentation,
)
from heimdallr.shared import store


class TestSegmentationReuse(unittest.TestCase):
    def test_segment_case_propagates_worker_shutdown_for_queue_retry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            case_id = "ShutdownCase_20260420_1"
            case_dir = base / case_id
            metadata_dir = case_dir / "metadata"
            logs_dir = case_dir / "logs"
            artifacts_dir = case_dir / "artifacts"
            derived_dir = case_dir / "derived"
            metadata_dir.mkdir(parents=True, exist_ok=True)
            logs_dir.mkdir(parents=True, exist_ok=True)
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            derived_dir.mkdir(parents=True, exist_ok=True)

            id_json_path = metadata_dir / "id.json"
            id_json_path.write_text(
                json.dumps(
                    {
                        "CaseID": case_id,
                        "StudyInstanceUID": "1.2.3",
                        "PatientName": "Alice Example",
                        "AccessionNumber": "1",
                        "StudyDate": "20260420",
                        "Modality": "CT",
                        "Pipeline": {},
                    }
                ),
                encoding="utf-8",
            )
            selected_nifti = base / "selected.nii.gz"
            selected_nifti.write_bytes(gzip.compress(b"1"))
            shutdown_error = WorkerShutdownRequestedError(
                "[total] Worker shutdown requested while task was still running"
            )

            with (
                patch("heimdallr.segmentation.worker.study_dir", return_value=case_dir),
                patch("heimdallr.segmentation.worker.study_artifacts_dir", return_value=artifacts_dir),
                patch("heimdallr.segmentation.worker.study_derived_dir", return_value=derived_dir),
                patch("heimdallr.segmentation.worker.study_logs_dir", return_value=logs_dir),
                patch("heimdallr.segmentation.worker.study_metadata_dir", return_value=metadata_dir),
                patch("heimdallr.segmentation.worker.study_id_json", return_value=id_json_path),
                patch(
                    "heimdallr.segmentation.worker.select_prepared_series",
                    return_value=(
                        selected_nifti,
                        {
                            "SelectedSeriesNumber": "2",
                            "SelectedPhase": "native",
                            "SliceCount": 100,
                            "SelectedSeriesInstanceUID": "1.2.3.4.5",
                        },
                    ),
                ),
                patch(
                    "heimdallr.segmentation.worker.resolve_segmentation_plan",
                    return_value=("ct_native_segmentation_only", [{"name": "total"}]),
                ),
                patch(
                    "heimdallr.segmentation.worker.should_reuse_existing_segmentation",
                    return_value=(False, None),
                ),
                patch(
                    "heimdallr.segmentation.worker.run_segmentation_pipeline",
                    side_effect=shutdown_error,
                ),
                patch("heimdallr.segmentation.worker.db_connect", return_value=MagicMock()),
                patch("heimdallr.segmentation.worker.store.update_id_json"),
            ):
                with self.assertRaises(WorkerShutdownRequestedError):
                    segment_case(case_dir)

            payload = json.loads(id_json_path.read_text(encoding="utf-8"))
            pipeline = payload["Pipeline"]
            self.assertEqual(pipeline["segmentation_status"], "error")
            self.assertEqual(
                pipeline["segmentation_error"],
                "[total] Worker shutdown requested while task was still running",
            )
            self.assertEqual(
                (logs_dir / "error.log").read_text(encoding="utf-8"),
                "[total] Worker shutdown requested while task was still running",
            )

    def test_resolve_segmentation_plan_accepts_portal_venous_fallback(self):
        with patch(
            "heimdallr.segmentation.worker.load_segmentation_pipeline_profile",
            return_value=(
                "ct_native_segmentation_only",
                {
                    "required": {"modality": "CT", "selected_phase": ["native"]},
                    "tasks": [{"name": "total", "enabled": True}],
                },
            ),
        ):
            profile_name, tasks = resolve_segmentation_plan("CT", "portal_venous")

        self.assertEqual(profile_name, "ct_native_segmentation_only")
        self.assertEqual(tasks, [{"name": "total", "enabled": True}])

    def test_resolve_segmentation_plan_accepts_any_contrast_fallback(self):
        with patch(
            "heimdallr.segmentation.worker.load_segmentation_pipeline_profile",
            return_value=(
                "ct_native_segmentation_only",
                {
                    "required": {"modality": "CT", "selected_phase": ["native"]},
                    "tasks": [{"name": "total", "enabled": True}],
                },
            ),
        ):
            profile_name, tasks = resolve_segmentation_plan("CT", "arterial")

        self.assertEqual(profile_name, "ct_native_segmentation_only")
        self.assertEqual(tasks, [{"name": "total", "enabled": True}])

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
                    elapsed_time="0:03:21",
                )

                with patch("heimdallr.segmentation.worker.db_connect", return_value=conn):
                    reused, elapsed = should_reuse_existing_segmentation(
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
        self.assertEqual(elapsed, "0:03:21")

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
                    elapsed_time="0:03:21",
                )

                with patch("heimdallr.segmentation.worker.db_connect", return_value=conn):
                    reused, elapsed = should_reuse_existing_segmentation(
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
        self.assertIsNone(elapsed)

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
                    elapsed_time="0:03:21",
                )

                with patch("heimdallr.segmentation.worker.db_connect", return_value=conn):
                    reused, elapsed = should_reuse_existing_segmentation(
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
        self.assertIsNone(elapsed)


if __name__ == "__main__":
    unittest.main()
