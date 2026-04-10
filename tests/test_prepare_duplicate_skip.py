import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from heimdallr.prepare import worker
from heimdallr.shared import store


class TestPrepareDuplicateSkip(unittest.TestCase):
    def _build_completed_case(self, root: Path, case_id: str) -> None:
        total_dir = root / "studies" / case_id / "artifacts" / "total"
        tissue_dir = root / "studies" / case_id / "artifacts" / "tissue_types"
        metadata_dir = root / "studies" / case_id / "metadata"
        total_dir.mkdir(parents=True, exist_ok=True)
        tissue_dir.mkdir(parents=True, exist_ok=True)
        metadata_dir.mkdir(parents=True, exist_ok=True)
        (total_dir / "vertebrae_L3.nii.gz").write_bytes(b"1")
        (tissue_dir / "skeletal_muscle.nii.gz").write_bytes(b"1")
        (metadata_dir / "resultados.json").write_text("{}", encoding="utf-8")

    def test_returns_context_when_completed_pipeline_matches(self):
        case_id = "AliceE_20260410_1"
        study_uid = "1.2.3"
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._build_completed_case(root, case_id)

            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            try:
                store.ensure_schema(conn)
                store.upsert_study_metadata(
                    conn,
                    {
                        "StudyInstanceUID": study_uid,
                        "PatientName": "Alice Example",
                        "ClinicalName": case_id,
                        "AccessionNumber": "1",
                        "StudyDate": "20260410",
                        "Modality": "CT",
                    },
                )
                store.update_segmentation_signature(
                    conn,
                    study_uid,
                    series_instance_uid="1.2.3.4.5",
                    slice_count=476,
                    profile_name="ct_native_segmentation_only",
                    task_names=["total", "tissue_types"],
                )
                store.update_metrics_completion(
                    conn,
                    study_uid,
                    profile_name="ct_native_basic_metrics",
                )

                with patch.object(worker.settings, "STUDIES_DIR", root / "studies"):
                    with patch.object(worker, "db_connect", return_value=conn):
                        with patch.object(
                            worker,
                            "_select_prepared_series_for_duplicate_check",
                            return_value=(
                                root / "studies" / case_id / "derived" / "series" / "selected.nii.gz",
                                {
                                    "SelectedSeriesInstanceUID": "1.2.3.4.5",
                                    "SelectedPhase": "native",
                                    "SliceCount": 476,
                                },
                            ),
                        ):
                            with patch.object(
                                worker,
                                "_resolve_segmentation_plan_for_duplicate_check",
                                return_value=(
                                    "ct_native_segmentation_only",
                                    [
                                        {"name": "total", "output_dir": "artifacts/total"},
                                        {"name": "tissue_types", "output_dir": "artifacts/tissue_types"},
                                    ],
                                ),
                            ):
                                with patch.object(
                                    worker,
                                    "_load_metrics_pipeline_profile_for_duplicate_check",
                                    return_value=("ct_native_basic_metrics", {}),
                                ):
                                    context = worker._completed_case_skip_context(
                                        case_id,
                                        {
                                            "StudyInstanceUID": study_uid,
                                            "Modality": "CT",
                                        },
                                    )
            finally:
                conn.close()

        self.assertIsNotNone(context)
        self.assertEqual(context["segmentation_profile"], "ct_native_segmentation_only")
        self.assertEqual(context["metrics_profile"], "ct_native_basic_metrics")
        self.assertEqual(context["selection_info"]["SelectedSeriesInstanceUID"], "1.2.3.4.5")

    def test_does_not_skip_when_egress_is_incomplete(self):
        case_id = "AliceE_20260410_1"
        study_uid = "1.2.3"
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._build_completed_case(root, case_id)

            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            try:
                store.ensure_schema(conn)
                store.upsert_study_metadata(
                    conn,
                    {
                        "StudyInstanceUID": study_uid,
                        "PatientName": "Alice Example",
                        "ClinicalName": case_id,
                        "AccessionNumber": "1",
                        "StudyDate": "20260410",
                        "Modality": "CT",
                    },
                )
                store.update_segmentation_signature(
                    conn,
                    study_uid,
                    series_instance_uid="1.2.3.4.5",
                    slice_count=476,
                    profile_name="ct_native_segmentation_only",
                    task_names=["total", "tissue_types"],
                )
                store.update_metrics_completion(
                    conn,
                    study_uid,
                    profile_name="ct_native_basic_metrics",
                )
                store.enqueue_dicom_export(
                    conn,
                    case_id=case_id,
                    study_uid=study_uid,
                    artifact_path="artifacts/metrics/out.dcm",
                    artifact_type="secondary_capture",
                    destination_name="return_to_sender",
                    destination_host="127.0.0.1",
                    destination_port=104,
                    destination_called_aet="TEST",
                    source_calling_aet="SRC",
                    source_remote_ip="127.0.0.2",
                    artifact_digest="abc",
                )
                queue_id = conn.execute(
                    "SELECT id FROM dicom_egress_queue WHERE case_id = ?",
                    (case_id,),
                ).fetchone()["id"]
                store.mark_dicom_egress_queue_item_error(conn, queue_id, "send failed")

                with patch.object(worker.settings, "STUDIES_DIR", root / "studies"):
                    with patch.object(worker, "db_connect", return_value=conn):
                        with patch.object(
                            worker,
                            "_select_prepared_series_for_duplicate_check",
                            return_value=(
                                root / "studies" / case_id / "derived" / "series" / "selected.nii.gz",
                                {
                                    "SelectedSeriesInstanceUID": "1.2.3.4.5",
                                    "SelectedPhase": "native",
                                    "SliceCount": 476,
                                },
                            ),
                        ):
                            with patch.object(
                                worker,
                                "_resolve_segmentation_plan_for_duplicate_check",
                                return_value=(
                                    "ct_native_segmentation_only",
                                    [
                                        {"name": "total", "output_dir": "artifacts/total"},
                                        {"name": "tissue_types", "output_dir": "artifacts/tissue_types"},
                                    ],
                                ),
                            ):
                                with patch.object(
                                    worker,
                                    "_load_metrics_pipeline_profile_for_duplicate_check",
                                    return_value=("ct_native_basic_metrics", {}),
                                ):
                                    context = worker._completed_case_skip_context(
                                        case_id,
                                        {
                                            "StudyInstanceUID": study_uid,
                                            "Modality": "CT",
                                        },
                                    )
            finally:
                conn.close()

        self.assertIsNone(context)

    def test_accepts_legacy_completed_case_without_metrics_timestamp(self):
        case_id = "AliceE_20260410_1"
        study_uid = "1.2.3"
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._build_completed_case(root, case_id)

            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            try:
                store.ensure_schema(conn)
                store.upsert_study_metadata(
                    conn,
                    {
                        "StudyInstanceUID": study_uid,
                        "PatientName": "Alice Example",
                        "ClinicalName": case_id,
                        "AccessionNumber": "1",
                        "StudyDate": "20260410",
                        "Modality": "CT",
                    },
                )
                store.update_segmentation_signature(
                    conn,
                    study_uid,
                    series_instance_uid="1.2.3.4.5",
                    slice_count=476,
                    profile_name="ct_native_segmentation_only",
                    task_names=["total", "tissue_types"],
                )
                store.enqueue_case_for_metrics(
                    conn,
                    case_id,
                    str(root / "studies" / case_id),
                )
                queue_id = conn.execute(
                    "SELECT id FROM metrics_queue WHERE case_id = ?",
                    (case_id,),
                ).fetchone()["id"]
                store.mark_metrics_queue_item_done(conn, queue_id)

                with patch.object(worker.settings, "STUDIES_DIR", root / "studies"):
                    with patch.object(worker, "db_connect", return_value=conn):
                        with patch.object(
                            worker,
                            "_select_prepared_series_for_duplicate_check",
                            return_value=(
                                root / "studies" / case_id / "derived" / "series" / "selected.nii.gz",
                                {
                                    "SelectedSeriesInstanceUID": "1.2.3.4.5",
                                    "SelectedPhase": "native",
                                    "SliceCount": 476,
                                },
                            ),
                        ):
                            with patch.object(
                                worker,
                                "_resolve_segmentation_plan_for_duplicate_check",
                                return_value=(
                                    "ct_native_segmentation_only",
                                    [
                                        {"name": "total", "output_dir": "artifacts/total"},
                                        {"name": "tissue_types", "output_dir": "artifacts/tissue_types"},
                                    ],
                                ),
                            ):
                                with patch.object(
                                    worker,
                                    "_load_metrics_pipeline_profile_for_duplicate_check",
                                    return_value=("ct_native_basic_metrics", {}),
                                ):
                                    context = worker._completed_case_skip_context(
                                        case_id,
                                        {
                                            "StudyInstanceUID": study_uid,
                                            "Modality": "CT",
                                        },
                                    )
            finally:
                conn.close()

        self.assertIsNotNone(context)


if __name__ == "__main__":
    unittest.main()
