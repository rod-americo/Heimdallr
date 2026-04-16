import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nibabel as nib
import numpy as np

from heimdallr.segmentation.worker import select_prepared_series
from heimdallr.shared import settings
from heimdallr.shared import store
from heimdallr.shared.segmentation_coverage import (
    SEGMENTATION_COVERAGE_CHEST_ABDOMEN,
    SEGMENTATION_COVERAGE_CHEST_ONLY,
    classify_segmentation_coverage,
)


class TestSeriesSelection(unittest.TestCase):
    @staticmethod
    def _write_nifti(path: Path, data: np.ndarray) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        nib.save(nib.Nifti1Image(data.astype(np.float32), np.eye(4)), str(path))

    def test_hard_rejects_kernel_and_description_matches(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "case-series-selection"
            derived_dir = root / "runtime" / "studies" / case_id / "derived" / "series"
            derived_dir.mkdir(parents=True, exist_ok=True)

            accepted_path = derived_dir / "accepted.nii.gz"
            rejected_path = derived_dir / "rejected.nii.gz"
            accepted_path.write_bytes(b"ok")
            rejected_path.write_bytes(b"no")

            config_path = root / "series_selection.json"
            config_path.write_text(
                json.dumps(
                    {
                        "default_profile": "ct_test",
                        "profiles": {
                            "ct_test": {
                                "required": {"modality": "CT", "min_slices": 120},
                                "hard_reject": {
                                    "description_contains": ["lung", "topogram"],
                                    "kernel_contains": ["sharp"],
                                    "kernel_exact": ["fc52"],
                                },
                                "phase_priority": ["native"],
                                "text_hints": {
                                    "description_avoid": [],
                                    "kernel_avoid": [],
                                },
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            id_data = {
                "AvailableSeries": [
                    {
                        "SeriesInstanceUID": "1.2.3.bad",
                        "SeriesNumber": "2",
                        "DerivedNiftiPath": "series/rejected.nii.gz",
                        "Modality": "CT",
                        "SliceCount": 240,
                        "DetectedPhase": "native",
                        "PhaseDetected": True,
                        "PhaseData": {"probability": 0.99},
                        "SeriesDescription": "PULMAO Lung 2.0",
                        "ConvolutionKernel": "FC52",
                    },
                    {
                        "SeriesInstanceUID": "1.2.3.good",
                        "SeriesNumber": "3",
                        "DerivedNiftiPath": "series/accepted.nii.gz",
                        "Modality": "CT",
                        "SliceCount": 180,
                        "DetectedPhase": "native",
                        "PhaseDetected": True,
                        "PhaseData": {"probability": 0.90},
                        "SeriesDescription": "MEDIASTINO Body 2.0",
                        "ConvolutionKernel": "FC18",
                    },
                ]
            }

            with patch.object(settings, "STUDIES_DIR", root / "runtime" / "studies"), patch.object(
                settings,
                "SERIES_SELECTION_CONFIG_PATH",
                config_path,
            ):
                selected_path, selection_info = select_prepared_series(case_id, id_data)

        self.assertEqual(selected_path.name, "accepted.nii.gz")
        self.assertEqual(selection_info["SelectedSeriesInstanceUID"], "1.2.3.good")

    def test_falls_back_to_portal_venous_when_native_is_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "case-series-selection"
            derived_dir = root / "runtime" / "studies" / case_id / "derived" / "series"
            derived_dir.mkdir(parents=True, exist_ok=True)

            portal_path = derived_dir / "portal.nii.gz"
            portal_path.write_bytes(b"ok")

            config_path = root / "series_selection.json"
            config_path.write_text(
                json.dumps(
                    {
                        "default_profile": "ct_test",
                        "profiles": {
                            "ct_test": {
                                "required": {"modality": "CT", "min_slices": 120},
                                "hard_reject": {},
                                "phase_priority": ["native", "portal_venous"],
                                "text_hints": {
                                    "description_avoid": [],
                                    "kernel_avoid": [],
                                },
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            id_data = {
                "AvailableSeries": [
                    {
                        "SeriesInstanceUID": "1.2.3.portal",
                        "SeriesNumber": "5",
                        "DerivedNiftiPath": "series/portal.nii.gz",
                        "Modality": "CT",
                        "SliceCount": 180,
                        "DetectedPhase": "portal_venous",
                        "PhaseDetected": True,
                        "PhaseData": {"probability": 0.95},
                        "SeriesDescription": "PORTAL Body 2.0 CE",
                        "ConvolutionKernel": "FC18",
                    }
                ]
            }

            with patch.object(settings, "STUDIES_DIR", root / "runtime" / "studies"), patch.object(
                settings,
                "SERIES_SELECTION_CONFIG_PATH",
                config_path,
            ):
                selected_path, selection_info = select_prepared_series(case_id, id_data)

        self.assertEqual(selected_path.name, "portal.nii.gz")
        self.assertEqual(selection_info["SelectedPhase"], "portal_venous")

    def test_falls_back_to_any_contrast_when_native_and_portal_are_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "case-series-selection"
            derived_dir = root / "runtime" / "studies" / case_id / "derived" / "series"
            derived_dir.mkdir(parents=True, exist_ok=True)

            arterial_path = derived_dir / "arterial.nii.gz"
            arterial_path.write_bytes(b"ok")

            config_path = root / "series_selection.json"
            config_path.write_text(
                json.dumps(
                    {
                        "default_profile": "ct_test",
                        "profiles": {
                            "ct_test": {
                                "required": {"modality": "CT", "min_slices": 120},
                                "hard_reject": {},
                                "phase_priority": ["native", "portal_venous"],
                                "text_hints": {
                                    "description_avoid": [],
                                    "kernel_avoid": [],
                                },
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            id_data = {
                "AvailableSeries": [
                    {
                        "SeriesInstanceUID": "1.2.3.arterial",
                        "SeriesNumber": "7",
                        "DerivedNiftiPath": "series/arterial.nii.gz",
                        "Modality": "CT",
                        "SliceCount": 180,
                        "DetectedPhase": "arterial",
                        "PhaseDetected": True,
                        "PhaseData": {"probability": 0.91},
                        "SeriesDescription": "ARTERIAL Body 2.0 CE",
                        "ConvolutionKernel": "FC18",
                    }
                ]
            }

            with patch.object(settings, "STUDIES_DIR", root / "runtime" / "studies"), patch.object(
                settings,
                "SERIES_SELECTION_CONFIG_PATH",
                config_path,
            ):
                selected_path, selection_info = select_prepared_series(case_id, id_data)

        self.assertEqual(selected_path.name, "arterial.nii.gz")
        self.assertEqual(selection_info["SelectedPhase"], "arterial")
        self.assertIn("fallback=contrast_fallback", selection_info["SelectionReason"])

    def test_prefers_follow_up_abdomen_when_previous_coverage_was_chest_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "case-series-selection"
            study_uid = "1.2.3.study"
            derived_dir = root / "runtime" / "studies" / case_id / "derived" / "series"
            derived_dir.mkdir(parents=True, exist_ok=True)

            chest_path = derived_dir / "chest.nii.gz"
            abdomen_path = derived_dir / "abdomen.nii.gz"
            chest_path.write_bytes(b"chest")
            abdomen_path.write_bytes(b"abdomen")

            config_path = root / "series_selection.json"
            config_path.write_text(
                json.dumps(
                    {
                        "default_profile": "ct_test",
                        "profiles": {
                            "ct_test": {
                                "required": {"modality": "CT", "min_slices": 120},
                                "hard_reject": {},
                                "phase_priority": ["native", "portal_venous"],
                                "text_hints": {
                                    "description_avoid": [],
                                    "kernel_avoid": [],
                                },
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

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
                        "StudyDate": "20260416",
                        "Modality": "CT",
                    },
                )
                store.update_segmentation_signature(
                    conn,
                    study_uid,
                    series_instance_uid="1.2.3.chest",
                    slice_count=320,
                    profile_name="ct_native_segmentation_only",
                    task_names=["total", "tissue_types"],
                    elapsed_time="0:03:12",
                    coverage_class=SEGMENTATION_COVERAGE_CHEST_ONLY,
                )

                id_data = {
                    "StudyInstanceUID": study_uid,
                    "AvailableSeries": [
                        {
                            "SeriesInstanceUID": "1.2.3.chest",
                            "SeriesNumber": "2",
                            "DerivedNiftiPath": "series/chest.nii.gz",
                            "Modality": "CT",
                            "SliceCount": 320,
                            "DetectedPhase": "native",
                            "PhaseDetected": True,
                            "PhaseData": {"probability": 0.98},
                            "SeriesDescription": "CHEST Body 2.0",
                            "ConvolutionKernel": "FC18",
                        },
                        {
                            "SeriesInstanceUID": "1.2.3.abdomen",
                            "SeriesNumber": "8",
                            "DerivedNiftiPath": "series/abdomen.nii.gz",
                            "Modality": "CT",
                            "SliceCount": 300,
                            "DetectedPhase": "portal_venous",
                            "PhaseDetected": True,
                            "PhaseData": {"probability": 0.90},
                            "SeriesDescription": "ABDOMEN PORTAL Body 2.0 CE",
                            "ConvolutionKernel": "FC18",
                        },
                    ]
                }

                with patch.object(settings, "STUDIES_DIR", root / "runtime" / "studies"), patch.object(
                    settings,
                    "SERIES_SELECTION_CONFIG_PATH",
                    config_path,
                ), patch("heimdallr.segmentation.worker.db_connect", return_value=conn):
                    selected_path, selection_info = select_prepared_series(case_id, id_data)
            finally:
                conn.close()

        self.assertEqual(selected_path.name, "abdomen.nii.gz")
        self.assertEqual(selection_info["SelectedSeriesInstanceUID"], "1.2.3.abdomen")
        self.assertIn("follow_up_after=chest_only", selection_info["SelectionReason"])

    def test_classifies_chest_and_abdomen_complete_coverage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            total_dir = Path(tmpdir) / "artifacts" / "total"
            shape = (12, 12, 12)
            complete = np.zeros(shape, dtype=np.float32)
            complete[2:10, 2:10, 2:10] = 1.0

            for filename in (
                "lung_upper_lobe_left.nii.gz",
                "lung_upper_lobe_right.nii.gz",
                "lung_middle_lobe_right.nii.gz",
                "lung_lower_lobe_left.nii.gz",
                "lung_lower_lobe_right.nii.gz",
                "liver.nii.gz",
                "spleen.nii.gz",
                "kidney_left.nii.gz",
            ):
                self._write_nifti(total_dir / filename, complete)

            coverage = classify_segmentation_coverage(total_dir)

        self.assertEqual(coverage, SEGMENTATION_COVERAGE_CHEST_ABDOMEN)


if __name__ == "__main__":
    unittest.main()
