import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from heimdallr.segmentation.worker import select_prepared_series
from heimdallr.shared import settings


class TestSeriesSelection(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
