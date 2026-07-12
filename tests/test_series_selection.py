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

    def test_prefers_canon_mediastinum_over_fc30_lung_reconstruction(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "case-canon-chest-selection"
            derived_dir = root / "runtime" / "studies" / case_id / "derived" / "series"
            derived_dir.mkdir(parents=True, exist_ok=True)
            mediastinum_path = derived_dir / "mediastinum.nii.gz"
            lung_path = derived_dir / "lung.nii.gz"
            mediastinum_path.write_bytes(b"mediastinum")
            lung_path.write_bytes(b"lung")

            config_path = root / "series_selection.json"
            config_path.write_text(
                json.dumps(
                    {
                        "default_profile": "ct_test",
                        "profiles": {
                            "ct_test": {
                                "required": {"modality": "CT", "min_slices": 60},
                                "hard_reject": {
                                    "description_contains": ["lung", "pulmao"],
                                    "kernel_contains": ["bone", "lung"],
                                },
                                "phase_priority": ["native"],
                                "geometry_priority": {
                                    "enabled": True,
                                    "coverage_equivalence_ratio": 0.92,
                                    "coverage_equivalence_mm": 50,
                                    "prefer_thinner_within_equivalent_coverage": True,
                                },
                                "text_hints": {
                                    "description_prefer": ["mediastino", "mediastinum"],
                                    "kernel_avoid": ["sharp"],
                                    "kernel_prefer": ["body"],
                                },
                                "window_hints": {
                                    "soft_tissue_center_range": [-200, 200],
                                    "soft_tissue_width_range": [200, 800],
                                    "lung_center_max": -300,
                                    "lung_width_min": 1000,
                                },
                                "manufacturer_hints": [
                                    {
                                        "name": "canon_toshiba_body_reconstruction",
                                        "manufacturer_contains": ["canon", "toshiba"],
                                        "kernel_prefer": ["body"],
                                        "kernel_avoid": ["fc30"],
                                    }
                                ],
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            common = {
                "Modality": "CT",
                "SliceCount": 691,
                "DetectedPhase": "native",
                "PhaseDetected": True,
                "PhaseData": {"probability": 1.0},
                "CoverageMm": 345.0,
                "ZSpacingMm": 0.5,
                "SliceThicknessMm": 0.5,
                "Manufacturer": "Canon Medical Systems",
                "ManufacturerModelName": "Aquilion Lightning",
                "ProtocolName": "SAF TORAX",
            }
            id_data = {
                "AvailableSeries": [
                    {
                        **common,
                        "SeriesInstanceUID": "1.2.3.mediastinum",
                        "SeriesNumber": "6",
                        "DerivedNiftiPath": "series/mediastinum.nii.gz",
                        "SeriesDescription": "MEDIASTINO AICE 0.5",
                        "ConvolutionKernel": "BODY_SHARP",
                        "WindowCenter": "40",
                        "WindowWidth": "400",
                    },
                    {
                        **common,
                        "SeriesInstanceUID": "1.2.3.lung",
                        "SeriesNumber": "7",
                        "DerivedNiftiPath": "series/lung.nii.gz",
                        "SeriesDescription": "PULMÃO Bone 0.5",
                        "ConvolutionKernel": "FC30",
                        "WindowCenter": "-700",
                        "WindowWidth": "1700",
                    },
                ]
            }

            with patch.object(settings, "STUDIES_DIR", root / "runtime" / "studies"), patch.object(
                settings,
                "SERIES_SELECTION_CONFIG_PATH",
                config_path,
            ):
                selected_path, selection_info = select_prepared_series(case_id, id_data)

        self.assertEqual(selected_path.name, "mediastinum.nii.gz")
        self.assertEqual(selection_info["SelectedSeriesInstanceUID"], "1.2.3.mediastinum")
        self.assertEqual(selection_info["SelectedWindowClass"], "soft_tissue")
        self.assertIn(
            "canon_toshiba_body_reconstruction",
            selection_info["SelectedManufacturerHintRules"],
        )
        self.assertEqual(selection_info["RejectedSeries"][0]["reason"], "description_rejected:pulmao")

    def test_soft_tissue_scoring_breaks_geometry_tie_without_hard_reject(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "case-window-scoring"
            derived_dir = root / "runtime" / "studies" / case_id / "derived" / "series"
            derived_dir.mkdir(parents=True, exist_ok=True)
            for name in ("body", "fc30"):
                (derived_dir / f"{name}.nii.gz").write_bytes(name.encode())

            config_path = root / "series_selection.json"
            config_path.write_text(
                json.dumps(
                    {
                        "default_profile": "ct_test",
                        "profiles": {
                            "ct_test": {
                                "required": {"modality": "CT", "min_slices": 60},
                                "hard_reject": {},
                                "phase_priority": ["native"],
                                "text_hints": {"kernel_avoid": ["sharp"], "kernel_prefer": ["body"]},
                                "window_hints": {
                                    "soft_tissue_center_range": [-200, 200],
                                    "soft_tissue_width_range": [200, 800],
                                    "lung_center_max": -300,
                                    "lung_width_min": 1000,
                                },
                                "manufacturer_hints": [
                                    {
                                        "name": "canon",
                                        "manufacturer_contains": ["canon"],
                                        "kernel_avoid": ["fc30"],
                                    }
                                ],
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            common = {
                "Modality": "CT",
                "SliceCount": 200,
                "DetectedPhase": "native",
                "PhaseDetected": True,
                "PhaseData": {"probability": 0.9},
                "Manufacturer": "Canon Medical Systems",
                "SeriesDescription": "CHEST ROUTINE",
            }
            id_data = {
                "AvailableSeries": [
                    {
                        **common,
                        "SeriesInstanceUID": "1.2.3.fc30",
                        "SeriesNumber": "1",
                        "DerivedNiftiPath": "series/fc30.nii.gz",
                        "ConvolutionKernel": "FC30",
                        "WindowCenter": "-700",
                        "WindowWidth": "1700",
                    },
                    {
                        **common,
                        "SeriesInstanceUID": "1.2.3.body",
                        "SeriesNumber": "2",
                        "DerivedNiftiPath": "series/body.nii.gz",
                        "ConvolutionKernel": "BODY_SHARP",
                        "WindowCenter": "40",
                        "WindowWidth": "400",
                    },
                ]
            }

            with patch.object(settings, "STUDIES_DIR", root / "runtime" / "studies"), patch.object(
                settings,
                "SERIES_SELECTION_CONFIG_PATH",
                config_path,
            ):
                selected_path, selection_info = select_prepared_series(case_id, id_data)

        self.assertEqual(selected_path.name, "body.nii.gz")
        self.assertLess(selection_info["SelectedPreferenceScore"], 0)

    def test_soft_tissue_scoring_breaks_residual_coverage_tie(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "case-mediastinum-residual-coverage"
            derived_dir = root / "runtime" / "studies" / case_id / "derived" / "series"
            derived_dir.mkdir(parents=True, exist_ok=True)
            mediastinum_path = derived_dir / "mediastinum.nii.gz"
            lung_path = derived_dir / "lung.nii.gz"
            mediastinum_path.write_bytes(b"mediastinum")
            lung_path.write_bytes(b"lung")

            config_path = root / "series_selection.json"
            config_path.write_text(
                json.dumps(
                    {
                        "default_profile": "ct_test",
                        "profiles": {
                            "ct_test": {
                                "required": {"modality": "CT", "min_slices": 60},
                                "hard_reject": {},
                                "phase_priority": ["native"],
                                "geometry_priority": {
                                    "enabled": True,
                                    "coverage_equivalence_ratio": 0.92,
                                    "coverage_equivalence_mm": 50,
                                    "prefer_thinner_within_equivalent_coverage": True,
                                },
                                "text_hints": {
                                    "description_prefer": ["mediastino"],
                                },
                                "window_hints": {
                                    "soft_tissue_center_range": [-200, 200],
                                    "soft_tissue_width_range": [200, 800],
                                    "lung_center_max": -300,
                                    "lung_width_min": 1000,
                                },
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            common = {
                "Modality": "CT",
                "DetectedPhase": "native",
                "PhaseDetected": True,
                "PhaseData": {"probability": 1.0},
                "ZSpacingMm": 1.0,
            }
            id_data = {
                "AvailableSeries": [
                    {
                        **common,
                        "SeriesInstanceUID": "1.2.3.mediastinum",
                        "SeriesNumber": "2",
                        "DerivedNiftiPath": "series/mediastinum.nii.gz",
                        "SliceCount": 303,
                        "SeriesDescription": "VOL MEDIASTINO",
                        "ConvolutionKernel": ["Br36f", "4"],
                        "WindowCenter": "40",
                        "WindowWidth": "400",
                        "CoverageMm": 302.0,
                        "SliceThicknessMm": 2.0,
                    },
                    {
                        **common,
                        "SeriesInstanceUID": "1.2.3.lung",
                        "SeriesNumber": "3",
                        "DerivedNiftiPath": "series/lung.nii.gz",
                        "SliceCount": 304,
                        "SeriesDescription": "VOL PARENQUIMA",
                        "ConvolutionKernel": ["Br60f", "3"],
                        "WindowCenter": "-600",
                        "WindowWidth": "1200",
                        "CoverageMm": 303.0,
                        "SliceThicknessMm": 1.0,
                    },
                ]
            }

            with patch.object(settings, "STUDIES_DIR", root / "runtime" / "studies"), patch.object(
                settings,
                "SERIES_SELECTION_CONFIG_PATH",
                config_path,
            ):
                selected_path, selection_info = select_prepared_series(case_id, id_data)

        self.assertEqual(selected_path.name, "mediastinum.nii.gz")
        self.assertEqual(selection_info["SelectedWindowClass"], "soft_tissue")
        self.assertEqual(selection_info["SelectedPreferenceScore"], -2)

    def test_external_policy_overrides_min_slices_for_job(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "case-external-series-policy"
            derived_dir = root / "runtime" / "studies" / case_id / "derived" / "series"
            derived_dir.mkdir(parents=True, exist_ok=True)

            selected_path = derived_dir / "selected.nii.gz"
            rejected_path = derived_dir / "rejected.nii.gz"
            selected_path.write_bytes(b"ok")
            rejected_path.write_bytes(b"no")

            config_path = root / "series_selection.json"
            config_path.write_text(
                json.dumps(
                    {
                        "default_profile": "ct_test",
                        "profiles": {
                            "ct_test": {
                                "required": {"modality": "CT", "min_slices": 120},
                                "hard_reject": {},
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
                "ExternalDelivery": {
                    "series_selection_policy": {
                        "name": "orchestrum_ct_opportunistic_v1",
                        "required": {"min_slices": 60},
                    }
                },
                "AvailableSeries": [
                    {
                        "SeriesInstanceUID": "1.2.3.too_short",
                        "SeriesNumber": "1",
                        "DerivedNiftiPath": "series/rejected.nii.gz",
                        "Modality": "CT",
                        "SliceCount": 50,
                        "DetectedPhase": "native",
                        "PhaseDetected": True,
                        "PhaseData": {"probability": 0.95},
                    },
                    {
                        "SeriesInstanceUID": "1.2.3.selected",
                        "SeriesNumber": "2",
                        "DerivedNiftiPath": "series/selected.nii.gz",
                        "Modality": "CT",
                        "SliceCount": 80,
                        "DetectedPhase": "native",
                        "PhaseDetected": True,
                        "PhaseData": {"probability": 0.90},
                    },
                ],
            }

            with patch.object(settings, "STUDIES_DIR", root / "runtime" / "studies"), patch.object(
                settings,
                "SERIES_SELECTION_CONFIG_PATH",
                config_path,
            ):
                selected, selection_info = select_prepared_series(case_id, id_data)

        self.assertEqual(selected.name, "selected.nii.gz")
        self.assertEqual(selection_info["SelectedSeriesInstanceUID"], "1.2.3.selected")
        self.assertEqual(selection_info["PolicySource"], "external_delivery")
        self.assertEqual(selection_info["ExternalPolicyName"], "orchestrum_ct_opportunistic_v1")

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
                                "follow_up_coverage": {
                                    "enabled": True,
                                    "when_previous_coverage": ["chest_only"],
                                    "prefer_region": "abdomen",
                                    "require_different_series": True,
                                },
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
                                "follow_up_coverage": {
                                    "enabled": True,
                                    "when_previous_coverage": ["chest_only"],
                                    "prefer_region": "abdomen",
                                    "require_different_series": True,
                                },
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

    def test_allows_unknown_phase_as_last_priority(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "case-series-selection"
            derived_dir = root / "runtime" / "studies" / case_id / "derived" / "series"
            derived_dir.mkdir(parents=True, exist_ok=True)

            unknown_path = derived_dir / "unknown.nii.gz"
            unknown_path.write_bytes(b"ok")

            config_path = root / "series_selection.json"
            config_path.write_text(
                json.dumps(
                    {
                        "default_profile": "ct_test",
                        "profiles": {
                            "ct_test": {
                                "required": {"modality": "CT", "min_slices": 60},
                                "hard_reject": {},
                                "phase_priority": ["native", "portal_venous", "unknown"],
                                "geometry_priority": {"enabled": True},
                                "text_hints": {"description_avoid": [], "kernel_avoid": []},
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            id_data = {
                "AvailableSeries": [
                    {
                        "SeriesInstanceUID": "1.2.3.unknown",
                        "SeriesNumber": "9",
                        "DerivedNiftiPath": "series/unknown.nii.gz",
                        "Modality": "CT",
                        "SliceCount": 72,
                        "DetectedPhase": "unknown",
                        "PhaseDetected": False,
                        "PhaseData": {},
                        "SeriesDescription": "WRIST 0.6",
                        "ConvolutionKernel": "FC18",
                        "CoverageMm": 42.6,
                        "ZSpacingMm": 0.6,
                        "SliceThicknessMm": 0.6,
                    }
                ]
            }

            with patch.object(settings, "STUDIES_DIR", root / "runtime" / "studies"), patch.object(
                settings,
                "SERIES_SELECTION_CONFIG_PATH",
                config_path,
            ):
                selected_path, selection_info = select_prepared_series(case_id, id_data)

        self.assertEqual(selected_path.name, "unknown.nii.gz")
        self.assertEqual(selection_info["SelectedPhase"], "unknown")
        self.assertEqual(selection_info["SelectedSeriesInstanceUID"], "1.2.3.unknown")

    def test_prefers_maximum_coverage_over_thinner_partial_series(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "case-series-selection"
            derived_dir = root / "runtime" / "studies" / case_id / "derived" / "series"
            derived_dir.mkdir(parents=True, exist_ok=True)

            thin_partial_path = derived_dir / "thin_partial.nii.gz"
            thicker_complete_path = derived_dir / "thicker_complete.nii.gz"
            thin_partial_path.write_bytes(b"thin")
            thicker_complete_path.write_bytes(b"complete")

            config_path = root / "series_selection.json"
            config_path.write_text(
                json.dumps(
                    {
                        "default_profile": "ct_test",
                        "profiles": {
                            "ct_test": {
                                "required": {"modality": "CT", "min_slices": 120},
                                "hard_reject": {},
                                "phase_priority": ["native"],
                                "geometry_priority": {
                                    "enabled": True,
                                    "coverage_equivalence_ratio": 0.92,
                                    "coverage_equivalence_mm": 50,
                                    "prefer_thinner_within_equivalent_coverage": True,
                                },
                                "text_hints": {"description_avoid": [], "kernel_avoid": []},
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            id_data = {
                "AvailableSeries": [
                    {
                        "SeriesInstanceUID": "1.2.3.thin",
                        "SeriesNumber": "2",
                        "DerivedNiftiPath": "series/thin_partial.nii.gz",
                        "Modality": "CT",
                        "SliceCount": 620,
                        "DetectedPhase": "native",
                        "PhaseDetected": True,
                        "PhaseData": {"probability": 0.99},
                        "SeriesDescription": "BODY THIN PARTIAL",
                        "ConvolutionKernel": "FC18",
                        "CoverageMm": 360.0,
                        "ZSpacingMm": 0.6,
                        "SliceThicknessMm": 0.6,
                    },
                    {
                        "SeriesInstanceUID": "1.2.3.complete",
                        "SeriesNumber": "3",
                        "DerivedNiftiPath": "series/thicker_complete.nii.gz",
                        "Modality": "CT",
                        "SliceCount": 360,
                        "DetectedPhase": "native",
                        "PhaseDetected": True,
                        "PhaseData": {"probability": 0.90},
                        "SeriesDescription": "BODY COMPLETE",
                        "ConvolutionKernel": "FC18",
                        "CoverageMm": 900.0,
                        "ZSpacingMm": 2.5,
                        "SliceThicknessMm": 2.5,
                    },
                ]
            }

            with patch.object(settings, "STUDIES_DIR", root / "runtime" / "studies"), patch.object(
                settings,
                "SERIES_SELECTION_CONFIG_PATH",
                config_path,
            ):
                selected_path, selection_info = select_prepared_series(case_id, id_data)

        self.assertEqual(selected_path.name, "thicker_complete.nii.gz")
        self.assertEqual(selection_info["SelectedSeriesInstanceUID"], "1.2.3.complete")
        self.assertTrue(selection_info["GeometryPriorityApplied"])
        self.assertEqual(selection_info["SelectedCoverageMm"], 900.0)

    def test_prefers_thinner_series_when_coverage_is_equivalent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "case-series-selection"
            derived_dir = root / "runtime" / "studies" / case_id / "derived" / "series"
            derived_dir.mkdir(parents=True, exist_ok=True)

            thick_path = derived_dir / "thick.nii.gz"
            thin_path = derived_dir / "thin.nii.gz"
            thick_path.write_bytes(b"thick")
            thin_path.write_bytes(b"thin")

            config_path = root / "series_selection.json"
            config_path.write_text(
                json.dumps(
                    {
                        "default_profile": "ct_test",
                        "profiles": {
                            "ct_test": {
                                "required": {"modality": "CT", "min_slices": 120},
                                "hard_reject": {},
                                "phase_priority": ["native"],
                                "geometry_priority": {
                                    "enabled": True,
                                    "coverage_equivalence_ratio": 0.92,
                                    "coverage_equivalence_mm": 50,
                                    "prefer_thinner_within_equivalent_coverage": True,
                                },
                                "text_hints": {"description_avoid": [], "kernel_avoid": []},
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            id_data = {
                "AvailableSeries": [
                    {
                        "SeriesInstanceUID": "1.2.3.thick",
                        "SeriesNumber": "2",
                        "DerivedNiftiPath": "series/thick.nii.gz",
                        "Modality": "CT",
                        "SliceCount": 400,
                        "DetectedPhase": "native",
                        "PhaseDetected": True,
                        "PhaseData": {"probability": 0.99},
                        "SeriesDescription": "BODY COMPLETE",
                        "ConvolutionKernel": "FC18",
                        "CoverageMm": 1000.0,
                        "ZSpacingMm": 2.5,
                        "SliceThicknessMm": 2.5,
                    },
                    {
                        "SeriesInstanceUID": "1.2.3.thin",
                        "SeriesNumber": "3",
                        "DerivedNiftiPath": "series/thin.nii.gz",
                        "Modality": "CT",
                        "SliceCount": 1200,
                        "DetectedPhase": "native",
                        "PhaseDetected": True,
                        "PhaseData": {"probability": 0.90},
                        "SeriesDescription": "BODY THIN COMPLETE",
                        "ConvolutionKernel": "FC18",
                        "CoverageMm": 960.0,
                        "ZSpacingMm": 0.8,
                        "SliceThicknessMm": 0.8,
                    },
                ]
            }

            with patch.object(settings, "STUDIES_DIR", root / "runtime" / "studies"), patch.object(
                settings,
                "SERIES_SELECTION_CONFIG_PATH",
                config_path,
            ):
                selected_path, selection_info = select_prepared_series(case_id, id_data)

        self.assertEqual(selected_path.name, "thin.nii.gz")
        self.assertEqual(selection_info["SelectedSeriesInstanceUID"], "1.2.3.thin")
        self.assertTrue(selection_info["GeometryPriorityApplied"])
        self.assertEqual(selection_info["SelectedEffectiveThicknessMm"], 0.8)

    def test_uses_legacy_ranking_when_geometry_is_unavailable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "case-series-selection"
            derived_dir = root / "runtime" / "studies" / case_id / "derived" / "series"
            derived_dir.mkdir(parents=True, exist_ok=True)

            fewer_slices_path = derived_dir / "fewer.nii.gz"
            more_slices_path = derived_dir / "more.nii.gz"
            fewer_slices_path.write_bytes(b"fewer")
            more_slices_path.write_bytes(b"more")

            config_path = root / "series_selection.json"
            config_path.write_text(
                json.dumps(
                    {
                        "default_profile": "ct_test",
                        "profiles": {
                            "ct_test": {
                                "required": {"modality": "CT", "min_slices": 120},
                                "hard_reject": {},
                                "phase_priority": ["native"],
                                "geometry_priority": {"enabled": True},
                                "text_hints": {"description_avoid": [], "kernel_avoid": []},
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            id_data = {
                "AvailableSeries": [
                    {
                        "SeriesInstanceUID": "1.2.3.fewer",
                        "SeriesNumber": "2",
                        "DerivedNiftiPath": "series/fewer.nii.gz",
                        "Modality": "CT",
                        "SliceCount": 180,
                        "DetectedPhase": "native",
                        "PhaseDetected": True,
                        "PhaseData": {"probability": 0.95},
                        "SeriesDescription": "BODY",
                        "ConvolutionKernel": "FC18",
                    },
                    {
                        "SeriesInstanceUID": "1.2.3.more",
                        "SeriesNumber": "3",
                        "DerivedNiftiPath": "series/more.nii.gz",
                        "Modality": "CT",
                        "SliceCount": 260,
                        "DetectedPhase": "native",
                        "PhaseDetected": True,
                        "PhaseData": {"probability": 0.95},
                        "SeriesDescription": "BODY",
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

        self.assertEqual(selected_path.name, "more.nii.gz")
        self.assertEqual(selection_info["SelectedSeriesInstanceUID"], "1.2.3.more")
        self.assertFalse(selection_info["GeometryPriorityApplied"])

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
                                "follow_up_coverage": {
                                    "enabled": True,
                                    "when_previous_coverage": ["chest_only"],
                                    "prefer_region": "abdomen",
                                    "require_different_series": True,
                                },
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
        self.assertIn("follow_up_policy=abdomen", selection_info["SelectionReason"])
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
