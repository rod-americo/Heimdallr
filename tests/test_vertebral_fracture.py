import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

from heimdallr.metrics.analysis.vertebral_fracture import (
    classify_fracture_pattern,
    isolate_vertebral_body,
    refine_classification_with_adjacent_reference,
    screen_vertebral_fracture,
    estimate_vertebral_heights,
    vertebra_level_index,
)
from heimdallr.metrics.jobs.vertebral_fracture_screen import discover_available_vertebrae


def make_profiled_vertebra(
    heights: np.ndarray,
    widths: np.ndarray | None = None,
    lateral_size: int = 18,
    si_size: int = 28,
) -> np.ndarray:
    ap_len = int(len(heights))
    if widths is None:
        widths = np.full(ap_len, max(4, lateral_size // 2), dtype=int)
    else:
        widths = np.asarray(widths, dtype=int)

    mask = np.zeros((lateral_size, ap_len, si_size), dtype=bool)
    for ap_idx, (height, width) in enumerate(zip(np.asarray(heights, dtype=int), widths, strict=True)):
        height = int(max(1, min(height, si_size)))
        width = int(max(1, min(width, lateral_size)))
        lat_start = max(0, (lateral_size - width) // 2)
        lat_end = min(lateral_size, lat_start + width)
        si_start = max(0, (si_size - height) // 2)
        si_end = min(si_size, si_start + height)
        mask[lat_start:lat_end, ap_idx, si_start:si_end] = True
    return mask


class TestVertebralFractureHelpers(unittest.TestCase):
    def test_discover_available_vertebrae_sorts_all_segmented_levels(self):
        with TemporaryDirectory() as tmp_dir:
            total_dir = Path(tmp_dir) / "total"
            total_dir.mkdir(parents=True, exist_ok=True)
            for filename in (
                "vertebrae_L1.nii.gz",
                "vertebrae_T12.nii.gz",
                "vertebrae_C7.nii.gz",
                "vertebrae_S1.nii.gz",
                "vertebrae_T1.nii.gz",
                "vertebrae_X1.nii.gz",
            ):
                (total_dir / filename).touch()

            discovered = discover_available_vertebrae(Path(tmp_dir))

        self.assertEqual(discovered, ["C7", "T1", "T12", "L1", "S1", "X1"])

    def test_vertebra_level_index_supports_cervical_to_sacral_levels(self):
        self.assertLess(vertebra_level_index("C7"), vertebra_level_index("T1"))
        self.assertLess(vertebra_level_index("T12"), vertebra_level_index("L1"))
        self.assertLess(vertebra_level_index("L5"), vertebra_level_index("S1"))
        self.assertLess(vertebra_level_index("S3"), vertebra_level_index("S4"))

    def test_refine_classification_uses_adjacent_normal_vertebrae(self):
        refined = refine_classification_with_adjacent_reference(
            {
                "T12": {
                    "status": "no_suspicion",
                    "screen_label": "grade_0",
                    "genant_label": "grade_0",
                    "genant_grade": 0,
                    "severity": "none",
                    "suspected_pattern": None,
                    "morphometry": {
                        "anterior_height_mm": 20.0,
                        "middle_height_mm": 20.0,
                        "posterior_height_mm": 20.0,
                    },
                    "ratios": {},
                    "qc_flags": [],
                },
                "L1": {
                    "status": "no_suspicion",
                    "screen_label": "grade_0",
                    "genant_label": "grade_0",
                    "genant_grade": 0,
                    "severity": "none",
                    "suspected_pattern": "wedge",
                    "morphometry": {
                        "anterior_height_mm": 15.0,
                        "middle_height_mm": 18.0,
                        "posterior_height_mm": 20.0,
                    },
                    "ratios": {"height_loss_ratio_percent": 25.0},
                    "qc_flags": [],
                },
                "L2": {
                    "status": "no_suspicion",
                    "screen_label": "grade_0",
                    "genant_label": "grade_0",
                    "genant_grade": 0,
                    "severity": "none",
                    "suspected_pattern": None,
                    "morphometry": {
                        "anterior_height_mm": 20.0,
                        "middle_height_mm": 20.0,
                        "posterior_height_mm": 20.0,
                    },
                    "ratios": {},
                    "qc_flags": [],
                },
            }
        )

        self.assertEqual(refined["L1"]["status"], "suspected")
        self.assertEqual(refined["L1"]["genant_grade"], 2)
        self.assertEqual(refined["L1"]["screen_label"], "grade_2")
        self.assertAlmostEqual(refined["L1"]["ratios"]["adjacent_reference_height_mm"], 20.0)
        self.assertAlmostEqual(refined["L1"]["ratios"]["height_loss_ratio_percent"], 25.0)
        self.assertEqual(refined["L1"]["ratios"]["adjacent_reference_levels"], ["T12", "L2"])

    def test_isolate_vertebral_body_removes_thin_appendage(self):
        mask = np.zeros((20, 16, 24), dtype=bool)
        mask[4:14, 3:13, 5:19] = True
        mask[13:17, 11:15, 10:13] = True

        result = isolate_vertebral_body(mask, spacing_mm=(1.0, 1.0, 1.0))

        self.assertEqual(result["body_mask"].shape, mask.shape)
        self.assertGreater(result["original_voxels"], result["body_voxels"])
        self.assertLess(result["body_fraction"], 1.0)
        self.assertTrue(np.any(result["body_mask"]))

    def test_estimate_heights_detects_wedge_with_explicit_axes(self):
        ap_len = 14
        heights = np.linspace(10, 20, ap_len)
        widths = np.linspace(6, 10, ap_len)
        mask = make_profiled_vertebra(heights=heights, widths=widths)

        result = estimate_vertebral_heights(mask, spacing_mm=(1.0, 1.0, 1.0), ap_axis=1, si_axis=2, body_mask=mask)

        self.assertIsNotNone(result["anterior_height_mm"])
        self.assertIsNotNone(result["posterior_height_mm"])
        self.assertLess(result["anterior_height_mm"], result["posterior_height_mm"])
        self.assertGreater(result["posterior_height_mm"], 0)

        classification = classify_fracture_pattern(result)
        self.assertEqual(classification["screen_label"], "grade_3")
        self.assertEqual(classification["genant_grade"], 3)
        self.assertEqual(classification["severity"], "severe")
        self.assertEqual(classification["suspected_pattern"], "wedge")

    def test_estimate_heights_ignores_empty_padding_around_vertebra(self):
        ap_len = 14
        heights = np.linspace(10, 20, ap_len)
        widths = np.linspace(6, 10, ap_len)
        mask = make_profiled_vertebra(heights=heights, widths=widths)
        padded = np.zeros((48, 64, 64), dtype=bool)
        padded[12:30, 20:34, 18:46] = np.moveaxis(mask, (0, 1, 2), (0, 1, 2))

        result = estimate_vertebral_heights(
            padded,
            spacing_mm=(1.0, 1.0, 1.0),
            ap_axis=1,
            si_axis=2,
            body_mask=padded,
        )

        self.assertGreater(result["anterior_height_mm"], 0.0)
        self.assertGreater(result["middle_height_mm"], 0.0)
        self.assertGreater(result["posterior_height_mm"], 0.0)
        self.assertGreater(result["ap_depth_mm"], 0.0)

    def test_classify_fracture_pattern_covers_biconcave_and_crush(self):
        biconcave = classify_fracture_pattern(
            {
                "anterior_height_mm": 20.0,
                "middle_height_mm": 12.0,
                "posterior_height_mm": 19.0,
                "ap_depth_mm": 32.0,
                "orientation_confidence": 0.9,
                "qc_flags": [],
            }
        )
        self.assertEqual(biconcave["screen_label"], "grade_2")
        self.assertEqual(biconcave["genant_grade"], 2)
        self.assertEqual(biconcave["severity"], "moderate")
        self.assertEqual(biconcave["suspected_pattern"], "biconcave")

        crush = classify_fracture_pattern(
            {
                "anterior_height_mm": 8.0,
                "middle_height_mm": 8.5,
                "posterior_height_mm": 10.5,
                "ap_depth_mm": 20.0,
                "orientation_confidence": 0.9,
                "qc_flags": [],
            }
        )
        self.assertEqual(crush["screen_label"], "grade_1")
        self.assertEqual(crush["genant_grade"], 1)
        self.assertEqual(crush["severity"], "mild")
        self.assertEqual(crush["suspected_pattern"], "crush")

    def test_screen_vertebral_fracture_returns_indeterminate_for_empty_mask(self):
        result = screen_vertebral_fracture(np.zeros((8, 8, 8), dtype=bool))

        self.assertEqual(result["status"], "indeterminate")
        self.assertEqual(result["screen_label"], "indeterminate")
        self.assertIn("empty_mask", result["qc_flags"])

    def test_screen_vertebral_fracture_returns_no_suspicion_for_clean_mask(self):
        ap_len = 16
        heights = np.full(ap_len, 18)
        widths = np.full(ap_len, 18)
        mask = make_profiled_vertebra(heights=heights, widths=widths, lateral_size=28, si_size=28)

        result = screen_vertebral_fracture(mask, spacing_mm=(1.0, 1.0, 1.0))

        self.assertEqual(result["status"], "no_suspicion")
        self.assertEqual(result["screen_label"], "grade_0")
        self.assertEqual(result["genant_grade"], 0)
        self.assertEqual(result["severity"], "none")
        self.assertIsNone(result["suspected_pattern"])
        self.assertIn("no_qualifying_genant_deformity", result["qc_flags"])

    def test_screen_vertebral_fracture_full_pipeline(self):
        ap_len = 16
        heights = np.linspace(11, 21, ap_len)
        widths = np.linspace(14, 24, ap_len)
        mask = make_profiled_vertebra(heights=heights, widths=widths, lateral_size=30, si_size=30)

        result = screen_vertebral_fracture(mask, spacing_mm=(1.0, 1.0, 1.0))

        self.assertEqual(result["job_name"], "vertebral_fracture_screen")
        self.assertEqual(result["screen_label"], "grade_2")
        self.assertEqual(result["genant_grade"], 2)
        self.assertEqual(result["severity"], "moderate")
        self.assertEqual(result["suspected_pattern"], "wedge")
        self.assertGreater(result["screen_confidence"], 0.0)
        self.assertGreater(result["morphometry"]["posterior_height_mm"], result["morphometry"]["anterior_height_mm"])


if __name__ == "__main__":
    unittest.main()
