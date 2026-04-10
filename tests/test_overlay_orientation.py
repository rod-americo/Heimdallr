import unittest

import numpy as np

from heimdallr.metrics.jobs._bone_job_common import reorient_display_array, reorient_display_spacing_mm


class TestOverlayOrientation(unittest.TestCase):
    def test_reorient_axial_ras_uses_radiological_left_right_convention(self):
        plane = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.int16)

        display = reorient_display_array(
            plane,
            source_axis_codes=("R", "A"),
            desired_row_code="P",
            desired_col_code="L",
        )

        expected = np.array([[6, 3], [5, 2], [4, 1]], dtype=np.int16)
        self.assertTrue(np.array_equal(display, expected))

    def test_reorient_axial_lps_uses_radiological_left_right_convention(self):
        plane = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.int16)

        display = reorient_display_array(
            plane,
            source_axis_codes=("L", "P"),
            desired_row_code="P",
            desired_col_code="L",
        )

        expected = np.array([[1, 4], [2, 5], [3, 6]], dtype=np.int16)
        self.assertTrue(np.array_equal(display, expected))

    def test_reorient_sagittal_keeps_posterior_on_right_for_ps_planes(self):
        plane = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.int16)

        display = reorient_display_array(
            plane,
            source_axis_codes=("P", "S"),
            desired_row_code="I",
            desired_col_code="P",
        )

        expected = np.array([[3, 6], [2, 5], [1, 4]], dtype=np.int16)
        self.assertTrue(np.array_equal(display, expected))

    def test_reorient_spacing_swaps_axes_when_plane_is_transposed(self):
        spacing = reorient_display_spacing_mm(
            (0.8, 2.5),
            source_axis_codes=("P", "S"),
            desired_row_code="I",
            desired_col_code="P",
        )

        self.assertEqual(spacing, (2.5, 0.8))


if __name__ == "__main__":
    unittest.main()
