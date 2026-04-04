import unittest

import numpy as np

from heimdallr.metrics.jobs.l3_muscle_area import render_overlay_rgb, sagittal_plane_from_mask


class TestL3MuscleAreaJob(unittest.TestCase):
    def test_sagittal_plane_from_mask_prefers_narrower_lateral_span(self):
        mask = np.zeros((12, 18, 10), dtype=bool)
        mask[4:7, 3:13, 2:8] = True

        plane, index, axis = sagittal_plane_from_mask(mask)

        self.assertEqual(axis, "x")
        self.assertEqual(index, 5)
        self.assertEqual(plane.shape, (18, 10))
        self.assertTrue(plane.any())

    def test_render_overlay_rgb_returns_combined_axial_and_sagittal_image(self):
        image = np.linspace(-200.0, 250.0, num=24 * 20 * 16, dtype=np.float32).reshape((24, 20, 16))
        l3_mask = np.zeros_like(image, dtype=bool)
        muscle_mask = np.zeros_like(image, dtype=bool)
        l3_mask[8:14, 5:15, 6:10] = True
        muscle_mask[9:13, 7:13, 8] = True

        rendered = render_overlay_rgb(
            image_data=image,
            l3_mask=l3_mask,
            muscle_mask=muscle_mask,
            slice_idx=8,
            title="L3 Center Slice",
            summary_lines=["SMA: 42.0 cm2", "Slice: 8"],
            spacing_mm=(1.0, 1.0, 2.5),
        )

        self.assertEqual(rendered.ndim, 3)
        self.assertEqual(rendered.shape[2], 3)
        self.assertGreater(rendered.shape[1], rendered.shape[0])
        self.assertGreater(int(rendered.max()), int(rendered.min()))


if __name__ == "__main__":
    unittest.main()
