import unittest

import numpy as np

from heimdallr.metrics.jobs.l3_muscle_area import (
    MetricSkip,
    build_skip_payload,
    centered_slab_bounds,
    compute_center_slice,
    render_overlay_rgb,
    sagittal_plane_from_mask,
    sagittal_slab_from_mask,
)


class TestL3MuscleAreaJob(unittest.TestCase):
    def test_centered_slab_bounds_prefers_odd_slice_count(self):
        start, end = centered_slab_bounds(center_index=10, axis_len=40, spacing_mm=1.0, slab_thickness_mm=3.0)

        self.assertEqual((start, end), (9, 12))

    def test_sagittal_plane_from_mask_uses_left_right_axis(self):
        mask = np.zeros((12, 18, 10), dtype=bool)
        mask[4:7, 3:13, 2:8] = True

        plane, index, axis = sagittal_plane_from_mask(mask)

        self.assertEqual(axis, "x")
        self.assertEqual(index, 5)
        self.assertEqual(plane.shape, (18, 10))
        self.assertTrue(plane.any())

    def test_sagittal_plane_from_mask_still_uses_x_for_wide_vertebra(self):
        mask = np.zeros((20, 12, 14), dtype=bool)
        mask[4:16, 5:8, 3:11] = True

        plane, index, axis = sagittal_plane_from_mask(mask)

        self.assertEqual(axis, "x")
        self.assertEqual(index, 10)
        self.assertEqual(plane.shape, (12, 14))
        self.assertTrue(plane.any())

    def test_sagittal_slab_from_mask_projects_three_millimeter_slab(self):
        image = np.zeros((12, 18, 10), dtype=np.float32)
        mask = np.zeros_like(image, dtype=bool)
        mask[4:7, 3:13, 2:8] = True

        _, plane_index, axis = sagittal_plane_from_mask(mask)
        sagittal_ct, sagittal_mask, slab_bounds, lateral_spacing = sagittal_slab_from_mask(
            image_data=image,
            mask=mask,
            plane_index=plane_index,
            axis=axis,
            spacing_mm=(1.0, 1.0, 2.5),
            slab_thickness_mm=3.0,
        )

        self.assertEqual(slab_bounds, (4, 7))
        self.assertEqual(lateral_spacing, 1.0)
        self.assertEqual(sagittal_ct.shape, (18, 10))
        self.assertEqual(sagittal_mask.shape, (18, 10))
        self.assertTrue(sagittal_mask.any())

    def test_sagittal_slab_from_mask_keeps_full_width_for_y_axis(self):
        image = np.zeros((20, 12, 14), dtype=np.float32)
        mask = np.zeros_like(image, dtype=bool)
        mask[4:16, 5:8, 3:11] = True

        sagittal_ct, sagittal_mask, slab_bounds, lateral_spacing = sagittal_slab_from_mask(
            image_data=image,
            mask=mask,
            plane_index=6,
            axis="y",
            spacing_mm=(1.0, 1.0, 2.5),
            slab_thickness_mm=3.0,
        )

        self.assertEqual(slab_bounds, (5, 8))
        self.assertEqual(lateral_spacing, 1.0)
        self.assertEqual(sagittal_ct.shape, (20, 14))
        self.assertEqual(sagittal_mask.shape, (20, 14))
        self.assertTrue(sagittal_mask.any())

    def test_compute_center_slice_raises_skip_when_l3_is_empty(self):
        mask = np.zeros((12, 18, 10), dtype=bool)

        with self.assertRaisesRegex(MetricSkip, "L3 mask is empty"):
            compute_center_slice(mask)

    def test_build_skip_payload_marks_job_as_skipped(self):
        payload = build_skip_payload(
            case_id="Case1",
            reason="L3 mask not available for this study",
            result_relpath="artifacts/metrics/l3_muscle_area/result.json",
            inputs={
                "canonical_nifti": "derived/case.nii.gz",
                "vertebra_l3_mask": "artifacts/total/vertebrae_L3.nii.gz",
                "skeletal_muscle_mask": "artifacts/tissue_types/skeletal_muscle.nii.gz",
            },
        )

        self.assertEqual(payload["status"], "skipped")
        self.assertEqual(payload["measurement"]["job_status"], "skipped")
        self.assertEqual(payload["skip_reason"], "L3 mask not available for this study")
        self.assertEqual(
            payload["artifacts"]["result_json"],
            "artifacts/metrics/l3_muscle_area/result.json",
        )

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
            panel_titles=("Axial", "Sagittal Reference"),
            spacing_mm=(1.0, 1.0, 2.5),
        )

        self.assertEqual(rendered.ndim, 3)
        self.assertEqual(rendered.shape[2], 3)
        self.assertGreater(rendered.shape[1], rendered.shape[0])
        self.assertGreater(int(rendered.max()), int(rendered.min()))


if __name__ == "__main__":
    unittest.main()
