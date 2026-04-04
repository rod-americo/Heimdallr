import unittest

import numpy as np

from heimdallr.segmentation.body_fat import (
    build_abdominal_slabs,
    calculate_body_fat_distribution,
    compute_l3_slice_fat_areas,
)


class TestBodyFatHelpers(unittest.TestCase):
    def test_build_abdominal_slabs_complete_coverage_uses_midpoints(self):
        vertebrae = {}
        for index, level in enumerate(("T12", "L1", "L2", "L3", "L4", "L5")):
            mask = np.zeros((8, 8, 24), dtype=bool)
            center = 3 + index * 3
            mask[:, :, center - 1 : center + 1] = True
            vertebrae[level] = mask

        slab_definition = build_abdominal_slabs(vertebrae, z_size=24)

        self.assertTrue(slab_definition["coverage_complete"])
        self.assertEqual(slab_definition["strategy"], "centroid_midpoint")
        self.assertEqual(slab_definition["slabs"]["L3"]["start_slice"], 10)
        self.assertEqual(slab_definition["slabs"]["L3"]["end_slice"], 12)

    def test_build_abdominal_slabs_partial_coverage_falls_back_to_extent(self):
        vertebrae = {}
        for level, start, end in (("L2", 8, 10), ("L3", 12, 14), ("L4", 16, 18)):
            mask = np.zeros((8, 8, 24), dtype=bool)
            mask[:, :, start : end + 1] = True
            vertebrae[level] = mask

        slab_definition = build_abdominal_slabs(vertebrae, z_size=24)

        self.assertFalse(slab_definition["coverage_complete"])
        self.assertEqual(slab_definition["strategy"], "mask_extent_fallback")
        self.assertEqual(slab_definition["missing_levels"], ["T12", "L1", "L5"])
        self.assertEqual(slab_definition["slabs"]["L3"]["start_slice"], 12)
        self.assertEqual(slab_definition["slabs"]["L3"]["end_slice"], 14)

    def test_calculate_distribution_and_l3_slice(self):
        sat_mask = np.zeros((12, 12, 24), dtype=bool)
        torso_mask = np.zeros_like(sat_mask)
        sat_mask[:, :, 6:18] = True
        torso_mask[3:9, 3:9, 6:18] = True

        vertebrae = {}
        for level, center in (("T12", 6), ("L1", 8), ("L2", 10), ("L3", 12), ("L4", 14), ("L5", 16)):
            mask = np.zeros_like(sat_mask)
            mask[4:8, 4:8, center - 1 : center + 2] = True
            vertebrae[level] = mask

        slab_definition = build_abdominal_slabs(vertebrae, z_size=sat_mask.shape[2])
        distribution = calculate_body_fat_distribution(
            subcutaneous_fat_mask=sat_mask,
            torso_fat_mask=torso_mask,
            spacing_mm=(1.0, 1.0, 2.0),
            slab_definition=slab_definition,
        )

        aggregate = distribution["aggregate"]
        self.assertTrue(aggregate["coverage_complete"])
        self.assertGreater(aggregate["subcutaneous_fat_cm3"], aggregate["torso_fat_cm3"])
        self.assertGreater(aggregate["torso_to_subcutaneous_ratio"], 0.0)

        l3 = compute_l3_slice_fat_areas(
            vertebra_l3_mask=vertebrae["L3"],
            subcutaneous_fat_mask=sat_mask,
            torso_fat_mask=torso_mask,
            spacing_mm=(1.0, 1.0),
        )
        self.assertEqual(l3["status"], "done")
        self.assertEqual(l3["slice_index"], 12)
        self.assertGreater(l3["subcutaneous_fat_area_cm2"], l3["torso_fat_area_cm2"])


if __name__ == "__main__":
    unittest.main()
