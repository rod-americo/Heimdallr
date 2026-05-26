import unittest

import numpy as np

from heimdallr.metrics.jobs._appendicular_exclusion import remove_appendicular_tissue_components


class TestAppendicularExclusion(unittest.TestCase):
    def test_removes_only_component_touching_appendicular_support(self):
        tissue = np.zeros((12, 12), dtype=bool)
        tissue[5:8, 5:8] = True
        tissue[1:3, 1:3] = True
        appendicular = np.zeros_like(tissue)
        appendicular[1, 1] = True

        cleaned, audit = remove_appendicular_tissue_components(
            tissue,
            appendicular,
            spacing_mm=(1.0, 1.0),
            tissue_label="subcutaneous_fat",
            margin_mm=1.5,
            max_removed_fraction=0.45,
        )

        self.assertTrue(audit["applied"])
        self.assertEqual(audit["excluded_pixels"], 4)
        self.assertEqual(int(np.count_nonzero(cleaned)), 9)
        self.assertTrue(cleaned[5:8, 5:8].all())
        self.assertFalse(cleaned[1:3, 1:3].any())

    def test_safety_limit_keeps_original_mask(self):
        tissue = np.zeros((8, 8), dtype=bool)
        tissue[1:5, 1:5] = True
        appendicular = np.zeros_like(tissue)
        appendicular[2, 2] = True

        cleaned, audit = remove_appendicular_tissue_components(
            tissue,
            appendicular,
            spacing_mm=(1.0, 1.0),
            tissue_label="skeletal_muscle",
            margin_mm=2.0,
            max_removed_fraction=0.25,
        )

        self.assertFalse(audit["applied"])
        self.assertEqual(audit["reason"], "candidate_removal_exceeds_safety_limit")
        self.assertTrue(np.array_equal(cleaned, tissue))

    def test_missing_appendicular_mask_keeps_original_mask(self):
        tissue = np.zeros((8, 8), dtype=bool)
        tissue[1:3, 1:3] = True
        appendicular = np.zeros_like(tissue)

        cleaned, audit = remove_appendicular_tissue_components(
            tissue,
            appendicular,
            spacing_mm=(1.0, 1.0),
            tissue_label="torso_fat",
            margin_mm=2.0,
        )

        self.assertFalse(audit["applied"])
        self.assertEqual(audit["reason"], "no_appendicular_mask_on_slice")
        self.assertTrue(np.array_equal(cleaned, tissue))


if __name__ == "__main__":
    unittest.main()
