import unittest
import tempfile
from pathlib import Path

import nibabel as nib
import numpy as np

from heimdallr.metrics.jobs._appendicular_exclusion import (
    load_upper_appendicular_mask_slice,
    remove_appendicular_tissue_components,
)


def write_nifti(path: Path, data: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    nib.save(nib.Nifti1Image(data.astype(np.float32), np.eye(4)), str(path))


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

    def test_load_mask_slice_projects_appendicular_mask_from_nearby_slice(self):
        with tempfile.TemporaryDirectory() as tmp:
            artifacts_dir = Path(tmp) / "artifacts"
            mask = np.zeros((8, 8, 12), dtype=np.float32)
            mask[2, 3, 8] = 1.0
            write_nifti(artifacts_dir / "total" / "humerus_right.nii.gz", mask)

            projected, audit = load_upper_appendicular_mask_slice(
                artifacts_dir,
                reference_shape=mask.shape,
                slice_idx=5,
                spacing_z_mm=2.0,
                slice_half_window_mm=6.0,
            )

            self.assertTrue(projected[2, 3])
            self.assertEqual(audit["projection"]["slice_start"], 2)
            self.assertEqual(audit["projection"]["slice_end_exclusive"], 9)


if __name__ == "__main__":
    unittest.main()
