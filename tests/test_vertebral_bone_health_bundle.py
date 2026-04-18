from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nibabel as nib
import numpy as np

from heimdallr.metrics.jobs.tests.vertebral_bone_health_bundle import (
    _build_attenuation_variants,
    _measure_complete_vertebra,
)


class VertebralBoneHealthBundleTests(unittest.TestCase):
    def test_measure_complete_vertebra_only_includes_complete_masks(self):
        ct = np.full((16, 16, 16), 120.0, dtype=np.float32)
        mask = np.zeros_like(ct, dtype=np.uint8)
        mask[4:12, 4:12, 4:12] = 1

        with tempfile.TemporaryDirectory() as tmp_dir:
            mask_path = Path(tmp_dir) / "vertebrae_L2.nii.gz"
            nib.save(nib.Nifti1Image(mask, affine=np.eye(4)), mask_path)
            measurement = _measure_complete_vertebra(
                vertebra="L2",
                ct_data=ct,
                mask_path=mask_path,
            )

        self.assertEqual(measurement["status"], "done")
        self.assertTrue(measurement["mask_complete"])
        self.assertTrue(measurement["included"])
        self.assertEqual(measurement["full_hu_mean"], 120.0)
        self.assertEqual(measurement["method"], "full_mask_no_erosion")

    def test_measure_complete_vertebra_excludes_truncated_mask(self):
        ct = np.full((16, 16, 16), 80.0, dtype=np.float32)
        mask = np.zeros_like(ct, dtype=np.uint8)
        mask[4:12, 4:12, 0:8] = 1

        with tempfile.TemporaryDirectory() as tmp_dir:
            mask_path = Path(tmp_dir) / "vertebrae_L3.nii.gz"
            nib.save(nib.Nifti1Image(mask, affine=np.eye(4)), mask_path)
            measurement = _measure_complete_vertebra(
                vertebra="L3",
                ct_data=ct,
                mask_path=mask_path,
            )

        self.assertEqual(measurement["status"], "done")
        self.assertFalse(measurement["mask_complete"])
        self.assertFalse(measurement["included"])
        self.assertEqual(measurement["full_hu_mean"], 80.0)

    def test_build_attenuation_variants_uses_requested_labels(self):
        ct = np.full((20, 20, 20), 100.0, dtype=np.float32)
        mask = np.zeros_like(ct, dtype=np.uint8)
        mask[5:15, 5:15, 5:15] = 1

        with tempfile.TemporaryDirectory() as tmp_dir:
            mask_path = Path(tmp_dir) / "vertebrae_L1.nii.gz"
            nib.save(nib.Nifti1Image(mask, affine=np.eye(4)), mask_path)
            variants = _build_attenuation_variants(
                vertebra="L1",
                ct_data=ct,
                mask_path=mask_path,
                spacing_mm=(1.0, 1.0, 1.0),
            )

        self.assertEqual([item["label"] for item in variants], ["L1", "L1_1", "L1_2", "L1_3", "L1_4", "L1_5"])
        self.assertTrue(all(item["method"] == "3d_volume_attenuation_mean" for item in variants))
        self.assertEqual(variants[0]["mean_hu"], 100.0)


if __name__ == "__main__":
    unittest.main()
