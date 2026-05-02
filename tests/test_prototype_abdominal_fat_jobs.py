import json
import tempfile
import unittest
from pathlib import Path

import nibabel as nib
import numpy as np

try:
    from scripts import prototype_abdominal_fat_jobs as fat_jobs
except ImportError as exc:  # pragma: no cover - documents removed prototype
    if "prototype_abdominal_fat_jobs" not in str(exc):
        raise
    raise unittest.SkipTest("scripts.prototype_abdominal_fat_jobs is not part of the current repository")


class TestPrototypeAbdominalFatJobs(unittest.TestCase):
    def test_synthetic_case_generates_expected_jobs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            study_dir = base / "SyntheticCase"
            tissue_dir = study_dir / "artifacts" / "tissue_types"
            total_dir = study_dir / "artifacts" / "total"
            derived_dir = study_dir / "derived"
            metadata_dir = study_dir / "metadata"
            tissue_dir.mkdir(parents=True, exist_ok=True)
            total_dir.mkdir(parents=True, exist_ok=True)
            derived_dir.mkdir(parents=True, exist_ok=True)
            metadata_dir.mkdir(parents=True, exist_ok=True)

            ct, ct_nii, subcutaneous, torso, vertebrae = fat_jobs._generate_synthetic_case()
            nib.save(ct_nii, derived_dir / "SyntheticCase.nii.gz")
            nib.save(nib.Nifti1Image(subcutaneous.astype(np.uint8), affine=np.eye(4)), tissue_dir / "subcutaneous_fat.nii.gz")
            nib.save(nib.Nifti1Image(torso.astype(np.uint8), affine=np.eye(4)), tissue_dir / "torso_fat.nii.gz")
            for level, mask in vertebrae.items():
                nib.save(nib.Nifti1Image(mask.astype(np.uint8), affine=np.eye(4)), total_dir / f"vertebrae_{level}.nii.gz")

            (metadata_dir / "id.json").write_text(
                json.dumps({"CaseID": "SyntheticCase", "Modality": "CT", "SliceThickness": 1.0}),
                encoding="utf-8",
            )

            outputs = fat_jobs.run_case(study_dir, base / "out")

            volumetry = outputs["abdominal_fat_t12_l5_volumetry"]
            self.assertEqual(volumetry["status"], "done")
            self.assertTrue(volumetry["qc"]["coverage_complete"])
            aggregate = volumetry["measurement"]["aggregate"]
            self.assertGreater(aggregate["subcutaneous_fat_volume_cm3"], 0.0)
            self.assertGreater(aggregate["torso_fat_volume_cm3"], 0.0)
            self.assertEqual(aggregate["levels_included"], fat_jobs.TARGET_LEVELS)

            l3_reference = outputs["abdominal_fat_l3_reference"]
            self.assertEqual(l3_reference["status"], "done")
            self.assertGreater(l3_reference["measurement"]["subcutaneous_fat_area_cm2"], 0.0)
            self.assertGreater(l3_reference["measurement"]["torso_fat_area_cm2"], 0.0)

            summary = outputs["abdominal_fat_summary"]
            self.assertEqual(summary["status"], "done")
            self.assertFalse(summary["qc"]["needs_manual_review"])

    def test_volumetry_marks_missing_levels(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            ct, ct_nii, subcutaneous, torso, vertebrae = fat_jobs._generate_synthetic_case()
            result = fat_jobs._abdominal_fat_t12_l5_volumetry(
                case_id="synthetic",
                study_dir=base,
                subcutaneous_mask=subcutaneous,
                torso_mask=torso,
                tissue_nii=ct_nii,
                level_paths={},
                out_dir=base / "out",
                study_meta={"modality": "CT"},
            )
            self.assertEqual(result["status"], "missing")
            self.assertIn("T12", result["qc"]["missing_levels"])
            self.assertTrue(result["qc"]["needs_manual_review"])


if __name__ == "__main__":
    unittest.main()
