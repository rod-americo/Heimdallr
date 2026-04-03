import json
import sys
import tempfile
import unittest
from pathlib import Path

import nibabel as nib
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.metrics import calculate_all_metrics  # noqa: E402


def write_nifti(path: Path, data: np.ndarray, spacing=(1.0, 1.0, 1.0)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    affine = np.diag([spacing[0], spacing[1], spacing[2], 1.0])
    nib.save(nib.Nifti1Image(data.astype(np.float32), affine), str(path))


class TestCoreFatMetrics(unittest.TestCase):
    def test_calculate_all_metrics_populates_abdominal_fat_fields_for_artifacts_layout(self):
        with tempfile.TemporaryDirectory() as tmp:
            case_dir = Path(tmp) / "CaseB_20260403_456"
            case_id = case_dir.name

            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)
            (case_dir / "artifacts" / "tissue_types").mkdir(parents=True)

            (case_dir / "metadata" / "id.json").write_text(
                json.dumps({"CaseID": case_id, "Modality": "CT"}),
                encoding="utf-8",
            )

            ct = np.zeros((12, 12, 20), dtype=np.float32)
            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct, spacing=(1.0, 1.0, 2.0))

            # Reverse z ordering to protect the real-case bug where T12 has a
            # higher slice index than L5.
            level_positions = {
                "T12": (15, 16),
                "L1": (13, 14),
                "L2": (10, 12),
                "L3": (7, 9),
                "L4": (4, 6),
                "L5": (2, 3),
            }
            for level, (z0, z1) in level_positions.items():
                vertebra = np.zeros_like(ct, dtype=np.float32)
                vertebra[2:10, 2:10, z0 : z1 + 1] = 1.0
                write_nifti(case_dir / "artifacts" / "total" / f"vertebrae_{level}.nii.gz", vertebra)

            vat = np.zeros_like(ct, dtype=np.float32)
            sat = np.zeros_like(ct, dtype=np.float32)
            vat[1:6, 1:6, 2:17] = 1.0
            sat[6:11, 6:11, 2:17] = 1.0
            muscle = np.zeros_like(ct, dtype=np.float32)
            muscle[3:9, 3:9, 7:10] = 1.0

            write_nifti(case_dir / "artifacts" / "tissue_types" / "torso_fat.nii.gz", vat)
            write_nifti(case_dir / "artifacts" / "tissue_types" / "subcutaneous_fat.nii.gz", sat)
            write_nifti(case_dir / "artifacts" / "tissue_types" / "skeletal_muscle.nii.gz", muscle)

            results = calculate_all_metrics(
                case_id=case_id,
                nifti_path=case_dir / "derived" / f"{case_id}.nii.gz",
                case_output_folder=case_dir,
                generate_overlays=True,
            )

            self.assertEqual(results["abdominal_fat_analysis_status"], "Complete")
            self.assertEqual(results["abdominal_fat_measurement_region"], "T12-L5")
            self.assertTrue(results["abdominal_fat_region_complete"])
            self.assertGreater(results["abdominal_visceral_fat_volume_cm3"], 0)
            self.assertGreater(results["abdominal_subcutaneous_fat_volume_cm3"], 0)
            self.assertGreater(results["visceral_to_subcutaneous_ratio"], 0)
            self.assertEqual(
                results["abdominal_fat_compartments"]["aggregate"]["z_start"],
                2,
            )
            self.assertEqual(
                results["abdominal_fat_compartments"]["aggregate"]["z_end"],
                16,
            )
            self.assertTrue((case_dir / "fat_compartments_overview.png").exists())
            self.assertTrue(
                (case_dir / "artifacts" / "metrics" / "abdominal_fat_compartments" / "result.json").exists()
            )


if __name__ == "__main__":
    unittest.main()
