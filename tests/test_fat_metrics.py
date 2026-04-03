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

from scripts.prototype_fat_metrics import (  # noqa: E402
    build_slab_plan,
    calculate_compartment_volume,
    calculate_slab_metrics,
    main,
)


def write_nifti(path: Path, data: np.ndarray, spacing=(1.0, 1.0, 1.0)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    affine = np.diag([spacing[0], spacing[1], spacing[2], 1.0])
    nib.save(nib.Nifti1Image(data.astype(np.float32), affine), str(path))


class TestFatMetricsPrototype(unittest.TestCase):
    def test_build_slab_plan_uses_intervertebral_landmarks(self):
        landmark_slices = {"T12": 2, "L1": 5, "L2": 8, "L3": 11, "L4": 14, "L5": 17}

        slabs = build_slab_plan(landmark_slices, z_size=20, spacing_z_mm=2.5)

        self.assertEqual([slab.label for slab in slabs], ["T12_to_L1", "L1_to_L2", "L2_to_L3", "L3_to_L4", "L4_to_L5"])
        self.assertEqual(slabs[0].start_slice, 2)
        self.assertEqual(slabs[-1].end_slice, 17)

    def test_calculate_slab_metrics_partitions_volume_by_slice_ranges(self):
        mask = np.zeros((6, 6, 12), dtype=bool)
        mask[:, :, 0:4] = True
        mask[0:3, 0:3, 4:8] = True
        mask[3:6, 3:6, 8:12] = True

        slabs = [
            {"label": "s1", "start_slice": 0, "end_slice": 4, "start_mm": 0.0, "end_mm": 4.0},
            {"label": "s2", "start_slice": 4, "end_slice": 8, "start_mm": 4.0, "end_mm": 8.0},
            {"label": "s3", "start_slice": 8, "end_slice": 12, "start_mm": 8.0, "end_mm": 12.0},
        ]

        slab_specs = [type("SlabSpec", (), slab) for slab in slabs]
        result = calculate_slab_metrics(mask, (1.0, 1.0, 1.0), slab_specs)

        self.assertEqual(result["voxel_count"], int(mask.sum()))
        self.assertEqual(len(result["slabs"]), 3)
        self.assertEqual(result["slabs"][0]["voxel_count"], 6 * 6 * 4)
        self.assertEqual(result["slabs"][1]["voxel_count"], 3 * 3 * 4)
        self.assertEqual(result["slabs"][2]["voxel_count"], 3 * 3 * 4)

    def test_calculate_compartment_volume_uses_spacing(self):
        mask = np.zeros((4, 4, 4), dtype=bool)
        mask[1:3, 1:3, 1:3] = True

        metrics = calculate_compartment_volume(mask, (1.0, 2.0, 3.0))

        self.assertEqual(metrics["voxel_count"], 8)
        self.assertEqual(metrics["volume_mm3"], 48.0)
        self.assertEqual(metrics["volume_cm3"], 0.048)

    def test_cli_smoke_writes_summary_and_overview(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_dir = tmp_path / "CaseA_20260403_123"
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)
            (case_dir / "artifacts" / "tissue_types").mkdir(parents=True)

            case_id = "CaseA_20260403_123"
            id_json = {
                "CaseID": case_id,
                "Modality": "CT",
                "PatientName": "Case A",
                "StudyDate": "20260403",
                "AccessionNumber": "123",
            }
            (case_dir / "metadata" / "id.json").write_text(json.dumps(id_json), encoding="utf-8")

            ct = np.zeros((10, 10, 20), dtype=np.float32)
            ct[:, :, :] = 50.0
            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct, spacing=(1.0, 1.0, 2.0))

            # Landmarks that create four non-overlapping slabs.
            for label, z in {"T12": 2, "L1": 6, "L2": 10, "L3": 14, "L4": 16, "L5": 18}.items():
                vertebra = np.zeros_like(ct, dtype=np.float32)
                vertebra[:, :, z] = 1.0
                write_nifti(case_dir / "artifacts" / "total" / f"vertebrae_{label}.nii.gz", vertebra)

            vat = np.zeros_like(ct, dtype=np.float32)
            sat = np.zeros_like(ct, dtype=np.float32)
            vat[0:5, 0:5, 2:18] = 1.0
            sat[5:10, 5:10, 2:18] = 1.0
            write_nifti(case_dir / "artifacts" / "tissue_types" / "torso_fat.nii.gz", vat)
            write_nifti(case_dir / "artifacts" / "tissue_types" / "subcutaneous_fat.nii.gz", sat)

            output_root = tmp_path / "out"
            exit_code = main([str(case_dir), "--output-dir", str(output_root)])
            self.assertEqual(exit_code, 0)

            summary_path = output_root / case_id / "fat_metrics" / "summary.json"
            overview_path = output_root / case_id / "fat_metrics" / "overview.png"

            self.assertTrue(summary_path.exists())
            self.assertTrue(overview_path.exists())

            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(summary["case_id"], case_id)
            self.assertEqual(summary["qc"]["landmark_count"], 6)
            self.assertIn("torso_fat", summary["fat_compartments"])
            self.assertIn("subcutaneous_fat", summary["fat_compartments"])
            self.assertGreater(summary["fat_compartments"]["torso_fat"]["volume_cm3"], 0)
            self.assertGreater(summary["fat_compartments"]["subcutaneous_fat"]["volume_cm3"], 0)


if __name__ == "__main__":
    unittest.main()
