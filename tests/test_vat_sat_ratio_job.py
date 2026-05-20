import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nibabel as nib
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from heimdallr.metrics.jobs import vat_sat_ratio  # noqa: E402
from heimdallr.shared import settings  # noqa: E402


def write_nifti(path: Path, data: np.ndarray, spacing=(1.0, 1.0, 1.0)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    affine = np.diag([spacing[0], spacing[1], spacing[2], 1.0])
    nib.save(nib.Nifti1Image(data.astype(np.float32), affine), str(path))


class TestVatSatRatioJob(unittest.TestCase):
    def test_vat_sat_ratio_writes_validated_style_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseVatSat_20260520_001"
            case_dir = tmp_path / case_id
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)
            (case_dir / "artifacts" / "tissue_types").mkdir(parents=True)

            (case_dir / "metadata" / "id.json").write_text(
                json.dumps(
                    {
                        "CaseID": case_id,
                        "Modality": "CT",
                        "StudyInstanceUID": "1.2.3.4.5",
                        "PatientSize": 1.70,
                        "PatientWeight": 80,
                        "Pipeline": {"series_selection": {"SelectedPhase": "native"}},
                    }
                ),
                encoding="utf-8",
            )
            (case_dir / "metadata" / "metadata.json").write_text("{}", encoding="utf-8")

            ct = np.zeros((24, 20, 16), dtype=np.float32)
            ct[:, :, :] = 50.0
            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct, spacing=(1.0, 1.0, 2.5))

            l3 = np.zeros_like(ct, dtype=np.float32)
            l3[8:14, 5:15, 6:10] = 1.0
            l1 = np.zeros_like(ct, dtype=np.float32)
            l1[8:14, 5:15, 2:5] = 1.0
            sat = np.zeros_like(ct, dtype=np.float32)
            vat = np.zeros_like(ct, dtype=np.float32)
            sat[3:18, 2:18, 8] = 1.0
            vat[7:14, 6:14, 8] = 1.0
            write_nifti(case_dir / "artifacts" / "total" / "vertebrae_L3.nii.gz", l3)
            write_nifti(case_dir / "artifacts" / "total" / "vertebrae_L1.nii.gz", l1)
            write_nifti(case_dir / "artifacts" / "tissue_types" / "subcutaneous_fat.nii.gz", sat)
            write_nifti(case_dir / "artifacts" / "tissue_types" / "torso_fat.nii.gz", vat)

            with patch.object(settings, "STUDIES_DIR", tmp_path):
                with patch.object(sys, "argv", ["vat_sat_ratio", "--case-id", case_id, "--job-config-json", "{}"]):
                    self.assertEqual(vat_sat_ratio.main(), 0)

            result = json.loads((case_dir / "artifacts" / "metrics" / "vat_sat_ratio" / "result.json").read_text(encoding="utf-8"))
            self.assertEqual(result["status"], "done")
            self.assertEqual(result["metric_key"], "vat_sat_ratio")
            self.assertEqual(result["measurement"]["anatomic_level_used"], "L3")
            self.assertGreater(result["measurement"]["visceral_fat_area_cm2"], 0)
            self.assertGreater(result["measurement"]["subcutaneous_fat_area_cm2"], 0)
            self.assertIn("overlay_png", result["artifacts"])


if __name__ == "__main__":
    unittest.main()
