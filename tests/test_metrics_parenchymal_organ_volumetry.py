import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nibabel as nib
import numpy as np
import pydicom

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from heimdallr.metrics.jobs import parenchymal_organ_volumetry  # noqa: E402
from heimdallr.shared import settings  # noqa: E402


def write_nifti(path: Path, data: np.ndarray, spacing=(1.0, 1.0, 1.0)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    affine = np.diag([spacing[0], spacing[1], spacing[2], 1.0])
    nib.save(nib.Nifti1Image(data.astype(np.float32), affine), str(path))


class TestParenchymalOrganVolumetryJob(unittest.TestCase):
    def test_job_writes_metrics_and_dicom_series(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseParenchyma_20260404_001"
            case_dir = tmp_path / case_id
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)

            id_payload = {
                "CaseID": case_id,
                "Modality": "CT",
                "StudyInstanceUID": "1.2.826.0.1.3680043.8.498.1",
                "PatientName": "Test^Patient",
                "PatientID": "P001",
                "Pipeline": {"series_selection": {"SelectedPhase": "native"}},
            }
            (case_dir / "metadata" / "id.json").write_text(json.dumps(id_payload), encoding="utf-8")
            (case_dir / "metadata" / "metadata.json").write_text(json.dumps(id_payload), encoding="utf-8")
            (case_dir / "metadata" / "resultados.json").write_text("{}", encoding="utf-8")

            shape = (16, 16, 12)
            ct = np.zeros(shape, dtype=np.float32)
            ct[2:10, 3:11, 2:9] = 55.0
            ct[4:10, 4:10, 4:9] = 48.0
            ct[8:13, 2:7, 3:8] = 40.0
            ct[2:6, 9:13, 3:9] = 28.0
            ct[9:13, 9:13, 3:9] = 31.0
            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct, spacing=(1.0, 1.0, 1.0))

            liver = np.zeros(shape, dtype=np.float32)
            liver[2:10, 3:11, 2:9] = 1.0
            spleen = np.zeros(shape, dtype=np.float32)
            spleen[8:13, 2:7, 3:8] = 1.0
            pancreas = np.zeros(shape, dtype=np.float32)
            pancreas[4:10, 4:10, 4:9] = 1.0
            kidney_right = np.zeros(shape, dtype=np.float32)
            kidney_right[2:6, 9:13, 3:9] = 1.0
            kidney_left = np.zeros(shape, dtype=np.float32)
            kidney_left[9:13, 9:13, 3:9] = 1.0

            write_nifti(case_dir / "artifacts" / "total" / "liver.nii.gz", liver)
            write_nifti(case_dir / "artifacts" / "total" / "spleen.nii.gz", spleen)
            write_nifti(case_dir / "artifacts" / "total" / "pancreas.nii.gz", pancreas)
            write_nifti(case_dir / "artifacts" / "total" / "kidney_right.nii.gz", kidney_right)
            write_nifti(case_dir / "artifacts" / "total" / "kidney_left.nii.gz", kidney_left)

            with patch.object(settings, "STUDIES_DIR", tmp_path):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "parenchymal_organ_volumetry",
                        "--case-id",
                        case_id,
                        "--job-config-json",
                        '{"generate_overlay": true, "emit_secondary_capture_dicom": true}',
                    ],
                ):
                    self.assertEqual(parenchymal_organ_volumetry.main(), 0)

            result_path = case_dir / "artifacts" / "metrics" / "parenchymal_organ_volumetry" / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))

            self.assertEqual(result["status"], "done")
            self.assertEqual(result["measurement"]["job_status"], "complete")
            self.assertEqual(result["measurement"]["target_slice_thickness_mm"], 5.0)
            self.assertGreater(result["measurement"]["exported_slice_count"], 0)
            self.assertEqual(len(result["dicom_exports"]), result["measurement"]["exported_slice_count"])
            self.assertAlmostEqual(result["measurement"]["organs"]["liver"]["hu_mean"], 55.0, places=2)
            self.assertTrue(result["measurement"]["organs"]["pancreas"]["complete"])

            first_dicom = case_dir / result["dicom_exports"][0]["path"]
            ds = pydicom.dcmread(str(first_dicom))
            self.assertEqual(str(ds.SeriesDescription), "Heimdallr Parenchymal Organ Overlay 5 mm")
            self.assertEqual(int(ds.InstanceNumber), 1)
            self.assertEqual(str(ds.Modality), "OT")


if __name__ == "__main__":
    unittest.main()
