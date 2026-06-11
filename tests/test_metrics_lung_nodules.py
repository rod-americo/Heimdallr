import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import nibabel as nib
import numpy as np
import pydicom

from heimdallr.metrics.jobs import lung_nodules


def write_nifti(path: Path, data: np.ndarray, spacing=(1.0, 1.0, 1.0)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    affine = np.diag([spacing[0], spacing[1], spacing[2], 1.0])
    nib.save(nib.Nifti1Image(data.astype(np.float32), affine), str(path))


class TestLungNodulesJob(unittest.TestCase):
    def _run_job(self, case_dir: Path, job_config: dict | None = None) -> int:
        case_id = case_dir.name
        artifacts_dir = case_dir / "artifacts"
        metadata_dir = case_dir / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        id_json = metadata_dir / "id.json"
        metadata_json = metadata_dir / "metadata.json"
        results_json = metadata_dir / "resultados.json"
        id_json.write_text(
            json.dumps(
                {
                    "CaseID": case_id,
                    "StudyInstanceUID": "1.2.826.0.1.3680043.10.54321.1",
                    "PatientName": "Heimdallr^Test",
                    "PatientID": "TEST",
                    "Modality": "CT",
                }
            ),
            encoding="utf-8",
        )
        metadata_json.write_text("{}", encoding="utf-8")
        argv = [
            "lung_nodules",
            "--case-id",
            case_id,
            "--job-config-json",
            json.dumps(job_config or {}),
        ]
        with (
            patch.object(sys, "argv", argv),
            patch("heimdallr.metrics.jobs._bone_job_common.study_dir", return_value=case_dir),
            patch("heimdallr.metrics.jobs._bone_job_common.study_artifacts_dir", return_value=artifacts_dir),
            patch("heimdallr.metrics.jobs._bone_job_common.study_nifti", return_value=case_dir / "derived" / f"{case_id}.nii.gz"),
            patch("heimdallr.metrics.jobs._bone_job_common.study_id_json", return_value=id_json),
            patch("heimdallr.metrics.jobs._bone_job_common.study_metadata_json", return_value=metadata_json),
            patch("heimdallr.metrics.jobs._bone_job_common.study_results_json", return_value=results_json),
            patch("heimdallr.metrics.jobs.lung_nodules.study_artifacts_dir", return_value=artifacts_dir),
            redirect_stdout(StringIO()),
        ):
            return lung_nodules.main()

    def test_job_writes_negative_json_without_dicom_for_empty_mask(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_dir = Path(tmpdir) / "CaseLungNegative"
            ct_path = case_dir / "derived" / "CaseLungNegative.nii.gz"
            write_nifti(ct_path, np.zeros((12, 12, 8), dtype=np.float32))
            lung = np.zeros((12, 12, 8), dtype=np.float32)
            lung[2:10, 2:10, 0:4] = 1.0
            write_nifti(case_dir / "artifacts" / "total" / "lung_lower_lobe_right.nii.gz", lung)
            write_nifti(case_dir / "artifacts" / "lung_nodules" / "lung.nii.gz", lung)
            write_nifti(case_dir / "artifacts" / "lung_nodules" / "lung_nodules.nii.gz", np.zeros_like(lung))

            exit_code = self._run_job(case_dir, {"secondary_capture_transfer_syntax": "original"})

            self.assertEqual(exit_code, 0)
            result = json.loads(
                (case_dir / "artifacts" / "metrics" / "lung_nodules" / "result.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(result["status"], "done")
            self.assertFalse(result["measurement"]["has_pulmonary_nodule"])
            self.assertNotIn("overlay_sc_dcm", result["artifacts"])

    def test_job_writes_positive_json_and_secondary_capture_for_positive_mask(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_dir = Path(tmpdir) / "CaseLungPositive"
            ct = np.full((16, 16, 10), -800.0, dtype=np.float32)
            ct[6:10, 6:10, 5] = -120.0
            ct_path = case_dir / "derived" / "CaseLungPositive.nii.gz"
            write_nifti(ct_path, ct)
            lung = np.zeros_like(ct, dtype=np.float32)
            lung[2:14, 2:14, 1:9] = 1.0
            nodule = np.zeros_like(ct, dtype=np.float32)
            nodule[7:9, 7:9, 5:7] = 1.0
            write_nifti(case_dir / "artifacts" / "total" / "lung_lower_lobe_right.nii.gz", lung)
            write_nifti(case_dir / "artifacts" / "lung_nodules" / "lung_nodules.nii.gz", nodule)

            exit_code = self._run_job(
                case_dir,
                {
                    "secondary_capture_transfer_syntax": "original",
                    "secondary_capture_max_dimension": 512,
                    "locale": "pt_BR",
                },
            )

            self.assertEqual(exit_code, 0)
            result_path = case_dir / "artifacts" / "metrics" / "lung_nodules" / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertTrue(result["measurement"]["has_pulmonary_nodule"])
            self.assertEqual(result["measurement"]["nodule_component_count"], 1)
            self.assertIn("overlay_sc_dcm", result["artifacts"])
            self.assertEqual(result["dicom_exports"][0]["kind"], "secondary_capture")
            ds = pydicom.dcmread(case_dir / result["artifacts"]["overlay_sc_dcm"])
            self.assertEqual(ds.SeriesDescription, "Heimdallr Overlay de Nódulos Pulmonares")
            self.assertEqual(
                ds.DerivationDescription,
                "Overlay de detecção de nódulos pulmonares a partir da segmentação lung_nodules.",
            )


if __name__ == "__main__":
    unittest.main()
