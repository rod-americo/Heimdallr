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

from heimdallr.metrics.jobs import liver_lesions


def write_nifti(path: Path, data: np.ndarray, spacing=(1.0, 1.0, 1.0)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    nib.save(nib.Nifti1Image(data.astype(np.float32), np.diag([*spacing, 1.0])), str(path))


class TestLiverLesionsJob(unittest.TestCase):
    def _run_job(self, case_dir: Path, job_config: dict | None = None) -> tuple[int, dict]:
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
                    "StudyInstanceUID": "1.2.826.0.1.3680043.10.54321.3",
                    "PatientName": "Heimdallr^Test",
                    "PatientID": "TEST",
                    "Modality": "CT",
                }
            ),
            encoding="utf-8",
        )
        metadata_json.write_text("{}", encoding="utf-8")
        argv = [
            "liver_lesions",
            "--case-id",
            case_id,
            "--job-config-json",
            json.dumps(job_config or {}),
        ]
        stdout = StringIO()
        with (
            patch.object(sys, "argv", argv),
            patch("heimdallr.metrics.jobs._bone_job_common.study_dir", return_value=case_dir),
            patch(
                "heimdallr.metrics.jobs._bone_job_common.study_artifacts_dir",
                return_value=artifacts_dir,
            ),
            patch(
                "heimdallr.metrics.jobs._bone_job_common.study_nifti",
                return_value=case_dir / "derived" / f"{case_id}.nii.gz",
            ),
            patch("heimdallr.metrics.jobs._bone_job_common.study_id_json", return_value=id_json),
            patch(
                "heimdallr.metrics.jobs._bone_job_common.study_metadata_json",
                return_value=metadata_json,
            ),
            patch(
                "heimdallr.metrics.jobs._bone_job_common.study_results_json",
                return_value=results_json,
            ),
            patch("heimdallr.metrics.jobs.liver_lesions.study_artifacts_dir", return_value=artifacts_dir),
            redirect_stdout(stdout),
        ):
            exit_code = liver_lesions.main()
        return exit_code, json.loads(stdout.getvalue())

    def test_empty_mask_writes_negative_result_without_overlay(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_dir = Path(tmpdir) / "CaseLiverNegative"
            shape = (12, 12, 8)
            write_nifti(case_dir / "derived" / "CaseLiverNegative.nii.gz", np.zeros(shape))
            liver = np.zeros(shape)
            liver[2:10, 2:10, 1:7] = 1
            write_nifti(case_dir / "artifacts" / "total" / "liver.nii.gz", liver)
            write_nifti(
                case_dir / "artifacts" / "liver_lesions" / "liver_lesions.nii.gz",
                np.zeros(shape),
            )
            metric_dir = case_dir / "artifacts" / "metrics" / "liver_lesions"
            metric_dir.mkdir(parents=True)
            (metric_dir / "stale_overlay.dcm").write_bytes(b"stale")

            exit_code, payload = self._run_job(case_dir)

            self.assertEqual(exit_code, 0)
            self.assertEqual(payload["status"], "done")
            self.assertFalse(payload["measurement"]["has_hepatic_lesion"])
            self.assertNotIn("overlay_sc_dcm", payload["artifacts"])
            self.assertFalse((metric_dir / "stale_overlay.dcm").exists())

    def test_positive_components_generate_spatial_secondary_capture_series(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_dir = Path(tmpdir) / "CaseLiverPositive"
            shape = (16, 16, 10)
            ct = np.full(shape, 60.0, dtype=np.float32)
            liver = np.zeros(shape)
            liver[2:14, 2:14, 1:9] = 1
            lesions = np.zeros(shape)
            lesions[4:6, 4:6, 2:3] = 1
            lesions[9:12, 9:12, 6:8] = 1
            write_nifti(case_dir / "derived" / "CaseLiverPositive.nii.gz", ct)
            write_nifti(case_dir / "artifacts" / "total" / "liver.nii.gz", liver)
            write_nifti(
                case_dir / "artifacts" / "liver_lesions" / "liver_lesions.nii.gz",
                lesions,
            )

            exit_code, payload = self._run_job(
                case_dir,
                {
                    "locale": "pt_BR",
                    "secondary_capture_transfer_syntax": "original",
                },
            )

            self.assertEqual(exit_code, 0)
            measurement = payload["measurement"]
            self.assertTrue(measurement["has_hepatic_lesion"])
            self.assertEqual(measurement["lesion_component_count"], 2)
            self.assertEqual(measurement["lesion_voxel_count"], 22)
            self.assertEqual([item["slice_index"] for item in payload["dicom_exports"]], [2, 6])
            datasets = [pydicom.dcmread(case_dir / item["path"]) for item in payload["dicom_exports"]]
            self.assertEqual(len({str(dataset.SeriesInstanceUID) for dataset in datasets}), 1)
            self.assertEqual([int(dataset.InstanceNumber) for dataset in datasets], [1, 2])
            self.assertEqual(
                [float(value) for value in datasets[0].ImagePositionPatient],
                [0.0, 0.0, 2.0],
            )
            self.assertEqual(
                datasets[0].SeriesDescription,
                "Heimdallr Overlay de Lesões Hepáticas",
            )
            self.assertIn("liver_lesions", datasets[0].DerivationDescription)


if __name__ == "__main__":
    unittest.main()
