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

from heimdallr.metrics.jobs import pleural_pericard_effusion


def write_nifti(path: Path, data: np.ndarray, spacing=(1.0, 1.0, 1.0)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    affine = np.diag([spacing[0], spacing[1], spacing[2], 1.0])
    nib.save(nib.Nifti1Image(data.astype(np.float32), affine), str(path))


class TestPleuralPericardEffusionJob(unittest.TestCase):
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
                    "StudyInstanceUID": "1.2.826.0.1.3680043.10.54321.2",
                    "PatientName": "Heimdallr^Test",
                    "PatientID": "TEST",
                    "Modality": "CT",
                }
            ),
            encoding="utf-8",
        )
        metadata_json.write_text("{}", encoding="utf-8")
        argv = [
            "pleural_pericard_effusion",
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
            patch(
                "heimdallr.metrics.jobs.pleural_pericard_effusion.study_artifacts_dir",
                return_value=artifacts_dir,
            ),
            redirect_stdout(stdout),
        ):
            exit_code = pleural_pericard_effusion.main()
        return exit_code, json.loads(stdout.getvalue())

    def test_negative_case_is_not_published_and_removes_stale_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_dir = Path(tmpdir) / "CaseEffusionNegative"
            ct = np.full((16, 16, 10), -800.0, dtype=np.float32)
            write_nifti(case_dir / "derived" / "CaseEffusionNegative.nii.gz", ct)
            lung = np.zeros_like(ct)
            lung[2:14, 2:14, 1:9] = 1.0
            write_nifti(case_dir / "artifacts" / "total" / "lung_lower_lobe_right.nii.gz", lung)
            empty = np.zeros_like(ct)
            task_dir = case_dir / "artifacts" / "pleural_pericard_effusion"
            write_nifti(task_dir / "pleural_effusion.nii.gz", empty)
            write_nifti(task_dir / "pericardial_effusion.nii.gz", empty)
            metric_dir = case_dir / "artifacts" / "metrics" / "pleural_pericard_effusion"
            metric_dir.mkdir(parents=True)
            (metric_dir / "result.json").write_text("{}", encoding="utf-8")
            (metric_dir / "stale.dcm").write_bytes(b"stale")

            exit_code, payload = self._run_job(case_dir)

            self.assertEqual(exit_code, 0)
            self.assertEqual(payload["status"], "not_present")
            self.assertFalse(payload["publish_result"])
            self.assertFalse(payload["measurement"]["notification_bool"])
            self.assertEqual(list(metric_dir.iterdir()), [])

    def test_positive_findings_write_component_overlays_in_one_dicom_series(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_dir = Path(tmpdir) / "CaseEffusionPositive"
            ct = np.full((20, 20, 12), -750.0, dtype=np.float32)
            write_nifti(case_dir / "derived" / "CaseEffusionPositive.nii.gz", ct)
            lung = np.zeros_like(ct)
            lung[2:18, 2:18, 1:11] = 1.0
            write_nifti(case_dir / "artifacts" / "total" / "lung_lower_lobe_right.nii.gz", lung)
            pleural = np.zeros_like(ct)
            pleural[2:5, 2:8, 3:6] = 1.0
            pleural[15:18, 12:18, 4:7] = 1.0
            pericardial = np.zeros_like(ct)
            pericardial[8:12, 8:12, 5:8] = 1.0
            task_dir = case_dir / "artifacts" / "pleural_pericard_effusion"
            write_nifti(task_dir / "pleural_effusion.nii.gz", pleural, spacing=(1.0, 1.0, 2.0))
            write_nifti(
                task_dir / "pericardial_effusion.nii.gz",
                pericardial,
                spacing=(1.0, 1.0, 2.0),
            )
            write_nifti(
                case_dir / "derived" / "CaseEffusionPositive.nii.gz",
                ct,
                spacing=(1.0, 1.0, 2.0),
            )

            exit_code, payload = self._run_job(
                case_dir,
                {
                    "secondary_capture_transfer_syntax": "original",
                    "secondary_capture_max_dimension": 512,
                    "locale": "pt_BR",
                },
            )

            self.assertEqual(exit_code, 0)
            self.assertNotIn("publish_result", payload)
            self.assertEqual(payload["status"], "done")
            measurement = payload["measurement"]
            self.assertEqual(
                measurement["present_findings"],
                ["pleural_effusion", "pericardial_effusion"],
            )
            self.assertTrue(measurement["has_pleural_effusion"])
            self.assertTrue(measurement["has_pericardial_effusion"])
            self.assertEqual(measurement["findings"]["pleural_effusion"]["component_count"], 2)
            self.assertEqual(
                measurement["findings"]["pericardial_effusion"]["component_count"],
                1,
            )
            self.assertAlmostEqual(
                measurement["findings"]["pericardial_effusion"]["volume_cm3"],
                0.096,
            )
            self.assertEqual(len(payload["dicom_exports"]), 3)
            datasets = [pydicom.dcmread(case_dir / item["path"]) for item in payload["dicom_exports"]]
            self.assertEqual(len({str(ds.SeriesInstanceUID) for ds in datasets}), 1)
            self.assertEqual([int(ds.InstanceNumber) for ds in datasets], [1, 2, 3])
            self.assertEqual(
                datasets[0].SeriesDescription,
                "Heimdallr Derrames Pleural e Pericárdico",
            )

    def test_single_positive_finding_omits_absent_finding_from_public_payload(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_dir = Path(tmpdir) / "CasePleuralOnly"
            ct = np.full((12, 12, 8), -800.0, dtype=np.float32)
            write_nifti(case_dir / "derived" / "CasePleuralOnly.nii.gz", ct)
            pleural = np.zeros_like(ct)
            pleural[2:5, 2:5, 2:4] = 1.0
            task_dir = case_dir / "artifacts" / "pleural_pericard_effusion"
            write_nifti(task_dir / "pleural_effusion.nii.gz", pleural)
            write_nifti(task_dir / "pericardial_effusion.nii.gz", np.zeros_like(ct))

            exit_code, payload = self._run_job(case_dir, {"generate_overlay": False})

            self.assertEqual(exit_code, 0)
            measurement = payload["measurement"]
            self.assertEqual(measurement["present_findings"], ["pleural_effusion"])
            self.assertTrue(measurement["has_pleural_effusion"])
            self.assertNotIn("has_pericardial_effusion", measurement)
            self.assertNotIn("pericardial_effusion", measurement["findings"])
            self.assertTrue(
                (case_dir / "artifacts" / "metrics" / "pleural_pericard_effusion" / "result.json").exists()
            )


if __name__ == "__main__":
    unittest.main()
