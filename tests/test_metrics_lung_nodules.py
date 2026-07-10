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
    def test_overlay_uses_lung_window(self):
        self.assertEqual(lung_nodules.LUNG_WINDOW_LEVEL_HU, -600.0)
        self.assertEqual(lung_nodules.LUNG_WINDOW_WIDTH_HU, 1500.0)
        self.assertEqual(lung_nodules.LUNG_WINDOW_LIMITS_HU, (-1350.0, 150.0))

    def test_axial_overlay_display_keeps_anterior_at_top_for_lps_affine(self):
        ct_slice = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float32)
        lung_slice = ct_slice > 2
        nodule_slice = ct_slice == 6
        lps_affine = np.diag([-1.0, -1.0, 1.0, 1.0])

        display_ct, display_lung, display_nodule, source_axis_codes = lung_nodules._display_axial_slices(
            ct_slice,
            lung_slice,
            nodule_slice,
            ct_affine=lps_affine,
        )

        expected_ct = np.array([[1, 4], [2, 5], [3, 6]], dtype=np.float32)
        self.assertEqual(source_axis_codes, ("L", "P"))
        self.assertTrue(np.array_equal(display_ct, expected_ct))
        self.assertTrue(np.array_equal(display_lung, expected_ct > 2))
        self.assertTrue(np.array_equal(display_nodule, expected_ct == 6))

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

    def test_job_writes_positive_json_and_component_secondary_capture_series_for_positive_mask(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_dir = Path(tmpdir) / "CaseLungPositive"
            ct = np.full((16, 16, 10), -800.0, dtype=np.float32)
            ct[6:10, 6:10, 5] = -120.0
            ct[3:5, 3:5, 2] = -100.0
            ct_path = case_dir / "derived" / "CaseLungPositive.nii.gz"
            write_nifti(ct_path, ct)
            lung = np.zeros_like(ct, dtype=np.float32)
            lung[2:14, 2:14, 1:9] = 1.0
            nodule = np.zeros_like(ct, dtype=np.float32)
            nodule[7:9, 7:9, 5:7] = 1.0
            nodule[3:4, 3:4, 2:3] = 1.0
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
            self.assertEqual(result["measurement"]["nodule_component_count"], 2)
            self.assertIn("overlay_sc_dcm", result["artifacts"])
            self.assertEqual(len(result["artifacts"]["component_overlays"]), 2)
            self.assertEqual(len(result["dicom_exports"]), 2)
            components = result["measurement"]["components"]
            self.assertEqual(components[0]["voxel_count"], 8)
            self.assertEqual(components[0]["slice_index"], 6)
            self.assertEqual(components[0]["probable_viewer_slice_index_one_based"], 4)
            self.assertEqual(result["dicom_exports"][0]["component_id"], components[0]["component_id"])
            self.assertEqual(result["dicom_exports"][0]["kind"], "secondary_capture")
            self.assertEqual(
                result["artifacts"]["overlay_sc_dcm"],
                result["dicom_exports"][0]["path"],
            )
            ds = pydicom.dcmread(case_dir / result["artifacts"]["overlay_sc_dcm"])
            self.assertEqual(ds.SeriesDescription, "Heimdallr Overlay de Nódulos Pulmonares")
            self.assertEqual(
                ds.DerivationDescription,
                "Overlay de detecção de nódulos pulmonares a partir da segmentação lung_nodules.",
            )
            ds_second = pydicom.dcmread(case_dir / result["dicom_exports"][1]["path"])
            self.assertEqual(ds.SeriesInstanceUID, ds_second.SeriesInstanceUID)
            self.assertEqual(ds.InstanceNumber, 1)
            self.assertEqual(ds_second.InstanceNumber, 2)


if __name__ == "__main__":
    unittest.main()
