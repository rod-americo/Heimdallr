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

from heimdallr.metrics.jobs import brain_volumetry  # noqa: E402
from heimdallr.shared import settings  # noqa: E402


def write_nifti(path: Path, data: np.ndarray, spacing=(1.0, 1.0, 1.0)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    affine = np.diag([spacing[0], spacing[1], spacing[2], 1.0])
    nib.save(nib.Nifti1Image(data.astype(np.float32), affine), str(path))


class TestBrainVolumetryJob(unittest.TestCase):
    def test_compute_brain_measurement_reports_complete_volume(self):
        mask = np.zeros((8, 8, 6), dtype=bool)
        mask[2:6, 2:6, 1:5] = True

        measurement = brain_volumetry._compute_brain_measurement(
            mask,
            (1.0, 1.0, 2.0),
        )

        self.assertEqual(measurement["analysis_status"], "complete")
        self.assertTrue(measurement["complete"])
        self.assertEqual(measurement["voxel_count"], 64)
        self.assertEqual(measurement["observed_volume_cm3"], 0.128)
        self.assertEqual(measurement["volume_cm3"], 0.128)
        self.assertEqual(measurement["axial_slice_extent"], {"start": 1, "end": 4})

    def test_compute_brain_measurement_suppresses_truncated_volume(self):
        mask = np.zeros((8, 8, 6), dtype=bool)
        mask[2:6, 2:6, 0:4] = True

        measurement = brain_volumetry._compute_brain_measurement(
            mask,
            (1.0, 1.0, 2.0),
        )

        self.assertEqual(measurement["analysis_status"], "incomplete")
        self.assertFalse(measurement["complete"])
        self.assertTrue(measurement["truncated_at_scan_bounds"])
        self.assertIsNone(measurement["observed_volume_cm3"])
        self.assertIsNone(measurement["volume_cm3"])

    def test_job_writes_metrics_and_dicom_series(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseBrain_20260510_001"
            case_dir = tmp_path / case_id
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)

            id_payload = {
                "CaseID": case_id,
                "Modality": "CT",
                "StudyInstanceUID": "1.2.826.0.1.3680043.8.498.6101",
                "PatientName": "Test^Brain",
                "PatientID": "PBRAIN001",
                "Pipeline": {"series_selection": {"SelectedPhase": "native"}},
            }
            (case_dir / "metadata" / "id.json").write_text(json.dumps(id_payload), encoding="utf-8")
            (case_dir / "metadata" / "metadata.json").write_text(json.dumps(id_payload), encoding="utf-8")
            (case_dir / "metadata" / "resultados.json").write_text("{}", encoding="utf-8")

            shape = (16, 16, 12)
            ct = np.zeros(shape, dtype=np.float32)
            brain = np.zeros(shape, dtype=np.float32)
            brain[4:12, 4:12, 2:10] = 1.0
            ct[brain.astype(bool)] = 36.0
            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct, spacing=(1.0, 1.0, 2.0))
            write_nifti(case_dir / "artifacts" / "total" / "brain.nii.gz", brain, spacing=(1.0, 1.0, 2.0))

            with patch.object(settings, "STUDIES_DIR", tmp_path):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "brain_volumetry",
                        "--case-id",
                        case_id,
                        "--job-config-json",
                        '{"generate_overlay": true, "emit_secondary_capture_dicom": true}',
                    ],
                ):
                    self.assertEqual(brain_volumetry.main(), 0)

            result_path = case_dir / "artifacts" / "metrics" / "brain_volumetry" / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))

            self.assertEqual(result["status"], "done")
            self.assertEqual(result["measurement"]["job_status"], "complete")
            self.assertEqual(result["measurement"]["target_slice_thickness_mm"], 5.0)
            self.assertEqual(result["measurement"]["brain"]["volume_cm3"], 1.024)
            self.assertEqual(result["measurement"]["brain_volume_cm3"], 1.024)
            self.assertGreater(result["measurement"]["exported_slice_count"], 0)
            self.assertEqual(len(result["dicom_exports"]), result["measurement"]["exported_slice_count"])

            first_dicom = case_dir / result["dicom_exports"][0]["path"]
            ds = pydicom.dcmread(str(first_dicom))
            self.assertEqual(str(ds.SeriesDescription), "Heimdallr Brain Volumetry Overlay 5 mm")
            self.assertIn("5 mm axial reconstruction", str(ds.DerivationDescription))
            self.assertEqual(int(ds.InstanceNumber), 1)
            self.assertEqual(str(ds.Modality), "OT")

    def test_job_skips_when_brain_mask_is_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseBrainMissing_20260510_001"
            case_dir = tmp_path / case_id
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)
            (case_dir / "metadata" / "id.json").write_text(
                json.dumps({"CaseID": case_id, "Modality": "CT"}),
                encoding="utf-8",
            )
            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", np.zeros((8, 8, 6), dtype=np.float32))

            with patch.object(settings, "STUDIES_DIR", tmp_path):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "brain_volumetry",
                        "--case-id",
                        case_id,
                        "--job-config-json",
                        "{}",
                    ],
                ):
                    self.assertEqual(brain_volumetry.main(), 0)

            result_path = case_dir / "artifacts" / "metrics" / "brain_volumetry" / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual(result["status"], "skipped")
            self.assertEqual(result["measurement"]["job_status"], "missing_brain_mask")

    def test_example_profile_keeps_brain_volumetry_disabled_by_default(self):
        config_path = ROOT / "config" / "metrics_pipeline.example.json"
        config = json.loads(config_path.read_text(encoding="utf-8"))
        jobs = config["profiles"]["ct_native_basic_metrics"]["jobs"]
        brain_job = next(job for job in jobs if job["name"] == "brain_volumetry")

        self.assertFalse(brain_job["enabled"])
        self.assertEqual(brain_job["requires_segmentation_tasks"], ["total"])


if __name__ == "__main__":
    unittest.main()
