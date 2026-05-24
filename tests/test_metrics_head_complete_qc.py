import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nibabel as nib
import numpy as np
import pydicom
from pydicom.uid import JPEGLSLossless

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from heimdallr.metrics.head import BRAIN_STRUCTURE_MASKS, compute_mask_status  # noqa: E402
from heimdallr.metrics.jobs import head_complete_qc  # noqa: E402
from heimdallr.shared import settings  # noqa: E402


def write_nifti(path: Path, data: np.ndarray, spacing=(1.0, 1.0, 1.0)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    affine = np.diag([spacing[0], spacing[1], spacing[2], 1.0])
    nib.save(nib.Nifti1Image(data.astype(np.float32), affine), str(path))


class TestHeadCompleteQcJob(unittest.TestCase):
    def test_example_profiles_define_opt_in_head_workflow(self):
        metrics_config_path = ROOT / "config" / "metrics_pipeline.example.json"
        config = json.loads(metrics_config_path.read_text(encoding="utf-8"))
        basic_jobs = {
            job["name"]: job
            for job in config["profiles"]["ct_native_basic_metrics"]["jobs"]
        }
        head_jobs = {
            job["name"]: job
            for job in config["profiles"]["ct_head_complete_metrics"]["jobs"]
        }
        head_profile = config["profiles"]["ct_head_complete_metrics"]

        self.assertFalse(basic_jobs["head_complete_qc"]["enabled"])
        self.assertTrue(head_jobs["head_complete_qc"]["enabled"])
        self.assertEqual(head_profile["required"]["selected_phase"], ["native", "unknown"])
        self.assertEqual(
            head_jobs["head_complete_qc"]["requires_segmentation_tasks"],
            ["total", "cerebral_bleed", "brain_structures"],
        )

        segmentation_config_path = ROOT / "config" / "segmentation_pipeline.example.json"
        segmentation_config = json.loads(segmentation_config_path.read_text(encoding="utf-8"))
        head_segmentation_profile = segmentation_config["profiles"]["ct_head_complete_segmentation"]
        head_tasks = {
            task["name"]: task
            for task in head_segmentation_profile["tasks"]
        }
        self.assertEqual(
            head_segmentation_profile["required"]["selected_phase"],
            ["native", "unknown"],
        )
        self.assertEqual(
            list(head_tasks),
            ["total", "cerebral_bleed", "brain_structures"],
        )
        self.assertEqual(head_tasks["total"]["extra_args"][:3], ["--roi_subset", "skull", "brain"])
        self.assertFalse(head_tasks["cerebral_bleed"]["license_required"])
        self.assertTrue(head_tasks["brain_structures"]["license_required"])

    def test_compute_mask_status_flags_scan_bound_truncation(self):
        mask = np.zeros((8, 8, 6), dtype=bool)
        mask[2:6, 2:6, 0:4] = True

        status = compute_mask_status(mask, (1.0, 1.0, 2.0))

        self.assertEqual(status["status"], "truncated")
        self.assertFalse(status["complete"])
        self.assertIn("z_min", status["touched_bounds"])

    def test_job_validates_complete_head_and_writes_normalized_nifti(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseHead_20260524_001"
            case_dir = tmp_path / case_id
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)
            (case_dir / "artifacts" / "cerebral_bleed").mkdir(parents=True)
            (case_dir / "artifacts" / "brain_structures").mkdir(parents=True)
            (case_dir / "metadata" / "id.json").write_text(
                json.dumps(
                    {
                        "CaseID": case_id,
                        "Modality": "CT",
                        "StudyInstanceUID": "1.2.826.0.1.3680043.8.498.1001",
                    }
                ),
                encoding="utf-8",
            )

            shape = (18, 18, 14)
            ct = np.zeros(shape, dtype=np.float32)
            skull = np.zeros(shape, dtype=np.float32)
            brain = np.zeros(shape, dtype=np.float32)
            bleed = np.zeros(shape, dtype=np.float32)
            skull[2:16, 2:16, 2:12] = 1.0
            skull[4:14, 4:14, 4:10] = 0.0
            brain[5:13, 5:13, 4:10] = 1.0
            ct[skull.astype(bool)] = 700.0
            ct[brain.astype(bool)] = 35.0

            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct, spacing=(0.6, 0.6, 1.25))
            write_nifti(case_dir / "artifacts" / "total" / "skull.nii.gz", skull, spacing=(0.6, 0.6, 1.25))
            write_nifti(case_dir / "artifacts" / "total" / "brain.nii.gz", brain, spacing=(0.6, 0.6, 1.25))
            write_nifti(
                case_dir / "artifacts" / "cerebral_bleed" / "intracerebral_hemorrhage.nii.gz",
                bleed,
                spacing=(0.6, 0.6, 1.25),
            )
            for idx, mask_name in enumerate(BRAIN_STRUCTURE_MASKS):
                structure = np.zeros(shape, dtype=np.float32)
                x = 5 + (idx % 4)
                y = 5 + ((idx // 4) % 4)
                z = 4 + (idx % 5)
                structure[x : x + 2, y : y + 2, z : z + 2] = 1.0
                write_nifti(
                    case_dir / "artifacts" / "brain_structures" / f"{mask_name}.nii.gz",
                    structure,
                    spacing=(0.6, 0.6, 1.25),
                )

            with patch.object(settings, "STUDIES_DIR", tmp_path):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "head_complete_qc",
                        "--case-id",
                        case_id,
                        "--job-config-json",
                        json.dumps(
                            {
                                "target_plane": "axial",
                                "target_in_plane_spacing_mm": [1.0, 1.0],
                                "target_slice_thickness_mm": 5.0,
                            }
                        ),
                    ],
                ):
                    self.assertEqual(head_complete_qc.main(), 0)

            result_path = case_dir / "artifacts" / "metrics" / "head_complete_qc" / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))

            self.assertEqual(result["status"], "done")
            self.assertEqual(result["measurement"]["job_status"], "complete")
            self.assertTrue(result["measurement"]["head_complete_without_truncation"])
            self.assertTrue(result["measurement"]["required_segmentation_complete"])
            self.assertFalse(
                result["measurement"]["cerebral_bleed"]["segmented_hemorrhage_present"]
            )
            normalized_relpath = result["artifacts"]["normalized_nifti"]
            self.assertTrue((case_dir / normalized_relpath).exists())
            normalized_2mm_relpath = result["artifacts"]["normalized_2mm_nifti"]
            self.assertTrue((case_dir / normalized_2mm_relpath).exists())
            brain_geometry_relpath = result["artifacts"]["normalized_brain_geometry_2mm_nifti"]
            self.assertTrue((case_dir / brain_geometry_relpath).exists())
            self.assertEqual(
                result["measurement"]["normalization"]["target_spacing_mm"]["z"],
                5.0,
            )
            self.assertEqual(
                result["measurement"]["normalization_2mm"]["normalized_spacing_mm"]["z"],
                2.0,
            )
            self.assertEqual(
                result["measurement"]["normalization_2mm"]["anatomic_alignment"]["status"],
                "landmarks_required",
            )
            self.assertEqual(
                result["measurement"]["normalization_brain_geometry_2mm"]["anatomic_alignment"]["status"],
                "mask_based",
            )
            self.assertEqual(
                result["measurement"]["normalization_brain_geometry_2mm"]["brain_mask"],
                "artifacts/total/brain.nii.gz",
            )
            self.assertEqual(
                result["measurement"]["normalization_brain_geometry_2mm"]["normalized_spacing_mm"]["z"],
                1.0,
            )
            volume_rows = result["measurement"]["brain_structure_volumes"]["rows"]
            self.assertEqual(len(volume_rows), 1 + len(BRAIN_STRUCTURE_MASKS))
            self.assertEqual(volume_rows[0]["key"], "brain_total")
            self.assertEqual(result["measurement"]["cerebral_bleed"]["has_cerebral_bleed"], False)
            self.assertEqual(result["measurement"]["cerebral_bleed"]["notification_bool"], False)

            volume_table = case_dir / result["artifacts"]["volume_table_dicom"]
            self.assertTrue(volume_table.exists())
            volume_table_ds = pydicom.dcmread(str(volume_table), stop_before_pixels=True)
            self.assertEqual(volume_table_ds.SeriesDescription, "Heimdallr Brain Structure Volumes")
            self.assertEqual(volume_table_ds.Rows, 1754)
            self.assertEqual(volume_table_ds.Columns, 1240)
            self.assertEqual(volume_table_ds.file_meta.TransferSyntaxUID, JPEGLSLossless)

            structures_dir = case_dir / result["artifacts"]["brain_structures_overlay_series_dir"]
            self.assertTrue(structures_dir.exists())
            structure_dicoms = sorted(structures_dir.glob("*.dcm"))
            self.assertGreater(len(structure_dicoms), 0)
            geometry_dicom_dir = case_dir / result["artifacts"]["brain_geometry_ct_2mm_series_dir"]
            geometry_dicoms = sorted(geometry_dicom_dir.glob("*.dcm"))
            self.assertGreater(len(geometry_dicoms), 11)
            self.assertEqual(
                result["measurement"]["normalization_brain_geometry_2mm"]["brain_geometry_frame"]["crop_margin_mm"],
                25.0,
            )
            self.assertIn(
                "skull.nii.gz",
                result["measurement"]["normalization_brain_geometry_2mm"]["brain_geometry_frame"]["crop_source"],
            )
            geometry_ds = pydicom.dcmread(str(geometry_dicoms[0]), stop_before_pixels=True)
            self.assertEqual(geometry_ds.Modality, "CT")
            self.assertEqual(geometry_ds.SeriesDescription, "Heimdallr Brain Geometry CT 2 mm")
            self.assertEqual([float(value) for value in geometry_ds.PixelSpacing], [0.6, 0.6])
            self.assertEqual(float(geometry_ds.SliceThickness), 2.0)
            self.assertEqual(float(geometry_ds.SpacingBetweenSlices), 1.0)
            structure_ds = pydicom.dcmread(str(structure_dicoms[0]), stop_before_pixels=True)
            self.assertEqual(structure_ds.Rows, geometry_ds.Rows)
            self.assertEqual(structure_ds.Columns, geometry_ds.Columns)
            self.assertEqual(int(geometry_ds.InstanceNumber), 1)
            self.assertEqual(int(geometry_ds.InStackPositionNumber), 1)
            center_dicoms = [
                pydicom.dcmread(str(path), stop_before_pixels=True)
                for path in geometry_dicoms
                if "brain center" in getattr(pydicom.dcmread(str(path), stop_before_pixels=True), "ImageComments", "")
            ]
            self.assertEqual(len(center_dicoms), 1)
            self.assertGreater(int(center_dicoms[0].InStackPositionNumber), 1)
            self.assertEqual(
                [round(float(value), 6) for value in geometry_ds.ImageOrientationPatient[3:]],
                [0.0, 1.0, 0.0],
            )
            row = np.asarray([float(value) for value in geometry_ds.ImageOrientationPatient[:3]])
            column = np.asarray([float(value) for value in geometry_ds.ImageOrientationPatient[3:]])
            self.assertGreater(float(np.cross(row, column)[2]), 0.0)
            self.assertEqual(geometry_ds.file_meta.TransferSyntaxUID, JPEGLSLossless)
            self.assertNotIn("cerebral_bleed_overlay_series_dir", result["artifacts"])

    def test_job_marks_truncated_skull_as_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseHeadTruncated_20260524_001"
            case_dir = tmp_path / case_id
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)
            (case_dir / "metadata" / "id.json").write_text(
                json.dumps({"CaseID": case_id, "Modality": "CT"}),
                encoding="utf-8",
            )

            shape = (10, 10, 8)
            ct = np.zeros(shape, dtype=np.float32)
            skull = np.zeros(shape, dtype=np.float32)
            brain = np.zeros(shape, dtype=np.float32)
            skull[0:8, 2:8, 1:7] = 1.0
            brain[3:7, 3:7, 2:6] = 1.0
            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct)
            write_nifti(case_dir / "artifacts" / "total" / "skull.nii.gz", skull)
            write_nifti(case_dir / "artifacts" / "total" / "brain.nii.gz", brain)

            with patch.object(settings, "STUDIES_DIR", tmp_path):
                with patch.object(
                    sys,
                    "argv",
                    ["head_complete_qc", "--case-id", case_id, "--job-config-json", "{}"],
                ):
                    self.assertEqual(head_complete_qc.main(), 0)

            result_path = case_dir / "artifacts" / "metrics" / "head_complete_qc" / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))

            self.assertEqual(
                result["measurement"]["job_status"],
                "incomplete_head_segmentation",
            )
            self.assertFalse(result["measurement"]["head_complete_without_truncation"])
            self.assertIn(
                "x_min",
                result["measurement"]["head_components"]["masks"]["skull"]["touched_bounds"],
            )

    def test_job_exports_positive_bleed_overlay_and_notification_bool(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseHeadBleed_20260524_001"
            case_dir = tmp_path / case_id
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)
            (case_dir / "artifacts" / "cerebral_bleed").mkdir(parents=True)
            (case_dir / "artifacts" / "brain_structures").mkdir(parents=True)
            (case_dir / "metadata" / "id.json").write_text(
                json.dumps(
                    {
                        "CaseID": case_id,
                        "Modality": "CT",
                        "StudyInstanceUID": "1.2.826.0.1.3680043.8.498.2001",
                    }
                ),
                encoding="utf-8",
            )

            shape = (18, 18, 14)
            ct = np.zeros(shape, dtype=np.float32)
            skull = np.zeros(shape, dtype=np.float32)
            brain = np.zeros(shape, dtype=np.float32)
            bleed = np.zeros(shape, dtype=np.float32)
            skull[2:16, 2:16, 2:12] = 1.0
            skull[4:14, 4:14, 4:10] = 0.0
            brain[5:13, 5:13, 4:10] = 1.0
            bleed[8:10, 8:10, 7:8] = 1.0
            ct[skull.astype(bool)] = 700.0
            ct[brain.astype(bool)] = 35.0
            ct[bleed.astype(bool)] = 70.0

            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct, spacing=(0.6, 0.6, 1.25))
            write_nifti(case_dir / "artifacts" / "total" / "skull.nii.gz", skull, spacing=(0.6, 0.6, 1.25))
            write_nifti(case_dir / "artifacts" / "total" / "brain.nii.gz", brain, spacing=(0.6, 0.6, 1.25))
            write_nifti(
                case_dir / "artifacts" / "cerebral_bleed" / "intracerebral_hemorrhage.nii.gz",
                bleed,
                spacing=(0.6, 0.6, 1.25),
            )
            for idx, mask_name in enumerate(BRAIN_STRUCTURE_MASKS):
                structure = np.zeros(shape, dtype=np.float32)
                x = 5 + (idx % 4)
                y = 5 + ((idx // 4) % 4)
                z = 4 + (idx % 5)
                structure[x : x + 2, y : y + 2, z : z + 2] = 1.0
                write_nifti(
                    case_dir / "artifacts" / "brain_structures" / f"{mask_name}.nii.gz",
                    structure,
                    spacing=(0.6, 0.6, 1.25),
                )

            with patch.object(settings, "STUDIES_DIR", tmp_path):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "head_complete_qc",
                        "--case-id",
                        case_id,
                        "--job-config-json",
                        json.dumps(
                            {
                                "target_plane": "axial",
                                "target_in_plane_spacing_mm": [1.0, 1.0],
                                "target_slice_thickness_mm": 5.0,
                                "overlay_slice_thickness_mm": 3.0,
                                "bleed_overlay_slice_thickness_mm": 5.0,
                            }
                        ),
                    ],
                ):
                    self.assertEqual(head_complete_qc.main(), 0)

            result_path = case_dir / "artifacts" / "metrics" / "head_complete_qc" / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))

            self.assertTrue(result["measurement"]["cerebral_bleed"]["has_cerebral_bleed"])
            self.assertTrue(result["measurement"]["cerebral_bleed"]["notification_bool"])
            bleed_dir = case_dir / result["artifacts"]["cerebral_bleed_overlay_series_dir"]
            bleed_dicoms = sorted(bleed_dir.glob("*.dcm"))
            self.assertGreaterEqual(len(bleed_dicoms), 2)
            self.assertTrue(
                any(
                    slab["contains_bleed"]
                    for slab in result["measurement"]["cerebral_bleed"]["overlay_exported_slabs"]
                )
            )
            bleed_ds = pydicom.dcmread(str(bleed_dicoms[0]), stop_before_pixels=True)
            self.assertEqual(bleed_ds.SeriesDescription, "Heimdallr Cerebral Bleed Overlay 5 mm")


if __name__ == "__main__":
    unittest.main()
