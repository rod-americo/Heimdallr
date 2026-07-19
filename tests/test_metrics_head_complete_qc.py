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

from heimdallr.metrics.head import (  # noqa: E402
    BRAIN_STRUCTURE_MASKS,
    compute_mask_status,
    normalize_nifti_to_brain_mask_geometry_isotropic,
)
from heimdallr.metrics.jobs import head_complete_qc  # noqa: E402
from heimdallr.shared import settings  # noqa: E402


def write_nifti(path: Path, data: np.ndarray, spacing=(1.0, 1.0, 1.0)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    affine = np.diag([spacing[0], spacing[1], spacing[2], 1.0])
    nib.save(nib.Nifti1Image(data.astype(np.float32), affine), str(path))


class TestHeadCompleteQcJob(unittest.TestCase):
    def test_brain_geometry_preserves_source_plane_and_uses_in_plane_pca(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            shape = (80, 80, 40)
            grid_x, grid_y, grid_z = np.indices(shape, dtype=np.float64)
            yaw_radians = np.radians(24.0)
            centered_x = grid_x - 39.5
            centered_y = grid_y - 39.5
            rotated_x = (np.cos(yaw_radians) * centered_x) + (
                np.sin(yaw_radians) * centered_y
            )
            rotated_y = (-np.sin(yaw_radians) * centered_x) + (
                np.cos(yaw_radians) * centered_y
            )
            brain = (
                (rotated_x / 18.0) ** 2
                + (rotated_y / 28.0) ** 2
                + ((grid_z - 19.5) / 13.0) ** 2
            ) <= 1.0
            ct = np.where(brain, 35.0, -1000.0).astype(np.float32)

            pitch_radians = np.radians(12.0)
            rotation_x = np.asarray(
                [
                    [1.0, 0.0, 0.0],
                    [0.0, np.cos(pitch_radians), -np.sin(pitch_radians)],
                    [0.0, np.sin(pitch_radians), np.cos(pitch_radians)],
                ],
                dtype=np.float64,
            )
            affine = np.eye(4, dtype=np.float64)
            affine[:3, :3] = rotation_x @ np.diag([0.6, 0.6, 1.25])
            source_path = tmp_path / "source.nii.gz"
            brain_path = tmp_path / "brain.nii.gz"
            output_path = tmp_path / "normalized.nii.gz"
            nib.save(nib.Nifti1Image(ct, affine), str(source_path))
            nib.save(nib.Nifti1Image(brain.astype(np.float32), affine), str(brain_path))

            result = normalize_nifti_to_brain_mask_geometry_isotropic(
                source_path,
                brain_path,
                output_path,
                crop_mask_path=None,
                voxel_size_mm=1.0,
            )

            frame = result["brain_geometry_frame"]
            expected_normal = affine[:3, 2] / np.linalg.norm(affine[:3, 2])
            actual_normal = np.asarray(frame["axes"]["superior_inferior"])
            self.assertGreater(float(np.dot(expected_normal, actual_normal)), 0.999999)
            self.assertEqual(result["target_orientation"], "brain_mask_source_axial_plane_pca")
            self.assertEqual(result["anatomic_alignment"]["status"], "brain_mask_in_plane")
            self.assertEqual(len(frame["in_plane_pca_eigenvalues"]), 2)
            self.assertAlmostEqual(abs(frame["in_plane_rotation_degrees"]), 24.0, delta=0.5)
            self.assertNotIn("midline_guide", frame)
            self.assertNotIn("pca_eigenvalues", frame)
            self.assertTrue(output_path.exists())

    def test_example_profiles_define_automatic_and_manual_head_workflows(self):
        metrics_config_path = ROOT / "config" / "metrics_pipeline.example.json"
        config = json.loads(metrics_config_path.read_text(encoding="utf-8"))
        automatic_jobs = {
            job["name"]: job
            for job in config["profiles"]["ct_automatic_metrics"]["jobs"]
        }
        head_jobs = {
            job["name"]: job
            for job in config["profiles"]["ct_head_complete_metrics"]["jobs"]
        }
        head_profile = config["profiles"]["ct_head_complete_metrics"]

        self.assertEqual(config["default_profile"], "ct_automatic_metrics")
        self.assertTrue(automatic_jobs["head_complete_qc"]["enabled"])
        self.assertTrue(automatic_jobs["head_complete_qc"]["automatic"])
        self.assertEqual(automatic_jobs["head_complete_qc"]["requires_inventory"], ["brain.complete"])
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
                        "ReferenceDicom": {
                            "SeriesDate": "20260520",
                            "SeriesTime": "101112.123",
                            "StudyDate": "20260519",
                            "StudyTime": "090000",
                        },
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
                                "locale": "en_US",
                                "target_plane": "axial",
                                "target_in_plane_spacing_mm": [1.0, 1.0],
                                "target_slice_thickness_mm": 5.0,
                                "derived_ct_transfer_syntax": "jpeg_ls_lossless",
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
                "brain_mask_in_plane",
            )
            self.assertEqual(
                result["measurement"]["normalization_brain_geometry_2mm"]["brain_mask"],
                "artifacts/total/brain.nii.gz",
            )
            self.assertEqual(
                result["measurement"]["normalization_brain_geometry_2mm"]["normalized_spacing_mm"]["z"],
                1.0,
            )
            geometry_frame = result["measurement"]["normalization_brain_geometry_2mm"][
                "brain_geometry_frame"
            ]
            self.assertEqual(len(geometry_frame["in_plane_pca_eigenvalues"]), 2)
            self.assertNotIn("midline_guide", geometry_frame)
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
            self.assertEqual(geometry_ds.SeriesDate, "20260520")
            self.assertEqual(geometry_ds.SeriesTime, "101112.123")
            self.assertNotEqual(geometry_ds.ContentDate, geometry_ds.SeriesDate)
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

    def test_job_allows_truncated_skull_when_brain_is_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseHeadTruncated_20260524_001"
            case_dir = tmp_path / case_id
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)
            (case_dir / "artifacts" / "cerebral_bleed").mkdir(parents=True)
            (case_dir / "artifacts" / "brain_structures").mkdir(parents=True)
            (case_dir / "metadata" / "id.json").write_text(
                json.dumps({"CaseID": case_id, "Modality": "CT"}),
                encoding="utf-8",
            )

            shape = (10, 10, 8)
            ct = np.zeros(shape, dtype=np.float32)
            skull = np.zeros(shape, dtype=np.float32)
            brain = np.zeros(shape, dtype=np.float32)
            bleed = np.zeros(shape, dtype=np.float32)
            skull[0:8, 2:8, 1:7] = 1.0
            brain[3:7, 3:7, 2:6] = 1.0
            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct)
            write_nifti(case_dir / "artifacts" / "total" / "skull.nii.gz", skull)
            write_nifti(case_dir / "artifacts" / "total" / "brain.nii.gz", brain)
            write_nifti(
                case_dir / "artifacts" / "cerebral_bleed" / "intracerebral_hemorrhage.nii.gz",
                bleed,
            )
            for idx, mask_name in enumerate(BRAIN_STRUCTURE_MASKS):
                structure = np.zeros(shape, dtype=np.float32)
                x = 3 + (idx % 2)
                y = 3 + ((idx // 2) % 2)
                z = 2 + (idx % 2)
                structure[x : x + 2, y : y + 2, z : z + 2] = 1.0
                write_nifti(
                    case_dir / "artifacts" / "brain_structures" / f"{mask_name}.nii.gz",
                    structure,
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
                                "emit_secondary_capture_dicom": False,
                                "emit_brain_geometry_dicom_series": False,
                            }
                        ),
                    ],
                ):
                    self.assertEqual(head_complete_qc.main(), 0)

            result_path = case_dir / "artifacts" / "metrics" / "head_complete_qc" / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))

            self.assertTrue(result["measurement"]["head_complete_without_truncation"])
            self.assertTrue(result["measurement"]["brain_complete_without_truncation"])
            self.assertIn(
                "x_min",
                result["measurement"]["head_components"]["masks"]["skull"]["touched_bounds"],
            )
            self.assertEqual(
                result["measurement"]["head_components"]["masks"]["brain"]["status"],
                "complete",
            )
            self.assertIn("normalization_brain_geometry_2mm", result["measurement"])

    def test_job_omits_incomplete_brain_structures_from_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseHeadIncompleteStructures_20260525_001"
            case_dir = tmp_path / case_id
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)
            (case_dir / "artifacts" / "cerebral_bleed").mkdir(parents=True)
            (case_dir / "artifacts" / "brain_structures").mkdir(parents=True)
            (case_dir / "metadata" / "id.json").write_text(
                json.dumps({"CaseID": case_id, "Modality": "CT"}),
                encoding="utf-8",
            )

            shape = (12, 12, 8)
            ct = np.zeros(shape, dtype=np.float32)
            skull = np.zeros(shape, dtype=np.float32)
            brain = np.zeros(shape, dtype=np.float32)
            bleed = np.zeros(shape, dtype=np.float32)
            skull[1:11, 1:11, 1:7] = 1.0
            brain[3:9, 3:9, 2:6] = 1.0
            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct)
            write_nifti(case_dir / "artifacts" / "total" / "skull.nii.gz", skull)
            write_nifti(case_dir / "artifacts" / "total" / "brain.nii.gz", brain)
            write_nifti(
                case_dir / "artifacts" / "cerebral_bleed" / "intracerebral_hemorrhage.nii.gz",
                bleed,
            )
            for idx, mask_name in enumerate(BRAIN_STRUCTURE_MASKS):
                structure = np.zeros(shape, dtype=np.float32)
                if idx == 0:
                    structure[3:6, 3:6, 0:3] = 1.0
                else:
                    structure[4:6, 4:6, 3:5] = 1.0
                write_nifti(
                    case_dir / "artifacts" / "brain_structures" / f"{mask_name}.nii.gz",
                    structure,
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
                                "emit_secondary_capture_dicom": True,
                                "emit_brain_geometry_dicom_series": True,
                            }
                        ),
                    ],
                ):
                    self.assertEqual(head_complete_qc.main(), 0)

            result_path = case_dir / "artifacts" / "metrics" / "head_complete_qc" / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))

            self.assertEqual(result["measurement"]["job_status"], "partial_brain_structures")
            self.assertTrue(result["measurement"]["brain_complete_without_truncation"])
            self.assertTrue(result["measurement"]["required_segmentation_complete"])
            self.assertFalse(result["measurement"]["brain_structures"]["complete"])
            self.assertIn("brainstem", result["measurement"]["brain_structures"]["incomplete"])
            self.assertIn("normalization_brain_geometry_2mm", result["measurement"])
            self.assertIn(
                {"name": "brainstem", "status": "truncated", "touched_bounds": ["z_min"]},
                result["measurement"]["omitted_brain_structures"],
            )
            self.assertNotIn("brainstem", result["measurement"]["usable_brain_structures"])
            volume_keys = [
                row["key"]
                for row in result["measurement"]["brain_structure_volumes"]["rows"]
            ]
            self.assertIn("brain_total", volume_keys)
            self.assertNotIn("brainstem", volume_keys)
            self.assertIn("brain_geometry_ct_2mm_series_dir", result["artifacts"])
            self.assertIn("brain_structures_overlay_series_dir", result["artifacts"])
            self.assertGreater(len(result["dicom_exports"]), 0)
            metric_dir = case_dir / "artifacts" / "metrics" / "head_complete_qc"
            self.assertTrue((metric_dir / "brain_geometry_ct_2mm_dicom").exists())
            self.assertTrue((metric_dir / "brain_structures_dicom").exists())

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
                                "locale": "en_US",
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

    def test_job_rejects_bleed_signal_outside_skull_support(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseHeadBleedOutsideSkull_20260524_001"
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
                        "StudyInstanceUID": "1.2.826.0.1.3680043.8.498.2002",
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
            bleed[0:1, 0:1, 0:1] = 1.0
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
            bleed_measurement = result["measurement"]["cerebral_bleed"]

            self.assertTrue(bleed_measurement["raw_has_cerebral_bleed"])
            self.assertFalse(bleed_measurement["has_cerebral_bleed"])
            self.assertFalse(bleed_measurement["notification_bool"])
            self.assertEqual(
                bleed_measurement["anatomic_support_qc"]["status"],
                "rejected_outside_skull_support",
            )
            self.assertGreater(
                bleed_measurement["anatomic_support_qc"]["outside_support_voxel_count"],
                0,
            )
            self.assertNotIn("cerebral_bleed_overlay_series_dir", result["artifacts"])


if __name__ == "__main__":
    unittest.main()
