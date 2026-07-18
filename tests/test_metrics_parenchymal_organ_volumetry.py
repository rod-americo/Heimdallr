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

from heimdallr.metrics.jobs import parenchymal_organ_volumetry  # noqa: E402
from heimdallr.metrics.jobs._parenchymal_overlay_text import (  # noqa: E402
    OverlayTextLine,
    build_overlay_lines,
    build_overlay_text,
)
from heimdallr.shared import settings  # noqa: E402


def write_nifti(path: Path, data: np.ndarray, spacing=(1.0, 1.0, 1.0)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    affine = np.diag([spacing[0], spacing[1], spacing[2], 1.0])
    nib.save(nib.Nifti1Image(data.astype(np.float32), affine), str(path))


class TestParenchymalOrganVolumetryJob(unittest.TestCase):
    def test_renderer_uses_uniform_body_line_spacing(self):
        lines = [
            OverlayTextLine("Órgãos parenquimatosos:"),
            OverlayTextLine("Fígado: 1.349 cm³ | 57 UH"),
            OverlayTextLine("Esteatose: não"),
            OverlayTextLine("Baço: 131 cm³ | 45 UH"),
        ]
        y_positions = []
        original_draw = parenchymal_organ_volumetry.ImageDraw.Draw

        class RecordingDraw:
            def __init__(self, image, mode=None):
                self._draw = original_draw(image, mode=mode)

            def __getattr__(self, name):
                return getattr(self._draw, name)

            def text(self, xy, text, *args, **kwargs):
                y_positions.append((text, xy[1]))
                return self._draw.text(xy, text, *args, **kwargs)

        with patch.object(parenchymal_organ_volumetry.ImageDraw, "Draw", RecordingDraw):
            parenchymal_organ_volumetry._render_slice_rgb(
                np.zeros((128, 256), dtype=np.float32),
                [],
                lines,
                source_axis_codes=("R", "A"),
            )

        line_texts = {line.text for line in lines[1:]}
        body_y = [y for text, y in y_positions if text in line_texts]
        self.assertEqual(body_y[1] - body_y[0], body_y[2] - body_y[1])

    def test_renderer_draws_alert_volume_span_in_red(self):
        lines = build_overlay_lines(
            organ_measurements={
                "liver": {
                    "analysis_status": "complete",
                    "observed_volume_cm3": 1801.0,
                    "hu_mean": 49.0,
                },
            },
            locale="en_US",
        )

        rgb = parenchymal_organ_volumetry._render_slice_rgb(
            np.zeros((256, 256), dtype=np.float32),
            [],
            lines,
            source_axis_codes=("R", "A"),
        )

        red_pixels = (rgb[:, :, 0] > 245) & (rgb[:, :, 1] < 110) & (rgb[:, :, 2] < 110)
        self.assertTrue(np.any(red_pixels))

    def test_truncated_mask_does_not_report_volume_or_overlay_text(self):
        shape = (8, 8, 6)
        ct = np.zeros(shape, dtype=np.float32)
        mask = np.zeros(shape, dtype=bool)
        mask[2:6, 2:6, 0:4] = True
        ct[mask] = 42.0

        measurement = parenchymal_organ_volumetry._compute_mask_measurement(
            "liver",
            "Liver",
            mask,
            ct,
            (1.0, 1.0, 1.0),
        )

        self.assertEqual(measurement["analysis_status"], "incomplete")
        self.assertFalse(measurement["complete"])
        self.assertTrue(measurement["truncated_at_scan_bounds"])
        self.assertEqual(measurement["attenuation_sample_volume_cm3"], 0.064)
        self.assertEqual(measurement["attenuation_sample_slice_count"], 4)
        self.assertEqual(measurement["attenuation_sample_axial_extent_mm"], 4.0)
        self.assertIsNone(measurement["observed_volume_cm3"])
        self.assertIsNone(measurement["volume_cm3"])

        lines = build_overlay_text(
            organ_measurements={"liver": measurement},
            locale="en_US",
        )
        self.assertEqual(lines, ["Parenchymal organs:"])

    def test_ambiguous_multiple_kidney_components_withhold_aggregate_payload(self):
        shape = (24, 24, 24)
        spacing = (10.0, 10.0, 10.0)
        affine = np.diag([10.0, 10.0, 10.0, 1.0])
        ct = np.zeros(shape, dtype=np.float32)
        kidney_right = np.zeros(shape, dtype=bool)
        kidney_right[3:7, 14:18, 16:22] = True
        kidney_right[14:19, 4:9, 3:9] = True
        organ_masks = {
            "kidney_right": kidney_right,
            "kidney_left": None,
        }
        organ_measurements = {
            "kidney_right": parenchymal_organ_volumetry._compute_mask_measurement(
                "kidney_right",
                "Right kidney",
                kidney_right,
                ct,
                spacing,
            ),
            "kidney_left": parenchymal_organ_volumetry._compute_mask_measurement(
                "kidney_left",
                "Left kidney",
                None,
                ct,
                spacing,
            ),
        }

        renal_qc, _overlay_components = (
            parenchymal_organ_volumetry._apply_renal_anatomy_measurements(
                organ_masks,
                organ_measurements,
                ct,
                affine,
                spacing,
                {},
                suppress_density=False,
            )
        )

        right = organ_measurements["kidney_right"]
        self.assertEqual(right["analysis_status"], "ambiguous_multiple_components")
        self.assertIsNone(right["volume_cm3"])
        self.assertEqual(right["raw_mask_volume_cm3"], 246.0)
        self.assertEqual(right["raw_mask_voxel_count"], 246)
        self.assertFalse(right["native_component_identified"])
        self.assertTrue(renal_qc["multiple_significant_components"])

    def test_partial_liver_sample_generates_attenuation_only_overlay(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseParenchyma_Partial_20260710_001"
            case_dir = tmp_path / case_id
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)

            id_payload = {
                "CaseID": case_id,
                "Modality": "CT",
                "StudyInstanceUID": "1.2.826.0.1.3680043.8.498.5",
                "PatientName": "Test^Patient",
                "PatientID": "P005",
                "KVP": 120,
                "Pipeline": {"series_selection": {"SelectedPhase": "native"}},
            }
            (case_dir / "metadata" / "id.json").write_text(json.dumps(id_payload), encoding="utf-8")
            (case_dir / "metadata" / "metadata.json").write_text(
                json.dumps(id_payload),
                encoding="utf-8",
            )
            (case_dir / "metadata" / "resultados.json").write_text("{}", encoding="utf-8")

            shape = (10, 10, 10)
            ct = np.zeros(shape, dtype=np.float32)
            liver = np.zeros(shape, dtype=np.float32)
            liver[2:7, 2:7, 0:4] = 1.0
            ct[liver.astype(bool)] = 55.0
            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct, spacing=(10.0, 10.0, 10.0))
            write_nifti(
                case_dir / "artifacts" / "total" / "liver.nii.gz",
                liver,
                spacing=(10.0, 10.0, 10.0),
            )

            with patch.object(settings, "STUDIES_DIR", tmp_path):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "parenchymal_organ_volumetry",
                        "--case-id",
                        case_id,
                        "--job-config-json",
                        '{"generate_overlay": true, "emit_secondary_capture_dicom": true, "locale": "pt_BR"}',
                    ],
                ):
                    self.assertEqual(parenchymal_organ_volumetry.main(), 0)

            result_path = case_dir / "artifacts" / "metrics" / "parenchymal_organ_volumetry" / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))
            assessment = result["measurement"]["hepatic_steatosis"]

            self.assertEqual(result["status"], "done")
            self.assertEqual(result["measurement"]["job_status"], "attenuation_only")
            self.assertIsNone(result["measurement"]["organs"]["liver"]["volume_cm3"])
            self.assertEqual(assessment["status"], "normal")
            self.assertTrue(assessment["partial_coverage"])
            self.assertEqual(len(result["dicom_exports"]), 8)
            lines = build_overlay_text(
                organ_measurements=result["measurement"]["organs"],
                locale="pt_BR",
                hepatic_steatosis=assessment,
            )
            self.assertEqual(lines, ["Órgãos parenquimatosos:", "Esteatose: não (cobertura parcial)"])

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
                "KVP": 120,
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
                        (
                            '{"generate_overlay": true, '
                            '"emit_secondary_capture_dicom": true, '
                            '"secondary_capture_max_dimension": 64, '
                            '"secondary_capture_transfer_syntax": "jpeg_ls_lossless", '
                            '"locale": "en_US"}'
                        ),
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
            self.assertAlmostEqual(result["measurement"]["organs"]["liver"]["hu_mean"], 47.92, places=2)
            self.assertAlmostEqual(
                result["measurement"]["organs"]["liver"]["estimated_pdff_percent"],
                10.41,
                places=2,
            )
            self.assertEqual(result["measurement"]["hepatic_steatosis"]["status"], "normal")
            self.assertTrue(result["measurement"]["organs"]["pancreas"]["complete"])

            first_dicom = case_dir / result["dicom_exports"][0]["path"]
            ds = pydicom.dcmread(str(first_dicom))
            self.assertEqual(str(ds.SeriesDescription), "Heimdallr Parenchymal Organ Overlay 5 mm")
            self.assertIn("5 mm axial reconstruction", str(ds.DerivationDescription))
            self.assertEqual(int(ds.InstanceNumber), 1)
            self.assertEqual(str(ds.Modality), "OT")
            self.assertLessEqual(max(int(ds.Rows), int(ds.Columns)), 64)
            self.assertEqual(ds.file_meta.TransferSyntaxUID, JPEGLSLossless)

    def test_transplant_pattern_separates_native_and_pelvic_renal_components(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseParenchyma_RenalAllograft_20260718_001"
            case_dir = tmp_path / case_id
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)

            id_payload = {
                "CaseID": case_id,
                "Modality": "CT",
                "StudyInstanceUID": "1.2.826.0.1.3680043.8.498.18",
                "PatientName": "Test^Patient",
                "PatientID": "P018",
                "Pipeline": {"series_selection": {"SelectedPhase": "native"}},
            }
            (case_dir / "metadata" / "id.json").write_text(
                json.dumps(id_payload),
                encoding="utf-8",
            )
            (case_dir / "metadata" / "metadata.json").write_text(
                json.dumps(id_payload),
                encoding="utf-8",
            )
            (case_dir / "metadata" / "resultados.json").write_text("{}", encoding="utf-8")

            shape = (24, 24, 24)
            spacing = (10.0, 10.0, 10.0)
            ct = np.zeros(shape, dtype=np.float32)
            kidney_right = np.zeros(shape, dtype=np.float32)
            kidney_right[3:7, 14:18, 16:22] = 1.0
            kidney_right[14:19, 4:9, 3:9] = 1.0
            kidney_left = np.zeros(shape, dtype=np.float32)
            kidney_left[16:20, 14:18, 15:22] = 1.0
            vertebra_l3 = np.zeros(shape, dtype=np.float32)
            vertebra_l3[9:15, 9:15, 13:16] = 1.0
            vertebra_l4 = np.zeros(shape, dtype=np.float32)
            vertebra_l4[9:15, 9:15, 9:12] = 1.0
            ct[kidney_right.astype(bool)] = 40.0
            ct[3:7, 14:18, 16:22] = 20.0
            ct[kidney_left.astype(bool)] = 25.0

            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct, spacing=spacing)
            write_nifti(
                case_dir / "artifacts" / "total" / "kidney_right.nii.gz",
                kidney_right,
                spacing=spacing,
            )
            write_nifti(
                case_dir / "artifacts" / "total" / "kidney_left.nii.gz",
                kidney_left,
                spacing=spacing,
            )
            write_nifti(
                case_dir / "artifacts" / "total" / "vertebrae_L3.nii.gz",
                vertebra_l3,
                spacing=spacing,
            )
            write_nifti(
                case_dir / "artifacts" / "total" / "vertebrae_L4.nii.gz",
                vertebra_l4,
                spacing=spacing,
            )

            with patch.object(settings, "STUDIES_DIR", tmp_path):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "parenchymal_organ_volumetry",
                        "--case-id",
                        case_id,
                        "--job-config-json",
                        (
                            '{"generate_overlay": true, '
                            '"emit_secondary_capture_dicom": true, '
                            '"secondary_capture_max_dimension": 256, '
                            '"locale": "pt_BR"}'
                        ),
                    ],
                ):
                    self.assertEqual(parenchymal_organ_volumetry.main(), 0)

            result_path = (
                case_dir
                / "artifacts"
                / "metrics"
                / "parenchymal_organ_volumetry"
                / "result.json"
            )
            result = json.loads(result_path.read_text(encoding="utf-8"))
            measurement = result["measurement"]
            right = measurement["organs"]["kidney_right"]
            left = measurement["organs"]["kidney_left"]
            renal_qc = measurement["renal_anatomy_qc"]

            self.assertEqual(result["status"], "done")
            self.assertEqual(right["volume_cm3"], 96.0)
            self.assertEqual(right["raw_mask_volume_cm3"], 246.0)
            self.assertEqual(right["hu_mean"], 20.0)
            self.assertEqual(left["volume_cm3"], 112.0)
            self.assertTrue(renal_qc["multiple_significant_components"])
            self.assertTrue(renal_qc["suspected_allograft"])
            self.assertEqual(
                renal_qc["kidneys"]["kidney_right"]["classification_status"],
                "native_and_suspected_allograft",
            )
            self.assertEqual(
                renal_qc["suspected_renal_allografts"][0]["volume_cm3"],
                150.0,
            )
            lines = build_overlay_lines(
                organ_measurements=measurement["organs"],
                locale="pt_BR",
                renal_anatomy_qc=renal_qc,
            )
            self.assertIn("Rim direito: 96 cm³ | 20 UH", [line.text for line in lines])
            self.assertIn(
                "Provável enxerto renal direito: 150 cm³",
                [line.text for line in lines],
            )
            alert_values = {
                line.text[line.alert_span[0] : line.alert_span[1]]
                for line in lines
                if line.alert_span is not None
            }
            self.assertEqual(alert_values, {"96"})
            self.assertGreater(len(result["dicom_exports"]), 0)
            self.assertTrue(all((case_dir / item["path"]).exists() for item in result["dicom_exports"]))

    def test_job_skips_artifacts_when_no_organ_has_volume(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseParenchyma_Truncated_20260404_001"
            case_dir = tmp_path / case_id
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)

            id_payload = {
                "CaseID": case_id,
                "Modality": "CT",
                "StudyInstanceUID": "1.2.826.0.1.3680043.8.498.3",
                "PatientName": "Test^Patient",
                "PatientID": "P003",
                "Pipeline": {"series_selection": {"SelectedPhase": "native"}},
            }
            (case_dir / "metadata" / "id.json").write_text(json.dumps(id_payload), encoding="utf-8")
            (case_dir / "metadata" / "metadata.json").write_text(json.dumps(id_payload), encoding="utf-8")
            (case_dir / "metadata" / "resultados.json").write_text("{}", encoding="utf-8")

            shape = (12, 12, 8)
            ct = np.zeros(shape, dtype=np.float32)
            ct[2:8, 2:8, 0:5] = 55.0
            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct, spacing=(1.0, 1.0, 1.0))

            liver = np.zeros(shape, dtype=np.float32)
            liver[2:8, 2:8, 0:5] = 1.0
            write_nifti(case_dir / "artifacts" / "total" / "liver.nii.gz", liver)

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

            metric_dir = case_dir / "artifacts" / "metrics" / "parenchymal_organ_volumetry"
            result_path = metric_dir / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))

            self.assertEqual(result["status"], "skipped")
            self.assertEqual(result["measurement"]["job_status"], "no_complete_organ_volume")
            self.assertEqual(result["dicom_exports"], [])
            self.assertNotIn("overlay_series_dir", result["artifacts"])
            self.assertFalse((metric_dir / "dicom").exists())
            self.assertIsNone(result["measurement"]["organs"]["liver"]["volume_cm3"])
            self.assertTrue(result["measurement"]["organs"]["liver"]["truncated_at_scan_bounds"])

    def test_job_writes_l1_overlay_without_organ_volume(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseParenchyma_L1Overlay_20260404_001"
            case_dir = tmp_path / case_id
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)

            id_payload = {
                "CaseID": case_id,
                "Modality": "CT",
                "StudyInstanceUID": "1.2.826.0.1.3680043.8.498.4",
                "PatientName": "Test^Patient",
                "PatientID": "P004",
                "Pipeline": {"series_selection": {"SelectedPhase": "native"}},
            }
            (case_dir / "metadata" / "id.json").write_text(json.dumps(id_payload), encoding="utf-8")
            (case_dir / "metadata" / "metadata.json").write_text(json.dumps(id_payload), encoding="utf-8")
            (case_dir / "metadata" / "resultados.json").write_text("{}", encoding="utf-8")

            shape = (12, 12, 8)
            ct = np.zeros(shape, dtype=np.float32)
            ct[4:8, 4:8, 2:6] = 300.0
            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct, spacing=(1.0, 1.0, 1.0))

            l1 = np.zeros(shape, dtype=np.float32)
            l1[4:8, 4:8, 2:6] = 1.0
            write_nifti(case_dir / "artifacts" / "total" / "vertebrae_L1.nii.gz", l1)

            with patch.object(settings, "STUDIES_DIR", tmp_path):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "parenchymal_organ_volumetry",
                        "--case-id",
                        case_id,
                        "--job-config-json",
                        (
                            '{"generate_overlay": true, '
                            '"emit_secondary_capture_dicom": true, '
                            '"secondary_capture_max_dimension": 64}'
                        ),
                    ],
                ):
                    self.assertEqual(parenchymal_organ_volumetry.main(), 0)

            result_path = case_dir / "artifacts" / "metrics" / "parenchymal_organ_volumetry" / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))

            self.assertEqual(result["status"], "done")
            self.assertEqual(result["measurement"]["job_status"], "overlay_only")
            self.assertEqual(result["measurement"]["organs"]["liver"]["analysis_status"], "missing")
            l1_measurement = result["measurement"]["overlay_only_masks"]["vertebra_l1"]
            self.assertTrue(l1_measurement["complete"])
            self.assertTrue(l1_measurement["included_in_overlay"])
            self.assertEqual(l1_measurement["measurement_role"], "overlay_only")
            self.assertGreater(result["measurement"]["exported_slice_count"], 0)
            self.assertEqual(len(result["dicom_exports"]), result["measurement"]["exported_slice_count"])
            self.assertIn("overlay_series_dir", result["artifacts"])

    def test_job_suppresses_hu_outputs_for_contrast_series(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseParenchyma_Contrast_20260404_001"
            case_dir = tmp_path / case_id
            (case_dir / "metadata").mkdir(parents=True)
            (case_dir / "derived").mkdir(parents=True)
            (case_dir / "artifacts" / "total").mkdir(parents=True)

            id_payload = {
                "CaseID": case_id,
                "Modality": "CT",
                "StudyInstanceUID": "1.2.826.0.1.3680043.8.498.2",
                "PatientName": "Test^Patient",
                "PatientID": "P002",
                "Pipeline": {"series_selection": {"SelectedPhase": "arterial"}},
            }
            (case_dir / "metadata" / "id.json").write_text(json.dumps(id_payload), encoding="utf-8")
            (case_dir / "metadata" / "metadata.json").write_text(json.dumps(id_payload), encoding="utf-8")
            (case_dir / "metadata" / "resultados.json").write_text("{}", encoding="utf-8")

            shape = (12, 12, 8)
            ct = np.zeros(shape, dtype=np.float32)
            ct[2:8, 2:8, 2:6] = 60.0
            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct, spacing=(1.0, 1.0, 1.0))

            liver = np.zeros(shape, dtype=np.float32)
            liver[2:8, 2:8, 2:6] = 1.0
            write_nifti(case_dir / "artifacts" / "total" / "liver.nii.gz", liver)

            with patch.object(settings, "STUDIES_DIR", tmp_path):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "parenchymal_organ_volumetry",
                        "--case-id",
                        case_id,
                        "--job-config-json",
                        '{"generate_overlay": true, "emit_secondary_capture_dicom": false}',
                    ],
                ):
                    self.assertEqual(parenchymal_organ_volumetry.main(), 0)

            result_path = case_dir / "artifacts" / "metrics" / "parenchymal_organ_volumetry" / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))

            self.assertTrue(result["measurement"]["density_suppressed_due_to_contrast"])
            self.assertIsNone(result["measurement"]["organs"]["liver"]["hu_mean"])
            self.assertIsNone(result["measurement"]["organs"]["liver"]["hu_std"])
            self.assertIsNone(result["measurement"]["organs"]["liver"]["estimated_pdff_percent"])


if __name__ == "__main__":
    unittest.main()
