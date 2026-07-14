import unittest
import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import nibabel as nib
import numpy as np
import pydicom

from heimdallr.metrics.jobs import l3_muscle_area
from heimdallr.metrics.jobs.l3_muscle_area import (
    MetricSkip,
    _overlay_display_directions,
    calculate_mask_hu_statistics,
    build_skip_payload,
    centered_slab_bounds,
    compute_center_slice,
    render_overlay_rgb,
    sagittal_plane_from_mask,
    sagittal_slab_from_mask,
)
from heimdallr.shared import settings


def write_nifti(path: Path, data: np.ndarray, spacing=(1.0, 1.0, 1.0)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    affine = np.diag([spacing[0], spacing[1], spacing[2], 1.0])
    nib.save(nib.Nifti1Image(data.astype(np.float32), affine), str(path))


class TestL3MuscleAreaJob(unittest.TestCase):
    def test_centered_slab_bounds_prefers_odd_slice_count(self):
        start, end = centered_slab_bounds(center_index=10, axis_len=40, spacing_mm=1.0, slab_thickness_mm=3.0)

        self.assertEqual((start, end), (9, 12))

    def test_sagittal_plane_from_mask_uses_left_right_axis(self):
        mask = np.zeros((12, 18, 10), dtype=bool)
        mask[4:7, 3:13, 2:8] = True

        plane, index, axis = sagittal_plane_from_mask(mask)

        self.assertEqual(axis, "x")
        self.assertEqual(index, 5)
        self.assertEqual(plane.shape, (18, 10))
        self.assertTrue(plane.any())

    def test_sagittal_plane_from_mask_still_uses_x_for_wide_vertebra(self):
        mask = np.zeros((20, 12, 14), dtype=bool)
        mask[4:16, 5:8, 3:11] = True

        plane, index, axis = sagittal_plane_from_mask(mask)

        self.assertEqual(axis, "x")
        self.assertEqual(index, 10)
        self.assertEqual(plane.shape, (12, 14))
        self.assertTrue(plane.any())

    def test_sagittal_slab_from_mask_projects_three_millimeter_slab(self):
        image = np.zeros((12, 18, 10), dtype=np.float32)
        mask = np.zeros_like(image, dtype=bool)
        mask[4:7, 3:13, 2:8] = True

        _, plane_index, axis = sagittal_plane_from_mask(mask)
        sagittal_ct, sagittal_mask, slab_bounds, lateral_spacing = sagittal_slab_from_mask(
            image_data=image,
            mask=mask,
            plane_index=plane_index,
            axis=axis,
            spacing_mm=(1.0, 1.0, 2.5),
            slab_thickness_mm=3.0,
        )

        self.assertEqual(slab_bounds, (4, 7))
        self.assertEqual(lateral_spacing, 1.0)
        self.assertEqual(sagittal_ct.shape, (18, 10))
        self.assertEqual(sagittal_mask.shape, (18, 10))
        self.assertTrue(sagittal_mask.any())

    def test_sagittal_slab_from_mask_keeps_full_width_for_y_axis(self):
        image = np.zeros((20, 12, 14), dtype=np.float32)
        mask = np.zeros_like(image, dtype=bool)
        mask[4:16, 5:8, 3:11] = True

        sagittal_ct, sagittal_mask, slab_bounds, lateral_spacing = sagittal_slab_from_mask(
            image_data=image,
            mask=mask,
            plane_index=6,
            axis="y",
            spacing_mm=(1.0, 1.0, 2.5),
            slab_thickness_mm=3.0,
        )

        self.assertEqual(slab_bounds, (5, 8))
        self.assertEqual(lateral_spacing, 1.0)
        self.assertEqual(sagittal_ct.shape, (20, 14))
        self.assertEqual(sagittal_mask.shape, (20, 14))
        self.assertTrue(sagittal_mask.any())

    def test_compute_center_slice_raises_skip_when_l3_is_empty(self):
        mask = np.zeros((12, 18, 10), dtype=bool)

        with self.assertRaisesRegex(MetricSkip, "L3 mask is empty"):
            compute_center_slice(mask)

    def test_build_skip_payload_marks_job_as_skipped(self):
        payload = build_skip_payload(
            case_id="Case1",
            reason="L3 mask not available for this study",
            result_relpath="artifacts/metrics/l3_muscle_area/result.json",
            inputs={
                "canonical_nifti": "derived/case.nii.gz",
                "vertebra_l3_mask": "artifacts/total/vertebrae_L3.nii.gz",
                "skeletal_muscle_mask": "artifacts/tissue_types/skeletal_muscle.nii.gz",
            },
        )

        self.assertEqual(payload["status"], "skipped")
        self.assertEqual(payload["measurement"]["job_status"], "skipped")
        self.assertEqual(payload["skip_reason"], "L3 mask not available for this study")
        self.assertEqual(
            payload["artifacts"]["result_json"],
            "artifacts/metrics/l3_muscle_area/result.json",
        )

    def test_calculate_mask_hu_statistics_uses_full_mask_area(self):
        image = np.array(
            [
                [10.0, 20.0, 30.0],
                [40.0, 50.0, 60.0],
            ],
            dtype=np.float32,
        )
        mask = np.array(
            [
                [False, True, True],
                [False, True, False],
            ],
            dtype=bool,
        )

        stats = calculate_mask_hu_statistics(image, mask)

        self.assertEqual(stats["voxel_count"], 3)
        self.assertEqual(stats["mean_hu"], 33.33)
        self.assertEqual(stats["std_hu"], 12.47)

    def test_l3_muscle_area_excludes_appendicular_muscle_component(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            case_id = "CaseL3_20260526_001"
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
                        "StudyInstanceUID": "1.2.3.4.6",
                        "PatientSize": 1.70,
                        "PatientWeight": 80,
                        "Pipeline": {"series_selection": {"SelectedPhase": "native"}},
                    }
                ),
                encoding="utf-8",
            )

            ct = np.full((24, 20, 16), 50.0, dtype=np.float32)
            l3 = np.zeros_like(ct, dtype=np.float32)
            muscle = np.zeros_like(ct, dtype=np.float32)
            humerus = np.zeros_like(ct, dtype=np.float32)
            l3[8:14, 5:15, 6:10] = 1.0
            muscle[9:13, 8:12, 8] = 1.0
            muscle[1:4, 2:5, 8] = 1.0
            humerus[2:3, 3:4, 8] = 1.0

            write_nifti(case_dir / "derived" / f"{case_id}.nii.gz", ct, spacing=(1.0, 1.0, 2.5))
            write_nifti(case_dir / "artifacts" / "total" / "vertebrae_L3.nii.gz", l3)
            write_nifti(case_dir / "artifacts" / "total" / "humerus_left.nii.gz", humerus)
            write_nifti(case_dir / "artifacts" / "tissue_types" / "skeletal_muscle.nii.gz", muscle)

            with patch.object(settings, "STUDIES_DIR", tmp_path):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "l3_muscle_area",
                        "--case-id",
                        case_id,
                        "--job-config-json",
                        '{"emit_secondary_capture_dicom": false, "appendicular_muscle_exclusion_margin_mm": 2.0}',
                    ],
                ):
                    self.assertEqual(l3_muscle_area.main(), 0)

            result = json.loads((case_dir / "artifacts" / "metrics" / "l3_muscle_area" / "result.json").read_text(encoding="utf-8"))
            measurement = result["measurement"]
            self.assertEqual(result["status"], "done")
            self.assertGreater(measurement["raw_muscle_pixels"], measurement["muscle_pixels"])
            self.assertEqual(measurement["excluded_appendicular_muscle_pixels"], 9)
            self.assertTrue(measurement["appendicular_muscle_filter"]["applied"])

            with patch.object(settings, "STUDIES_DIR", tmp_path), patch.object(
                l3_muscle_area,
                "sagittal_plane_from_mask",
                side_effect=AssertionError("single-series DICOM must not render sagittal data"),
            ):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "l3_muscle_area",
                        "--case-id",
                        case_id,
                        "--job-config-json",
                        json.dumps(
                            {
                                "emit_secondary_capture_dicom": True,
                                "secondary_capture_series_mode": "single_series",
                                "secondary_capture_transfer_syntax": "original",
                            }
                        ),
                    ],
                ):
                    self.assertEqual(l3_muscle_area.main(), 0)

            result = json.loads(
                (
                    case_dir
                    / "artifacts"
                    / "metrics"
                    / "l3_muscle_area"
                    / "result.json"
                ).read_text(encoding="utf-8")
            )
            dataset = pydicom.dcmread(case_dir / result["artifacts"]["overlay_sc_dcm"])
            self.assertEqual(int(dataset.Rows), int(dataset.Columns))

    def test_render_overlay_rgb_returns_combined_axial_and_sagittal_image(self):
        image = np.linspace(-200.0, 250.0, num=24 * 20 * 16, dtype=np.float32).reshape((24, 20, 16))
        l3_mask = np.zeros_like(image, dtype=bool)
        muscle_mask = np.zeros_like(image, dtype=bool)
        l3_mask[8:14, 5:15, 6:10] = True
        muscle_mask[9:13, 7:13, 8] = True

        rendered = render_overlay_rgb(
            image_data=image,
            ct_affine=np.diag([1.0, 1.0, 2.5, 1.0]),
            l3_mask=l3_mask,
            muscle_mask=muscle_mask,
            slice_idx=8,
            title="L3 Center Slice",
            summary_lines=["SMA: 42.0 cm2", "Slice: 8"],
            panel_titles=("Axial", "Sagittal Reference"),
            sagittal_level_text="Axial level z=8 | slab 3 mm",
            spacing_mm=(1.0, 1.0, 2.5),
        )

        self.assertEqual(rendered.ndim, 3)
        self.assertEqual(rendered.shape[2], 3)
        self.assertGreater(rendered.shape[1], rendered.shape[0])
        self.assertGreater(int(rendered.max()), int(rendered.min()))

        with patch.object(
            l3_muscle_area,
            "sagittal_plane_from_mask",
            side_effect=AssertionError("axial renderer must not load sagittal data"),
        ):
            axial_rendered = render_overlay_rgb(
                image_data=image,
                ct_affine=np.diag([1.0, 1.0, 2.5, 1.0]),
                l3_mask=l3_mask,
                muscle_mask=muscle_mask,
                slice_idx=8,
                title="L3 Center Slice",
                summary_lines=["SMA: 42.0 cm2", "Slice: 8"],
                panel_titles=("Axial", "Sagittal Reference"),
                sagittal_level_text="Axial level z=8 | slab 3 mm",
                spacing_mm=(1.0, 1.0, 2.5),
                include_sagittal_panel=False,
            )

        self.assertEqual(axial_rendered.shape[0], axial_rendered.shape[1])
        self.assertGreater(int(axial_rendered.max()), int(axial_rendered.min()))

    def test_overlay_display_directions_supports_ap_and_lr_planes(self):
        self.assertEqual(_overlay_display_directions(("A", "S")), ("I", "P"))
        self.assertEqual(_overlay_display_directions(("R", "S")), ("I", "L"))


if __name__ == "__main__":
    unittest.main()
