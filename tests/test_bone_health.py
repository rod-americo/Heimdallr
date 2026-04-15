import unittest

import numpy as np

from heimdallr.metrics.analysis.bone_health import (
    build_bone_health_qc_flags,
    build_opportunistic_osteoporosis_composite,
    calculate_mask_hu_statistics,
    classify_l1_hu,
    compute_l1_fracture_screen,
    compute_l1_volumetric_metrics,
    extract_study_technique_context,
)
from heimdallr.metrics.jobs._bone_job_common import display_aspect_from_spacing_mm
from heimdallr.metrics.jobs.bone_health_l1_hu import render_sagittal_overlay_rgb
from heimdallr.metrics.jobs._bone_health_overlay_text import build_overlay_text, hu_mean_color


class TestBoneHealthHelpers(unittest.TestCase):
    def test_display_aspect_uses_row_over_column_spacing(self):
        self.assertEqual(display_aspect_from_spacing_mm((5.0, 1.0)), 5.0)
        self.assertEqual(display_aspect_from_spacing_mm((1.0, 5.0)), 0.2)

    def test_extract_study_technique_context_prefers_results(self):
        id_data = {
            "Modality": "CT",
            "KVP": "100",
            "SliceThickness": "3.5",
            "Contrast": "native",
            "Manufacturer": "Siemens",
        }
        results = {
            "modality": "ct",
            "kvp": "120",
            "slice_thickness_mm": 2.0,
            "contrast_phase": "venous",
            "manufacturer_model": "Somatom Drive",
            "body_part_examined": "CHEST",
        }

        context = extract_study_technique_context(id_data=id_data, results=results)

        self.assertEqual(context["modality"], "CT")
        self.assertEqual(context["kvp"], 120.0)
        self.assertTrue(context["contrast"])
        self.assertEqual(context["slice_thickness_mm"], 2.0)
        self.assertEqual(context["manufacturer"], "Siemens")
        self.assertEqual(context["manufacturer_model"], "Somatom Drive")
        self.assertEqual(context["body_part_examined"], "CHEST")

    def test_calculate_mask_hu_statistics_and_volumetric_roi(self):
        ct = np.zeros((16, 16, 6), dtype=np.float32)
        mask = np.zeros_like(ct, dtype=bool)

        body = np.s_[4:11, 4:11, 1:5]
        posterior_attachment = np.s_[4:8, 10:14, 1:5]
        mask[body] = True
        mask[posterior_attachment] = True

        ct[body] = 180.0
        ct[posterior_attachment] = 20.0

        full_stats = calculate_mask_hu_statistics(ct, mask)
        volumetric = compute_l1_volumetric_metrics(ct, mask, spacing_mm=(1.0, 1.0, 1.0), erosion_mm=1.0)

        self.assertEqual(full_stats["voxel_count"], int(mask.sum()))
        self.assertGreater(full_stats["mean_hu"], 100.0)
        self.assertEqual(volumetric["bone_health_l1_volumetric_full_voxel_count"], int(mask.sum()))
        self.assertLess(
            volumetric["bone_health_l1_volumetric_trabecular_voxel_count"],
            volumetric["bone_health_l1_volumetric_full_voxel_count"],
        )
        self.assertGreater(
            volumetric["bone_health_l1_volumetric_trabecular_hu_mean"],
            volumetric["bone_health_l1_volumetric_full_hu_mean"],
        )

    def test_fracture_screen_detects_height_asymmetry(self):
        mask = np.zeros((12, 12, 12), dtype=bool)

        for y in range(3, 9):
            if y < 5:
                z_start = 6
            elif y < 7:
                z_start = 4
            else:
                z_start = 2
            mask[3:8, y, z_start:10] = True

        fracture = compute_l1_fracture_screen(mask, spacing_mm=(1.0, 1.0, 1.0))

        self.assertEqual(fracture["bone_health_l1_fracture_screen_status"], "complete")
        self.assertTrue(fracture["bone_health_l1_fracture_screen_suspicion"])
        self.assertEqual(fracture["bone_health_l1_fracture_screen_classification"], "suspected_fracture")
        self.assertLess(fracture["bone_health_l1_fracture_screen_min_height_ratio"], 0.8)

    def test_qc_flags_classification_and_composite(self):
        context = {
            "modality": "CT",
            "kvp": 120.0,
            "contrast": False,
            "slice_thickness_mm": 2.0,
        }

        qc = build_bone_health_qc_flags(
            context=context,
            full_mask_voxel_count=120,
            trabecular_voxel_count=40,
            mask_complete=True,
            strict=True,
        )

        self.assertTrue(qc["bone_health_qc_pass"])
        self.assertTrue(qc["bone_health_qc_kvp_in_range"])
        self.assertFalse(qc["bone_health_qc_contrast_present"])
        self.assertTrue(qc["bone_health_qc_slice_thickness_ok"])

        self.assertEqual(classify_l1_hu(180.0), "normal")
        self.assertEqual(classify_l1_hu(135.0), "osteopenia")
        self.assertEqual(classify_l1_hu(95.0), "osteoporosis")
        self.assertEqual(classify_l1_hu(None), "indeterminate")

        composite = build_opportunistic_osteoporosis_composite(
            l1_trabecular_hu_mean=92.0,
            l1_full_hu_mean=120.0,
            fracture_suspicion=True,
            qc_pass=True,
        )

        self.assertEqual(composite["opportunistic_osteoporosis_composite"], "high")
        self.assertGreaterEqual(composite["opportunistic_osteoporosis_composite_score"], 70)
        self.assertEqual(composite["opportunistic_osteoporosis_composite_density_label"], "osteoporosis")

    def test_render_overlay_supports_lateral_superior_planes(self):
        ct_plane = np.arange(12, dtype=np.float32).reshape(3, 4)
        overlay_mask = np.zeros((3, 4), dtype=bool)
        overlay_mask[1, 1:3] = True
        outline_mask = overlay_mask.copy()

        rgb = render_sagittal_overlay_rgb(
            ct_plane=ct_plane,
            overlay_mask=overlay_mask,
            mask_outline=outline_mask,
            title="L1",
            summary_lines=[
                {"text": "Mean: 150 HU", "color": "#ffd166"},
            ],
            plane_spacing_mm=(1.0, 1.0),
            source_axis_codes=("L", "S"),
        )

        self.assertEqual(rgb.ndim, 3)
        self.assertEqual(rgb.shape[2], 3)

    def test_hu_mean_color_uses_requested_thresholds(self):
        self.assertEqual(hu_mean_color(170.0), "white")
        self.assertEqual(hu_mean_color(160.0), "#ffd166")
        self.assertEqual(hu_mean_color(110.0), "#ffd166")
        self.assertEqual(hu_mean_color(109.0), "#ef4444")

    def test_build_overlay_text_colors_mean_line_by_band(self):
        title, summary_lines = build_overlay_text(
            hu_mean=125.0,
            hu_std=18.0,
            locale="pt_BR",
        )

        self.assertEqual(title, "Atenuação trabecular em L1")
        self.assertEqual(len(summary_lines), 1)
        self.assertEqual(summary_lines[0]["text"], "Média: 125 UH")
        self.assertEqual(summary_lines[0]["color"], "#ffd166")


if __name__ == "__main__":
    unittest.main()
