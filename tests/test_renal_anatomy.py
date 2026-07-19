import unittest

import numpy as np

from heimdallr.metrics.analysis.renal_anatomy import analyze_renal_anatomy


class TestRenalAnatomyQc(unittest.TestCase):
    def setUp(self):
        self.shape = (24, 24, 24)
        self.spacing = (10.0, 10.0, 10.0)
        self.affine = np.diag([10.0, 10.0, 10.0, 1.0])
        self.ct = np.zeros(self.shape, dtype=np.float32)
        self.l3 = np.zeros(self.shape, dtype=bool)
        self.l3[9:15, 9:15, 13:16] = True
        self.l4 = np.zeros(self.shape, dtype=bool)
        self.l4[9:15, 9:15, 9:12] = True

    def test_native_kidney_and_pelvic_component_are_separated(self):
        kidney_right = np.zeros(self.shape, dtype=bool)
        kidney_right[3:7, 14:18, 16:22] = True
        kidney_right[14:19, 4:9, 3:9] = True
        self.ct[kidney_right] = 40.0
        self.ct[3:7, 14:18, 16:22] = 20.0

        audit, selected, overlay_components = analyze_renal_anatomy(
            {"kidney_right": kidney_right, "kidney_left": None},
            self.ct,
            self.affine,
            self.spacing,
            {"vertebra_l3": self.l3, "vertebra_l4": self.l4},
        )

        right = audit["kidneys"]["kidney_right"]
        self.assertEqual(right["classification_status"], "native_and_suspected_allograft")
        self.assertEqual(right["significant_component_count"], 2)
        self.assertEqual(selected["kidney_right"]["voxel_count"], 96)
        native, allograft = right["components"]
        self.assertEqual(native["anatomic_role"], "native_kidney_right")
        self.assertEqual(native["volume_cm3"], 96.0)
        self.assertEqual(native["hu_mean"], 20.0)
        self.assertEqual(allograft["anatomic_role"], "suspected_renal_allograft_right")
        self.assertEqual(allograft["volume_cm3"], 150.0)
        self.assertEqual(len(audit["suspected_renal_allografts"]), 1)
        self.assertTrue(audit["multiple_significant_components"])
        self.assertEqual(len(overlay_components), 1)
        self.assertEqual(
            overlay_components[0]["anatomic_role"],
            "suspected_renal_allograft_right",
        )

    def test_single_pelvic_component_is_measured_without_allograft_label(self):
        kidney_right = np.zeros(self.shape, dtype=bool)
        kidney_right[14:18, 4:8, 3:9] = True
        self.ct[kidney_right] = 28.0

        audit, selected, overlay_components = analyze_renal_anatomy(
            {"kidney_right": kidney_right, "kidney_left": None},
            self.ct,
            self.affine,
            self.spacing,
            {"vertebra_l3": self.l3, "vertebra_l4": self.l4},
        )

        right = audit["kidneys"]["kidney_right"]
        component = right["components"][0]
        self.assertEqual(
            right["classification_status"],
            "single_pelvic_component_anatomy_indeterminate",
        )
        self.assertEqual(selected["kidney_right"]["volume_cm3"], 96.0)
        self.assertEqual(
            component["anatomic_role"],
            "indeterminate_pelvic_renal_component_right",
        )
        self.assertEqual(
            component["classification_reason"],
            "pelvic_position_without_identified_native_component",
        )
        self.assertFalse(audit["suspected_allograft"])
        self.assertEqual(audit["suspected_renal_allografts"], [])
        self.assertEqual(overlay_components, [])

    def test_multiple_pelvic_components_without_native_are_ambiguous(self):
        kidney_right = np.zeros(self.shape, dtype=bool)
        kidney_right[2:5, 2:5, 3:9] = True
        kidney_right[14:18, 4:8, 3:9] = True

        audit, selected, overlay_components = analyze_renal_anatomy(
            {"kidney_right": kidney_right, "kidney_left": None},
            self.ct,
            self.affine,
            self.spacing,
            {"vertebra_l3": self.l3, "vertebra_l4": self.l4},
        )

        self.assertEqual(
            audit["kidneys"]["kidney_right"]["classification_status"],
            "ambiguous_multiple_components",
        )
        self.assertIsNone(selected["kidney_right"])
        self.assertFalse(audit["suspected_allograft"])
        self.assertEqual(overlay_components, [])

    def test_multiple_components_without_reference_withhold_native_selection(self):
        kidney_right = np.zeros(self.shape, dtype=bool)
        kidney_right[3:7, 14:18, 16:22] = True
        kidney_right[14:19, 4:9, 3:9] = True

        audit, selected, _overlay_components = analyze_renal_anatomy(
            {"kidney_right": kidney_right, "kidney_left": None},
            self.ct,
            self.affine,
            self.spacing,
            {},
        )

        self.assertIsNone(selected["kidney_right"])
        self.assertEqual(
            audit["kidneys"]["kidney_right"]["classification_status"],
            "ambiguous_multiple_components",
        )
        self.assertFalse(audit["suspected_allograft"])

    def test_single_component_without_reference_preserves_legacy_measurement(self):
        kidney_left = np.zeros(self.shape, dtype=bool)
        kidney_left[14:18, 14:18, 15:21] = True

        audit, selected, _overlay_components = analyze_renal_anatomy(
            {"kidney_right": None, "kidney_left": kidney_left},
            self.ct,
            self.affine,
            self.spacing,
            {},
        )

        self.assertEqual(selected["kidney_left"]["voxel_count"], 96)
        self.assertEqual(
            audit["kidneys"]["kidney_left"]["classification_status"],
            "single_component_legacy_fallback",
        )

    def test_subthreshold_fragment_does_not_create_multiple_component_qc(self):
        kidney_left = np.zeros(self.shape, dtype=bool)
        kidney_left[14:18, 14:18, 15:21] = True
        kidney_left[2:3, 2:3, 2:3] = True

        audit, selected, _overlay_components = analyze_renal_anatomy(
            {"kidney_right": None, "kidney_left": kidney_left},
            self.ct,
            self.affine,
            self.spacing,
            {},
        )

        left = audit["kidneys"]["kidney_left"]
        self.assertEqual(left["raw_component_count"], 2)
        self.assertEqual(left["significant_component_count"], 1)
        self.assertEqual(left["classification_status"], "single_component_legacy_fallback")
        self.assertEqual(selected["kidney_left"]["voxel_count"], 96)
        fragment = next(item for item in left["components"] if not item["significant"])
        self.assertEqual(fragment["anatomic_role"], "segmentation_fragment")

    def test_truncated_vertebral_reference_does_not_drive_topography(self):
        kidney_right = np.zeros(self.shape, dtype=bool)
        kidney_right[3:7, 14:18, 16:22] = True
        truncated_l3 = self.l3.copy()
        truncated_l3[:, :, 0] = False
        truncated_l3[9:15, 9:15, 0:2] = True

        audit, selected, _overlay_components = analyze_renal_anatomy(
            {"kidney_right": kidney_right, "kidney_left": None},
            self.ct,
            self.affine,
            self.spacing,
            {"vertebra_l3": truncated_l3, "vertebra_l4": self.l4},
        )

        self.assertEqual(audit["topographic_reference"]["status"], "unavailable")
        self.assertEqual(
            audit["kidneys"]["kidney_right"]["classification_status"],
            "single_component_legacy_fallback",
        )
        self.assertEqual(selected["kidney_right"]["voxel_count"], 96)

    def test_five_cm3_component_is_significant_but_smaller_is_not(self):
        kidney_right = np.zeros(self.shape, dtype=bool)
        kidney_right[3:8, 3:4, 3:4] = True

        audit, selected, _overlay_components = analyze_renal_anatomy(
            {"kidney_right": kidney_right, "kidney_left": None},
            self.ct,
            self.affine,
            self.spacing,
            {},
        )
        self.assertTrue(audit["kidneys"]["kidney_right"]["components"][0]["significant"])
        self.assertEqual(selected["kidney_right"]["volume_cm3"], 5.0)

        kidney_right[7, 3, 3] = False
        audit, selected, _overlay_components = analyze_renal_anatomy(
            {"kidney_right": kidney_right, "kidney_left": None},
            self.ct,
            self.affine,
            self.spacing,
            {},
        )
        self.assertFalse(audit["kidneys"]["kidney_right"]["components"][0]["significant"])
        self.assertIsNone(selected["kidney_right"])


if __name__ == "__main__":
    unittest.main()
