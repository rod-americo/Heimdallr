import unittest

from heimdallr.metrics.analysis.hepatic_steatosis import assess_hepatic_steatosis


class TestHepaticSteatosisAssessment(unittest.TestCase):
    def test_reports_out_of_range_when_kvp_is_missing_or_outside_bounds(self):
        missing_kvp = assess_hepatic_steatosis(45.0, 50.0, None)

        self.assertEqual(missing_kvp["status"], "kvp_out_of_range")
        self.assertEqual(missing_kvp["spleen_hu"], 50.0)
        self.assertEqual(missing_kvp["liver_to_spleen_ratio"], 0.9)
        self.assertEqual(assess_hepatic_steatosis(45.0, 50.0, 114.9)["status"], "kvp_out_of_range")
        self.assertEqual(assess_hepatic_steatosis(45.0, 50.0, 125.1)["status"], "kvp_out_of_range")

    def test_inclusive_kvp_bounds_allow_calculation(self):
        self.assertEqual(assess_hepatic_steatosis(45.0, 50.0, 115.0)["status"], "estimated")
        self.assertEqual(assess_hepatic_steatosis(45.0, 50.0, 125.0)["status"], "estimated")

    def test_normal_when_liver_hu_or_liver_spleen_ratio_passes_rule(self):
        self.assertEqual(assess_hepatic_steatosis(50.0, 60.0, 120.0)["status"], "normal")
        self.assertEqual(assess_hepatic_steatosis(49.0, 48.0, 120.0)["status"], "normal")

    def test_estimate_is_rounded_to_whole_percent(self):
        assessment = assess_hepatic_steatosis(45.0, 50.0, 120.0)

        self.assertEqual(assessment["status"], "estimated")
        self.assertEqual(assessment["estimated_percent"], 12)

    def test_missing_liver_hu_does_not_produce_assessment(self):
        self.assertIsNone(assess_hepatic_steatosis(None, 50.0, 120.0))

    def test_partial_liver_sample_uses_volume_and_axial_extent(self):
        assessment = assess_hepatic_steatosis(
            55.0,
            None,
            120.0,
            liver_complete=False,
            liver_sample_volume_cm3=100.0,
            liver_sample_axial_extent_mm=30.0,
            spleen_complete=False,
        )

        self.assertEqual(assessment["status"], "normal")
        self.assertTrue(assessment["partial_coverage"])
        self.assertTrue(assessment["sample_qc"]["liver"]["sufficient"])

    def test_partial_liver_sample_must_pass_both_minimums(self):
        too_small = assess_hepatic_steatosis(
            55.0,
            None,
            120.0,
            liver_complete=False,
            liver_sample_volume_cm3=99.9,
            liver_sample_axial_extent_mm=40.0,
            spleen_complete=False,
        )
        too_short = assess_hepatic_steatosis(
            55.0,
            None,
            120.0,
            liver_complete=False,
            liver_sample_volume_cm3=120.0,
            liver_sample_axial_extent_mm=29.9,
            spleen_complete=False,
        )

        self.assertEqual(too_small["status"], "liver_sample_insufficient")
        self.assertEqual(too_short["status"], "liver_sample_insufficient")

    def test_low_liver_hu_requires_sufficient_spleen_sample(self):
        assessment = assess_hepatic_steatosis(
            45.0,
            50.0,
            120.0,
            liver_complete=False,
            liver_sample_volume_cm3=120.0,
            liver_sample_axial_extent_mm=35.0,
            spleen_complete=False,
            spleen_sample_volume_cm3=19.9,
            spleen_sample_axial_extent_mm=25.0,
        )

        self.assertEqual(assessment["status"], "spleen_sample_insufficient")
        self.assertIsNone(assessment["estimated_percent"])

    def test_sufficient_partial_samples_allow_percentage(self):
        assessment = assess_hepatic_steatosis(
            45.0,
            50.0,
            120.0,
            liver_complete=False,
            liver_sample_volume_cm3=120.0,
            liver_sample_axial_extent_mm=35.0,
            spleen_complete=False,
            spleen_sample_volume_cm3=20.0,
            spleen_sample_axial_extent_mm=20.0,
        )

        self.assertEqual(assessment["status"], "estimated")
        self.assertEqual(assessment["estimated_percent"], 12)
        self.assertTrue(assessment["partial_coverage"])


if __name__ == "__main__":
    unittest.main()
