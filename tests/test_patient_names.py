import unittest

from heimdallr.shared.patient_names import normalize_patient_name_display


class TestPatientNames(unittest.TestCase):
    def test_default_profile_only_normalizes_spacing(self):
        self.assertEqual(
            normalize_patient_name_display("COIMBRA^JACINTA   GIOVANI", "default"),
            "COIMBRA JACINTA GIOVANI",
        )

    def test_dicom_caret_moves_given_names_before_surname_block(self):
        self.assertEqual(
            normalize_patient_name_display("MACEDO^CARMELITA BRAGA DA SILVA", "dicom_caret"),
            "CARMELITA BRAGA DA SILVA MACEDO",
        )

    def test_dicom_caret_preserves_multi_component_suffix_order(self):
        self.assertEqual(
            normalize_patient_name_display("SILVA^JOAO^NETO", "dicom_caret"),
            "JOAO NETO SILVA",
        )


if __name__ == "__main__":
    unittest.main()
