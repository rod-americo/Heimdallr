import unittest
from unittest.mock import patch

from heimdallr.prepare.worker import generate_clinical_name, normalize_patient_name_for_prepare


class TestPreparePatientNames(unittest.TestCase):
    def test_normalize_patient_name_for_prepare_uses_configured_profile(self):
        with patch("heimdallr.prepare.worker.settings.PATIENT_NAME_PROFILE", "dicom_caret"):
            normalized = normalize_patient_name_for_prepare("MACEDO^CARMELITA BRAGA DA SILVA")

        self.assertEqual(normalized, "CARMELITA BRAGA DA SILVA MACEDO")

    def test_generate_clinical_name_uses_normalized_name_order(self):
        with patch("heimdallr.prepare.worker.settings.PATIENT_NAME_PROFILE", "dicom_caret"):
            normalized = normalize_patient_name_for_prepare("COIMBRA^JACINTA GIOVANI")

        clinical_name = generate_clinical_name(normalized, "20230331", "9256422")

        self.assertEqual(clinical_name, "JacintaGC_20230331_9256422")


if __name__ == "__main__":
    unittest.main()
