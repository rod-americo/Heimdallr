import tempfile
import unittest
from pathlib import Path

import numpy as np
import pydicom

from heimdallr.metrics.jobs._dicom_secondary_capture import create_secondary_capture_from_rgb, metadata_value


class TestDicomSecondaryCapture(unittest.TestCase):
    def test_metadata_value_prefers_raw_reference_patient_name(self):
        case_metadata = {
            "PatientName": "JOAO SILVA",
            "ReferenceDicom": {"PatientName": "SILVA^JOAO"},
        }

        self.assertEqual(metadata_value(case_metadata, "PatientName"), "SILVA^JOAO")

    def test_secondary_capture_uses_raw_reference_patient_name(self):
        rgb = np.zeros((8, 8, 3), dtype=np.uint8)
        case_metadata = {
            "PatientName": "JOAO SILVA",
            "PatientID": "123",
            "StudyInstanceUID": "1.2.3",
            "ReferenceDicom": {
                "PatientName": "SILVA^JOAO",
                "StudyDate": "20260405",
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "overlay_sc.dcm"
            create_secondary_capture_from_rgb(
                rgb,
                output_path,
                case_metadata,
                series_description="Test Overlay",
                series_number=9001,
                instance_number=1,
                derivation_description="Test artifact",
            )
            ds = pydicom.dcmread(str(output_path))

        self.assertEqual(str(ds.PatientName), "SILVA^JOAO")
        self.assertEqual(ds.PatientID, "123")


if __name__ == "__main__":
    unittest.main()
