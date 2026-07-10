import tempfile
import unittest
from pathlib import Path

import numpy as np
import pydicom
from pydicom.uid import DeflatedExplicitVRLittleEndian, JPEGLSLossless

from heimdallr.metrics.jobs._dicom_secondary_capture import (
    axial_dicom_geometry_from_nifti,
    create_secondary_capture_from_rgb,
    metadata_value,
    nearest_source_dicom_geometry,
    resolve_secondary_capture_transfer_syntax,
)


class TestDicomSecondaryCapture(unittest.TestCase):
    def test_nearest_source_geometry_preserves_source_orientation_and_position(self):
        geometries = [
            {
                "image_position_patient": [-170.1, -41.2, 78.76],
                "image_orientation_patient": [1.0, 0.0, 0.0, 0.0, 1.0, 0.0],
                "slice_location": 78.76,
            },
            {
                "image_position_patient": [-170.1, -41.2, 79.76],
                "image_orientation_patient": [1.0, 0.0, 0.0, 0.0, 1.0, 0.0],
                "slice_location": 79.76,
            },
        ]

        geometry = nearest_source_dicom_geometry(geometries, [10.0, 20.0, 78.8])

        self.assertEqual(geometry, geometries[0])

    def test_axial_geometry_converts_nifti_ras_position_to_dicom_lps(self):
        affine = np.asarray(
            [
                [0.8, 0.0, 0.0, 12.0],
                [0.0, 0.8, 0.0, -34.0],
                [0.0, 0.0, 2.5, 50.0],
                [0.0, 0.0, 0.0, 1.0],
            ]
        )

        geometry = axial_dicom_geometry_from_nifti(affine, 4.0)

        self.assertEqual(geometry["image_position_patient"], [-12.0, 34.0, 60.0])
        self.assertEqual(
            geometry["image_orientation_patient"],
            [-1.0, 0.0, 0.0, 0.0, -1.0, 0.0],
        )
        self.assertEqual(geometry["slice_location"], 60.0)

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
        self.assertEqual(ds.file_meta.TransferSyntaxUID, JPEGLSLossless)

    def test_secondary_capture_preserves_reference_study_and_patient_tags(self):
        rgb = np.zeros((8, 8, 3), dtype=np.uint8)
        case_metadata = {
            "PatientName": "JOAO SILVA",
            "PatientID": "123",
            "StudyInstanceUID": "1.2.3",
            "ReferenceDicom": {
                "PatientName": "SILVA^JOAO",
                "IssuerOfPatientID": "HOSPITAL_A",
                "PatientBirthDate": "19800115",
                "PatientBirthTime": "074500",
                "PatientSex": "M",
                "PatientAge": "046Y",
                "StudyDate": "20260405",
                "StudyTime": "101112.123",
                "StudyID": "STUDY-42",
                "StudyDescription": "CT ABDOMEN",
                "AccessionNumber": "ACC123",
                "InstitutionName": "General Hospital",
                "InstitutionAddress": "Main St",
                "StationName": "CT01",
                "ReferringPhysicianName": "DOE^JANE",
                "PerformingPhysicianName": "ROE^JOHN",
                "OperatorsName": "TECH^ONE",
                "FrameOfReferenceUID": "1.2.3.4.5",
                "BodyPartExamined": "ABDOMEN",
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
                image_position_patient=[-12.0, 34.0, 60.0],
                image_orientation_patient=[-1.0, 0.0, 0.0, 0.0, -1.0, 0.0],
                slice_location=60.0,
                slice_thickness_mm=5.0,
                spacing_between_slices_mm=5.0,
            )
            ds = pydicom.dcmread(str(output_path))

        self.assertEqual(ds.StudyInstanceUID, "1.2.3")
        self.assertEqual(str(ds.PatientName), "SILVA^JOAO")
        self.assertEqual(ds.PatientID, "123")
        self.assertEqual(ds.IssuerOfPatientID, "HOSPITAL_A")
        self.assertEqual(ds.PatientBirthDate, "19800115")
        self.assertEqual(ds.PatientBirthTime, "074500")
        self.assertEqual(ds.PatientSex, "M")
        self.assertEqual(ds.PatientAge, "046Y")
        self.assertEqual(ds.StudyDate, "20260405")
        self.assertEqual(ds.StudyTime, "101112.123")
        self.assertEqual(ds.StudyID, "STUDY-42")
        self.assertEqual(ds.StudyDescription, "CT ABDOMEN")
        self.assertEqual(ds.AccessionNumber, "ACC123")
        self.assertEqual(ds.InstitutionName, "General Hospital")
        self.assertEqual(ds.InstitutionAddress, "Main St")
        self.assertEqual(ds.StationName, "CT01")
        self.assertEqual(str(ds.ReferringPhysicianName), "DOE^JANE")
        self.assertEqual(str(ds.PerformingPhysicianName), "ROE^JOHN")
        self.assertEqual(str(ds.OperatorsName), "TECH^ONE")
        self.assertEqual(ds.FrameOfReferenceUID, "1.2.3.4.5")
        self.assertEqual(ds.BodyPartExamined, "ABDOMEN")
        self.assertEqual([float(value) for value in ds.ImagePositionPatient], [-12.0, 34.0, 60.0])
        self.assertEqual(float(ds.SliceLocation), 60.0)
        self.assertEqual(float(ds.SliceThickness), 5.0)
        self.assertEqual(float(ds.SpacingBetweenSlices), 5.0)
        self.assertNotEqual(ds.SeriesInstanceUID, case_metadata["ReferenceDicom"].get("SeriesInstanceUID"))
        self.assertNotEqual(ds.SOPInstanceUID, case_metadata["ReferenceDicom"].get("SOPInstanceUID"))

    def test_secondary_capture_downscales_large_rgb_canvas_to_shared_default(self):
        rgb = np.zeros((800, 1260, 3), dtype=np.uint8)
        case_metadata = {"StudyInstanceUID": "1.2.3", "PatientName": "Test^Patient"}

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

        self.assertEqual(ds.Columns, 512)
        self.assertLess(ds.Rows, 512)
        self.assertLess(len(ds.PixelData), 800 * 1260 * 3)

    def test_secondary_capture_can_write_deflated_lossless(self):
        rgb = np.zeros((64, 64, 3), dtype=np.uint8)
        rgb[8:56, 8:56, 0] = 255
        case_metadata = {"StudyInstanceUID": "1.2.3", "PatientName": "Test^Patient"}

        with tempfile.TemporaryDirectory() as tmpdir:
            original_path = Path(tmpdir) / "original.dcm"
            deflated_path = Path(tmpdir) / "deflated.dcm"
            create_secondary_capture_from_rgb(
                rgb,
                original_path,
                case_metadata,
                series_description="Test Overlay",
                series_number=9001,
                instance_number=1,
                derivation_description="Test artifact",
                transfer_syntax="original",
            )
            create_secondary_capture_from_rgb(
                rgb,
                deflated_path,
                case_metadata,
                series_description="Test Overlay",
                series_number=9001,
                instance_number=1,
                derivation_description="Test artifact",
                transfer_syntax="deflated_explicit_vr_little_endian",
            )
            ds = pydicom.dcmread(str(deflated_path))
            original_size = original_path.stat().st_size
            deflated_size = deflated_path.stat().st_size

        self.assertEqual(ds.file_meta.TransferSyntaxUID, DeflatedExplicitVRLittleEndian)
        self.assertTrue(np.array_equal(ds.pixel_array, rgb))
        self.assertLess(deflated_size, original_size)

    def test_secondary_capture_transfer_syntax_accepts_named_options(self):
        for name in [
            "original",
            "deflated_explicit_vr_little_endian",
            "jpeg_ls_lossless",
            "JPEGLSLossless",
            "jpeg2000_lossless",
            "rle_lossless",
        ]:
            self.assertIsNotNone(resolve_secondary_capture_transfer_syntax(name))


if __name__ == "__main__":
    unittest.main()
