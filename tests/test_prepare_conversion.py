import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pydicom.uid import ExplicitVRLittleEndian, JPEGLosslessSV1

from heimdallr.prepare import worker


class TestPrepareConversion(unittest.TestCase):
    def test_convert_series_logs_dcm2niix_failure_details(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_dir = Path(tmpdir)
            source = temp_dir / "input.dcm"
            source.write_bytes(b"not-a-real-dicom")
            output = temp_dir / "output.nii.gz"

            with patch.object(worker, "_detect_series_transfer_syntax_uid", return_value=str(ExplicitVRLittleEndian)):
                with patch.object(
                    worker,
                    "_run_dcm2niix_conversion",
                    return_value=subprocess.CompletedProcess(
                        args=["dcm2niix"],
                        returncode=1,
                        stdout=None,
                        stderr="decoder missing\nbad offset",
                    ),
                ):
                    with patch("builtins.print") as print_mock:
                        result = worker.convert_series("4", [source], output, temp_dir, modality="CT")

            self.assertIsNone(result)
            rendered = " ".join(
                " ".join(str(arg) for arg in call.args)
                for call in print_mock.call_args_list
            )
            self.assertIn("dcm2niix failed for series 4", rendered)
            self.assertIn("decoder missing", rendered)
            self.assertFalse(output.exists())

    def test_convert_series_falls_back_to_dicom2nifti_for_ct_jpeg_lossless(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_dir = Path(tmpdir)
            source = temp_dir / "input.dcm"
            source.write_bytes(b"not-a-real-dicom")
            output = temp_dir / "output.nii.gz"

            def fake_convert(input_dir, output_path, reorient_nifti=False):
                Path(output_path).write_bytes(b"nifti")

            fake_module = SimpleNamespace(dicom_series_to_nifti=fake_convert)

            with patch.object(worker, "_detect_series_transfer_syntax_uid", return_value=str(JPEGLosslessSV1)):
                with patch.object(
                    worker,
                    "_run_dcm2niix_conversion",
                    return_value=subprocess.CompletedProcess(
                        args=["dcm2niix"],
                        returncode=1,
                        stdout=None,
                        stderr="Error: JPEG signature 0xFFD8FF not found",
                    ),
                ):
                    with patch.object(worker.importlib, "import_module", return_value=fake_module) as import_mock:
                        result = worker.convert_series("4", [source], output, temp_dir, modality="CT")

            self.assertIsNotNone(result)
            self.assertEqual(result["method"], "dicom2nifti")
            self.assertEqual(result["transfer_syntax_uid"], str(JPEGLosslessSV1))
            self.assertTrue(output.exists())
            import_mock.assert_called_once_with("dicom2nifti")


if __name__ == "__main__":
    unittest.main()
