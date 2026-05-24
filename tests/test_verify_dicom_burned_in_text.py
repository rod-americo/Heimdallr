import unittest

import numpy as np
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian

from scripts.verify_dicom_burned_in_text import (
    frames_for_ocr,
    has_ocr_text,
    normalize_text,
)


class TestVerifyDicomBurnedInText(unittest.TestCase):
    def test_normalize_text_removes_empty_lines_and_extra_spaces(self):
        self.assertEqual(normalize_text("  A   B  \n\n C\tD "), "A B\nC D")

    def test_has_ocr_text_respects_minimum_alphanumeric_count(self):
        self.assertFalse(has_ocr_text(".. A ?", min_text_chars=2))
        self.assertTrue(has_ocr_text("A7", min_text_chars=2))

    def test_frames_for_ocr_handles_monochrome2_single_frame(self):
        ds = Dataset()
        ds.Rows = 2
        ds.Columns = 2
        ds.SamplesPerPixel = 1
        ds.PhotometricInterpretation = "MONOCHROME2"
        ds.BitsAllocated = 16
        ds.BitsStored = 16
        ds.HighBit = 15
        ds.PixelRepresentation = 0
        ds.RescaleSlope = 1
        ds.RescaleIntercept = 0
        ds.file_meta = FileMetaDataset()
        ds.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
        pixels = np.array([[0, 100], [200, 300]], dtype=np.uint16)
        ds.PixelData = pixels.tobytes()

        frames = frames_for_ocr(ds, max_frames_per_instance=1)

        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].shape, (2, 2))
        self.assertEqual(frames[0].dtype, np.uint8)


if __name__ == "__main__":
    unittest.main()
