import sqlite3
import unittest
from unittest.mock import patch

from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, JPEGLosslessSV1, SecondaryCaptureImageStorage

from heimdallr.dicom_egress import worker
from heimdallr.shared import store


def _build_secondary_capture_dataset() -> Dataset:
    ds = Dataset()
    ds.file_meta = FileMetaDataset()
    ds.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds.SOPClassUID = SecondaryCaptureImageStorage
    ds.SOPInstanceUID = "1.2.3.4"
    ds.Rows = 1
    ds.Columns = 1
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = 8
    ds.BitsStored = 8
    ds.HighBit = 7
    ds.PixelRepresentation = 0
    ds.PixelData = b"\x00"
    return ds


class TestDicomEgressWorker(unittest.TestCase):
    def test_prepare_dataset_for_peer_keeps_uncompressed_dataset_when_peer_accepts_uncompressed(self):
        ds = _build_secondary_capture_dataset()

        prepared = worker._prepare_dataset_for_peer(ds, ExplicitVRLittleEndian)

        self.assertIs(prepared, ds)

    def test_prepare_dataset_for_peer_compresses_when_peer_only_accepts_compressed(self):
        ds = _build_secondary_capture_dataset()

        with patch.object(Dataset, "compress", autospec=True) as compress:
            prepared = worker._prepare_dataset_for_peer(ds, JPEGLosslessSV1)

        self.assertIsNot(prepared, ds)
        compress.assert_called_once_with(prepared, str(JPEGLosslessSV1), generate_instance_uid=False)

    def test_prepare_dataset_for_peer_raises_clear_error_when_compression_is_unavailable(self):
        ds = _build_secondary_capture_dataset()

        with patch.object(Dataset, "compress", autospec=True, side_effect=NotImplementedError("encoder missing")):
            with self.assertRaises(RuntimeError) as ctx:
                worker._prepare_dataset_for_peer(ds, JPEGLosslessSV1)

        self.assertIn("Peer only accepted transfer syntax", str(ctx.exception))
        self.assertIn("encoder missing", str(ctx.exception))


class TestStudyMetadataUpsert(unittest.TestCase):
    def test_upsert_study_metadata_persists_patient_identity_columns(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            store.ensure_schema(conn)
            store.upsert_study_metadata(
                conn,
                {
                    "StudyInstanceUID": "1.2.840.10008.1",
                    "PatientName": "Alice Example",
                    "PatientID": "12345",
                    "PatientBirthDate": "19800115",
                    "ClinicalName": "AliceE_20260404_123",
                    "AccessionNumber": "123",
                    "StudyDate": "20260404",
                    "PatientSex": "F",
                    "Modality": "CT",
                },
            )

            row = conn.execute(
                "SELECT PatientID, PatientBirthDate FROM dicom_metadata WHERE StudyInstanceUID = ?",
                ("1.2.840.10008.1",),
            ).fetchone()
            self.assertEqual(row["PatientID"], "12345")
            self.assertEqual(row["PatientBirthDate"], "19800115")
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
