import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from heimdallr.intake import gateway
from heimdallr.shared import store
from heimdallr.shared.study_manifest import build_study_manifest_digest


class TestIntakeMetadata(unittest.TestCase):
    def test_build_intake_manifest_includes_manifest_digest(self):
        study = gateway.StudyState(
            study_uid="1.2.840.10008.1",
            path=Path("/tmp/study"),
            first_update_ts=100.0,
            last_update_ts=112.5,
            calling_aet="SRC",
            remote_ip="10.20.30.40",
            instance_count=42,
        )

        manifest = gateway.build_intake_manifest(
            study,
            handoff_ts=120.0,
            manifest_digest="abc123",
        )

        self.assertEqual(manifest["manifest_digest"], "abc123")
        self.assertEqual(manifest["instance_count"], 42)

    def test_build_study_manifest_digest_is_stable_for_same_tree(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "1.2.3").mkdir()
            (root / "1.2.3" / "a.dcm").write_bytes(b"a")
            (root / "1.2.4").mkdir()
            (root / "1.2.4" / "b.dcm").write_bytes(b"bb")

            digest_a = build_study_manifest_digest(
                root,
                study_uid="1.2.840.10008.1",
                calling_aet="SRC",
                instance_count=2,
            )
            digest_b = build_study_manifest_digest(
                root,
                study_uid="1.2.840.10008.1",
                calling_aet="SRC",
                instance_count=2,
            )

        self.assertEqual(digest_a, digest_b)

    def test_upsert_study_metadata_preserves_intake_columns(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            store.ensure_schema(conn)
            store.upsert_intake_metadata(
                conn,
                "1.2.840.10008.1",
                calling_aet="CT_SCANNER_A",
                remote_ip="10.20.30.40",
            )

            store.upsert_study_metadata(
                conn,
                {
                    "StudyInstanceUID": "1.2.840.10008.1",
                    "PatientName": "Alice Example",
                    "ClinicalName": "AliceE_20260404_123",
                    "AccessionNumber": "123",
                    "StudyDate": "20260404",
                    "PatientSex": "F",
                    "Modality": "CT",
                },
            )

            row = conn.execute(
                "SELECT CallingAET, RemoteIP, PatientName FROM dicom_metadata WHERE StudyInstanceUID = ?",
                ("1.2.840.10008.1",),
            ).fetchone()
            self.assertEqual(row["CallingAET"], "CT_SCANNER_A")
            self.assertEqual(row["RemoteIP"], "10.20.30.40")
            self.assertEqual(row["PatientName"], "Alice Example")
        finally:
            conn.close()

    def test_extract_requestor_identity_reads_calling_aet_and_remote_ip(self):
        event = SimpleNamespace(
            assoc=SimpleNamespace(
                requestor=SimpleNamespace(
                    ae_title="PACS_A",
                    address="192.168.10.25",
                )
            )
        )

        calling_aet, remote_ip = gateway.extract_requestor_identity(event)

        self.assertEqual(calling_aet, "PACS_A")
        self.assertEqual(remote_ip, "192.168.10.25")


if __name__ == "__main__":
    unittest.main()
