import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from heimdallr.control_plane.app import create_app
from heimdallr.integration_delivery import package as delivery_package
from heimdallr.integration_delivery import worker
from heimdallr.shared import settings, store
from heimdallr.shared.external_delivery import load_external_submission_sidecar


class TestJobSubmissionRoute(unittest.TestCase):
    def test_submit_job_stores_zip_and_submission_sidecar(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            upload_dir = Path(tmpdir) / "uploads" / "external"
            upload_dir.mkdir(parents=True, exist_ok=True)
            app = create_app()
            client = TestClient(app)

            with patch.object(settings, "UPLOAD_EXTERNAL_DIR", upload_dir):
                response = client.post(
                    "/jobs",
                    files={"study_file": ("study.zip", io.BytesIO(b"fake zip payload"), "application/zip")},
                    data={
                        "client_case_id": "external-123",
                        "callback_url": "http://receiver.local/callback",
                        "source_system": "partner_a",
                        "requested_outputs": json.dumps({"include_report_pdf": False}),
                    },
                )
            self.assertEqual(response.status_code, 200)
            body = response.json()
            stored_file = upload_dir / body["stored_file"]
            self.assertTrue(stored_file.exists())
            self.assertEqual(stored_file.read_bytes(), b"fake zip payload")

            sidecar = load_external_submission_sidecar(stored_file)
            self.assertEqual(sidecar["client_case_id"], "external-123")
            self.assertEqual(sidecar["callback_url"], "http://receiver.local/callback")
            self.assertEqual(sidecar["source_system"], "partner_a")
            self.assertFalse(sidecar["requested_outputs"]["include_report_pdf"])


class TestIntegrationDeliveryStore(unittest.TestCase):
    def test_enqueue_claim_and_complete_delivery_queue_item(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            store.ensure_schema(conn)
            store.enqueue_integration_delivery(
                conn,
                job_id="job-1",
                event_type="case.completed",
                event_version=1,
                case_id="Case123",
                study_uid="1.2.3",
                client_case_id="external-123",
                source_system="partner_a",
                callback_url="http://receiver.local/callback",
                http_method="POST",
                timeout_seconds=120,
                requested_outputs={"include_report_pdf": True},
            )

            claimed = store.claim_next_pending_integration_delivery_queue_item(conn)
            self.assertIsNotNone(claimed)
            self.assertEqual(claimed[1], "job-1")
            self.assertEqual(claimed[4], "Case123")

            store.mark_integration_delivery_queue_item_done(conn, claimed[0], response_status=202)
            row = conn.execute(
                "SELECT status, response_status FROM integration_delivery_queue WHERE id = ?",
                (claimed[0],),
            ).fetchone()
            self.assertEqual(row["status"], "done")
            self.assertEqual(row["response_status"], 202)
        finally:
            conn.close()


class TestIntegrationDeliveryPackageAndWorker(unittest.TestCase):
    def test_build_delivery_package_includes_metadata_results_report_and_metrics_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_root = Path(tmpdir) / "runtime" / "studies" / "Case123"
            (case_root / "metadata").mkdir(parents=True, exist_ok=True)
            (case_root / "artifacts" / "metrics" / "l3_muscle_area").mkdir(parents=True, exist_ok=True)
            (case_root / "metadata" / "id.json").write_text(
                json.dumps(
                    {
                        "StudyInstanceUID": "1.2.3",
                        "Pipeline": {"metrics_end_time": "2026-04-27T10:00:00-03:00"},
                        "ExternalDelivery": {"received_at": "2026-04-27T09:50:00-03:00"},
                    }
                ),
                encoding="utf-8",
            )
            (case_root / "metadata" / "metadata.json").write_text("{}", encoding="utf-8")
            (case_root / "metadata" / "resultados.json").write_text('{"metrics":{}}', encoding="utf-8")
            (case_root / "artifacts" / "metrics" / "l3_muscle_area" / "result.json").write_text(
                "{}",
                encoding="utf-8",
            )
            (case_root / "artifacts" / "metrics" / "l3_muscle_area" / "overlay_sc.dcm").write_bytes(b"dicom")

            report_path = case_root / "metadata" / "report.pdf"
            def _fake_report(_case_root):
                report_path.write_bytes(b"%PDF")
                return report_path

            with (
                patch("heimdallr.integration_delivery.package.study_dir", return_value=case_root),
                patch("heimdallr.integration_delivery.package.study_id_json", return_value=case_root / "metadata" / "id.json"),
                patch("heimdallr.integration_delivery.package.study_metadata_json", return_value=case_root / "metadata" / "metadata.json"),
                patch("heimdallr.integration_delivery.package.study_results_json", return_value=case_root / "metadata" / "resultados.json"),
                patch("heimdallr.integration_delivery.package.study_artifacts_dir", return_value=case_root / "artifacts"),
                patch("heimdallr.integration_delivery.package.build_case_report", side_effect=_fake_report),
            ):
                manifest, package_path = delivery_package.build_delivery_package(
                    case_id="Case123",
                    job_id="job-1",
                    client_case_id="external-123",
                    source_system="partner_a",
                    requested_outputs={"include_report_pdf": True},
                )

            self.assertEqual(manifest["event_type"], "case.completed")
            self.assertEqual(manifest["job_id"], "job-1")
            self.assertTrue(package_path.exists())

            import zipfile

            with zipfile.ZipFile(package_path, "r") as zip_handle:
                names = set(zip_handle.namelist())
            self.assertIn("manifest.json", names)
            self.assertIn("metadata/id.json", names)
            self.assertIn("metadata/resultados.json", names)
            self.assertIn("metadata/report.pdf", names)
            self.assertIn("artifacts/metrics/l3_muscle_area/result.json", names)
            self.assertIn("artifacts/metrics/l3_muscle_area/overlay_sc.dcm", names)

    def test_deliver_case_package_posts_multipart(self):
        response = Mock(status_code=202, text="")
        with tempfile.TemporaryDirectory() as tmpdir:
            package_path = Path(tmpdir) / "package.zip"
            package_path.write_bytes(b"zip")
            manifest = {"package_name": "package.zip"}

            with patch("heimdallr.integration_delivery.worker.requests.request", return_value=response) as request:
                returned = worker.deliver_case_package(
                    callback_url="http://receiver.local/callback",
                    http_method="POST",
                    timeout_seconds=60,
                    manifest=manifest,
                    package_path=str(package_path),
                )

        self.assertIs(returned, response)
        args, kwargs = request.call_args
        self.assertEqual(args[:2], ("POST", "http://receiver.local/callback"))
        self.assertEqual(kwargs["timeout"], 60)
        self.assertIn("manifest", kwargs["files"])
        self.assertIn("package", kwargs["files"])


if __name__ == "__main__":
    unittest.main()
