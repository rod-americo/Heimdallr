import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from heimdallr.control_plane.app import create_app
from heimdallr.integration.delivery import package as delivery_package
from heimdallr.integration.delivery import worker
from heimdallr.shared import settings, store
from heimdallr.integration.submissions import load_external_submission_sidecar


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
                        "requested_outputs": json.dumps({"report_pdf": False}),
                        "requested_metrics_modules": json.dumps(["l3_muscle_area", "bone_health_l1_hu"]),
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
            self.assertFalse(sidecar["requested_outputs"]["report_pdf"])
            self.assertEqual(
                sidecar["requested_metrics_modules"],
                ["l3_muscle_area", "bone_health_l1_hu"],
            )
            self.assertEqual(
                body["requested_metrics_modules"],
                ["l3_muscle_area", "bone_health_l1_hu"],
            )

            with patch.object(settings, "UPLOAD_EXTERNAL_DIR", upload_dir):
                status_response = client.get(f"/jobs/{body['job_id']}")
            self.assertEqual(status_response.status_code, 200)
            status_body = status_response.json()
            self.assertEqual(status_body["job_id"], body["job_id"])
            self.assertEqual(status_body["status"], "queued")
            self.assertEqual(status_body["client_case_id"], "external-123")


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
                requested_outputs={"report_pdf": True},
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

    def test_enqueue_claim_failed_delivery_queue_item_keeps_payload(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            store.ensure_schema(conn)
            store.enqueue_integration_delivery(
                conn,
                job_id="job-failed",
                event_type="case.failed",
                event_version=1,
                case_id="CaseFailed",
                study_uid="1.2.3",
                client_case_id="external-123",
                source_system="partner_a",
                callback_url="http://receiver.local/callback",
                http_method="POST",
                timeout_seconds=120,
                requested_outputs={},
                payload={"failure_stage": "metrics", "error": "boom"},
            )

            claimed = store.claim_next_pending_integration_delivery_queue_item(conn)
            self.assertIsNotNone(claimed)
            self.assertEqual(claimed[1], "job-failed")
            self.assertEqual(claimed[2], "case.failed")
            self.assertEqual(json.loads(claimed[12])["failure_stage"], "metrics")
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
            (case_root / "artifacts" / "metrics" / "l3_muscle_area" / "overlay.png").write_bytes(b"png")
            (case_root / "artifacts" / "metrics" / "instructions").mkdir(parents=True, exist_ok=True)
            (case_root / "artifacts" / "metrics" / "instructions" / "artifact_instructions.pdf").write_bytes(b"%PDF")
            (case_root / "artifacts" / "metrics" / "instructions" / "artifact_instructions.dcm").write_bytes(b"DICM")

            report_path = case_root / "metadata" / "report.pdf"
            report_dicom_path = case_root / "metadata" / "report.dcm"
            def _fake_report(_case_root):
                report_path.write_bytes(b"%PDF")
                return report_path

            def _fake_report_dicom(*, output_path, **_kwargs):
                output_path.write_bytes(b"DICM")

            with (
                patch("heimdallr.integration.delivery.package.study_dir", return_value=case_root),
                patch("heimdallr.integration.delivery.package.study_id_json", return_value=case_root / "metadata" / "id.json"),
                patch("heimdallr.integration.delivery.package.study_metadata_json", return_value=case_root / "metadata" / "metadata.json"),
                patch("heimdallr.integration.delivery.package.study_results_json", return_value=case_root / "metadata" / "resultados.json"),
                patch("heimdallr.integration.delivery.package.study_artifacts_dir", return_value=case_root / "artifacts"),
                patch("heimdallr.integration.delivery.package.build_case_report", side_effect=_fake_report),
                patch("heimdallr.integration.delivery.package.create_encapsulated_pdf_dicom", side_effect=_fake_report_dicom),
            ):
                manifest, package_path = delivery_package.build_delivery_package(
                    case_id="Case123",
                    job_id="job-1",
                    client_case_id="external-123",
                    source_system="partner_a",
                    requested_outputs={
                        "metrics_json": True,
                        "overlays_png": True,
                        "overlays_dicom": True,
                        "report_pdf": True,
                        "report_pdf_dicom": True,
                        "artifact_instructions_pdf": True,
                        "artifact_instructions_dicom": True,
                        "artifacts_tree": False,
                    },
                )

            self.assertEqual(manifest["event_type"], "case.completed")
            self.assertEqual(manifest["job_id"], "job-1")
            self.assertTrue(package_path.exists())
            self.assertTrue(report_dicom_path.exists())

            import zipfile

            with zipfile.ZipFile(package_path, "r") as zip_handle:
                names = set(zip_handle.namelist())
                package_manifest = json.loads(zip_handle.read("manifest.json").decode("utf-8"))
            self.assertIn("manifest.json", names)
            self.assertIn("metadata/id.json", names)
            self.assertIn("metadata/resultados.json", names)
            self.assertIn("metadata/report.pdf", names)
            self.assertIn("metadata/report.dcm", names)
            self.assertIn("artifacts/metrics/l3_muscle_area/result.json", names)
            self.assertIn("artifacts/metrics/l3_muscle_area/overlay.png", names)
            self.assertIn("artifacts/metrics/l3_muscle_area/overlay_sc.dcm", names)
            self.assertIn("artifacts/metrics/instructions/artifact_instructions.pdf", names)
            self.assertIn("artifacts/metrics/instructions/artifact_instructions.dcm", names)
            self.assertEqual(manifest["missing_outputs"], [])
            self.assertIn("report_pdf_dicom", manifest["delivered_outputs"])
            self.assertEqual(package_manifest["requested_outputs"]["report_pdf_dicom"], True)

    def test_build_delivery_package_honors_output_selection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_root = Path(tmpdir) / "runtime" / "studies" / "CaseSelected"
            (case_root / "metadata").mkdir(parents=True, exist_ok=True)
            (case_root / "artifacts" / "metrics" / "l3_muscle_area").mkdir(parents=True, exist_ok=True)
            (case_root / "metadata" / "id.json").write_text(
                json.dumps({"StudyInstanceUID": "1.2.3", "ExternalDelivery": {}}),
                encoding="utf-8",
            )
            (case_root / "metadata" / "resultados.json").write_text('{"metrics":{}}', encoding="utf-8")
            (case_root / "artifacts" / "metrics" / "l3_muscle_area" / "result.json").write_text(
                "{}",
                encoding="utf-8",
            )
            (case_root / "artifacts" / "metrics" / "l3_muscle_area" / "overlay.png").write_bytes(b"png")
            (case_root / "artifacts" / "metrics" / "l3_muscle_area" / "overlay_sc.dcm").write_bytes(b"dicom")

            with (
                patch("heimdallr.integration.delivery.package.study_dir", return_value=case_root),
                patch("heimdallr.integration.delivery.package.study_id_json", return_value=case_root / "metadata" / "id.json"),
                patch("heimdallr.integration.delivery.package.study_metadata_json", return_value=case_root / "metadata" / "metadata.json"),
                patch("heimdallr.integration.delivery.package.study_results_json", return_value=case_root / "metadata" / "resultados.json"),
                patch("heimdallr.integration.delivery.package.study_artifacts_dir", return_value=case_root / "artifacts"),
            ):
                manifest, package_path = delivery_package.build_delivery_package(
                    case_id="CaseSelected",
                    job_id="job-2",
                    client_case_id="external-456",
                    source_system=None,
                    requested_outputs={
                        "id_json": False,
                        "metadata_json": False,
                        "metrics_json": True,
                        "overlays_png": False,
                        "overlays_dicom": False,
                        "report_pdf": False,
                        "report_pdf_dicom": False,
                        "artifact_instructions_pdf": False,
                        "artifact_instructions_dicom": False,
                        "artifacts_tree": False,
                    },
                )

            import zipfile

            with zipfile.ZipFile(package_path, "r") as zip_handle:
                names = set(zip_handle.namelist())
            self.assertEqual(
                names,
                {
                    "manifest.json",
                    "metadata/resultados.json",
                    "artifacts/metrics/l3_muscle_area/result.json",
                },
            )
            self.assertEqual(manifest["delivered_outputs"]["metrics_json"], ["metadata/resultados.json"])
            self.assertEqual(
                manifest["delivered_outputs"]["metric_result_json"],
                ["artifacts/metrics/l3_muscle_area/result.json"],
            )

    def test_deliver_case_package_posts_multipart(self):
        response = Mock(status_code=202, text="")
        with tempfile.TemporaryDirectory() as tmpdir:
            package_path = Path(tmpdir) / "package.zip"
            package_path.write_bytes(b"zip")
            manifest = {"package_name": "package.zip"}

            with patch("heimdallr.integration.delivery.worker.requests.request", return_value=response) as request:
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

    def test_deliver_failed_callback_posts_manifest_without_package(self):
        response = Mock(status_code=202, text="")
        manifest = {
            "event_type": "case.failed",
            "package_name": None,
            "job_id": "job-failed",
        }

        with patch("heimdallr.integration.delivery.worker.requests.request", return_value=response) as request:
            returned = worker.deliver_callback(
                callback_url="http://receiver.local/callback",
                http_method="POST",
                timeout_seconds=60,
                manifest=manifest,
            )

        self.assertIs(returned, response)
        args, kwargs = request.call_args
        self.assertEqual(args[:2], ("POST", "http://receiver.local/callback"))
        self.assertIn("manifest", kwargs["files"])
        self.assertNotIn("package", kwargs["files"])


if __name__ == "__main__":
    unittest.main()
