import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from heimdallr.integration.dispatch import config as dispatch_config
from heimdallr.integration.dispatch import events, worker
from heimdallr.shared import settings, store


class TestIntegrationDispatchConfig(unittest.TestCase):
    def test_build_dispatch_queue_items_filters_destinations_and_merges_env_headers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "integration_dispatch.json"
            config_path.write_text(
                json.dumps(
                    {
                        "enabled": True,
                        "destinations": [
                            {
                                "name": "asha",
                                "enabled": True,
                                "url": "http://asha.local/webhooks/patient-identified",
                                "events": ["patient_identified"],
                                "headers": {"X-Heimdallr-Source": "prepare"},
                                "headers_from_env": {"Authorization": "HEIMDALLR_INTEGRATION_AUTH"},
                            },
                            {
                                "name": "disabled",
                                "enabled": False,
                                "url": "http://disabled.local/webhooks/patient-identified",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(settings, "INTEGRATION_DISPATCH_CONFIG_PATH", config_path):
                with patch.dict(os.environ, {"HEIMDALLR_INTEGRATION_AUTH": "Bearer test-token"}, clear=False):
                    items = dispatch_config.build_dispatch_queue_items(
                        event_type="patient_identified",
                        event_version=1,
                        event_key="patient_identified:1.2.3",
                        case_id="Case123",
                        study_uid="1.2.3",
                        payload={"event_type": "patient_identified"},
                    )

        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item["destination_name"], "asha")
        self.assertEqual(item["request_headers"]["X-Heimdallr-Source"], "prepare")
        self.assertEqual(item["request_headers"]["Authorization"], "Bearer test-token")


class TestIntegrationDispatchEvents(unittest.TestCase):
    def test_build_patient_identified_event_uses_raw_name_when_available(self):
        event_key, payload = events.build_patient_identified_event(
            id_data={
                "StudyInstanceUID": "1.2.840.1",
                "CaseID": "Case123",
                "ClinicalName": "Case123",
                "AccessionNumber": "5001",
                "StudyDate": "20260407",
                "Modality": "CT",
                "Pipeline": {
                    "prepare_end_time": "2026-04-07T10:20:00-03:00",
                },
            },
            metadata_data={
                "PatientName": "DISPLAY NAME",
                "PatientID": "PID123",
                "PatientBirthDate": "19800115",
                "PatientSex": "F",
                "ReferenceDicom": {
                    "PatientName": "RAW^NAME",
                },
            },
            intake_manifest={
                "calling_aet": "OSIRIX",
                "remote_ip": "10.0.0.1",
            },
        )

        self.assertEqual(event_key, "patient_identified:1.2.840.1")
        self.assertEqual(payload["patient_name_display"], "DISPLAY NAME")
        self.assertEqual(payload["patient_name_raw"], "RAW^NAME")
        self.assertEqual(payload["calling_aet"], "OSIRIX")
        self.assertEqual(payload["remote_ip"], "10.0.0.1")


class TestIntegrationDispatchStoreAndWorker(unittest.TestCase):
    def test_enqueue_claim_and_complete_dispatch_queue_item(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            store.ensure_schema(conn)
            store.enqueue_integration_dispatch(
                conn,
                event_type="patient_identified",
                event_version=1,
                event_key="patient_identified:1.2.3",
                case_id="Case123",
                study_uid="1.2.3",
                destination_name="asha",
                destination_url="http://asha.local/webhooks/patient-identified",
                http_method="POST",
                timeout_seconds=10,
                request_headers={"Authorization": "Bearer test"},
                payload={"event_type": "patient_identified"},
            )

            claimed = store.claim_next_pending_integration_dispatch_queue_item(conn)
            self.assertIsNotNone(claimed)
            self.assertEqual(claimed[1], "patient_identified")
            self.assertEqual(claimed[3], "patient_identified:1.2.3")

            store.mark_integration_dispatch_queue_item_done(conn, claimed[0], response_status=202)
            row = conn.execute(
                "SELECT status, response_status FROM integration_dispatch_queue WHERE id = ?",
                (claimed[0],),
            ).fetchone()
            self.assertEqual(row["status"], "done")
            self.assertEqual(row["response_status"], 202)
        finally:
            conn.close()

    def test_dispatch_integration_event_posts_json_and_accepts_202(self):
        response = Mock(status_code=202, text="")

        with patch("heimdallr.integration.dispatch.worker.requests.request", return_value=response) as request:
            returned = worker.dispatch_integration_event(
                destination_url="http://asha.local/webhooks/patient-identified",
                http_method="POST",
                timeout_seconds=10,
                request_headers_json=json.dumps({"Authorization": "Bearer test"}),
                payload_json=json.dumps({"event_type": "patient_identified"}),
            )

        self.assertIs(returned, response)
        request.assert_called_once_with(
            "POST",
            "http://asha.local/webhooks/patient-identified",
            json={"event_type": "patient_identified"},
            headers={"Authorization": "Bearer test"},
            timeout=10,
        )

    def test_dispatch_integration_event_raises_on_non_2xx(self):
        response = Mock(status_code=503, text="service unavailable")

        with patch("heimdallr.integration.dispatch.worker.requests.request", return_value=response):
            with self.assertRaises(RuntimeError) as ctx:
                worker.dispatch_integration_event(
                    destination_url="http://asha.local/webhooks/patient-identified",
                    http_method="POST",
                    timeout_seconds=10,
                    request_headers_json="{}",
                    payload_json=json.dumps({"event_type": "patient_identified"}),
                )

        self.assertIn("HTTP 503", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
