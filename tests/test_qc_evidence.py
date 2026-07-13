import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nibabel as nib
import numpy as np
from fastapi.testclient import TestClient

from heimdallr.control_plane.app import create_app
from heimdallr.integration.submissions import (
    load_external_submission_sidecar,
    load_upload_options_sidecar,
    resolve_qc_evidence,
)
from heimdallr.prepare import worker as prepare_worker
from heimdallr.segmentation import worker as segmentation_worker
from heimdallr.shared import store
from heimdallr.shared.dependencies import get_db
from heimdallr.shared.qc_evidence import (
    build_inventory,
    consolidate_coverage,
    inventory_total_masks,
    study_content_fingerprint,
)


class TestQcResolution(unittest.TestCase):
    def test_tri_state_precedence(self):
        self.assertFalse(resolve_qc_evidence(None, host_enabled=False)["effective"])
        self.assertTrue(resolve_qc_evidence(True, host_enabled=False)["effective"])
        self.assertTrue(resolve_qc_evidence(None, host_enabled=True)["effective"])
        self.assertFalse(resolve_qc_evidence(False, host_enabled=True)["effective"])

    def test_upload_persists_explicit_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            upload_dir = Path(tmp)
            with (
                patch("heimdallr.control_plane.routers.upload.settings.UPLOAD_EXTERNAL_DIR", upload_dir),
                patch("heimdallr.control_plane.routers.upload.settings.QC_EVIDENCE_ENABLED", False),
            ):
                response = TestClient(create_app()).post(
                    "/upload",
                    files={"file": ("study.zip", io.BytesIO(b"zip"), "application/zip")},
                    data={"qc_evidence": "true"},
                )
            self.assertEqual(response.status_code, 200)
            self.assertIs(response.json()["qc_evidence_requested"], True)
            stored = upload_dir / response.json()["stored_file"]
            sidecar = load_upload_options_sidecar(stored)
            self.assertTrue(sidecar["qc_evidence"]["effective"])
            self.assertEqual(sidecar["qc_evidence"]["reason"], "api_override_enabled")
            self.assertEqual(sidecar["qc_evidence"]["source"], "upload")

    def test_upload_rejects_non_boolean_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch(
                "heimdallr.control_plane.routers.upload.settings.UPLOAD_EXTERNAL_DIR",
                Path(tmp),
            ):
                response = TestClient(create_app()).post(
                    "/upload",
                    files={"file": ("study.zip", io.BytesIO(b"zip"), "application/zip")},
                    data={"qc_evidence": "yes"},
                )
        self.assertEqual(response.status_code, 422)

    def test_jobs_can_disable_enabled_host_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            upload_dir = Path(tmp)
            with (
                patch("heimdallr.control_plane.routers.upload.settings.UPLOAD_EXTERNAL_DIR", upload_dir),
                patch("heimdallr.control_plane.routers.upload.settings.QC_EVIDENCE_ENABLED", True),
            ):
                response = TestClient(create_app()).post(
                    "/jobs",
                    files={"study_file": ("study.zip", io.BytesIO(b"zip"), "application/zip")},
                    data={
                        "client_case_id": "external-1",
                        "callback_url": "https://receiver.invalid/callback",
                        "qc_evidence": "false",
                    },
                )
            self.assertEqual(response.status_code, 200)
            self.assertFalse(response.json()["qc_evidence_effective"])
            sidecar = load_external_submission_sidecar(upload_dir / response.json()["stored_file"])
            self.assertEqual(sidecar["qc_evidence"]["reason"], "api_override_disabled")
            self.assertEqual(sidecar["qc_evidence"]["source"], "job")

    def test_disabled_prepare_path_does_not_hash_or_create_analysis(self):
        with patch.object(
            prepare_worker,
            "study_content_fingerprint",
            side_effect=AssertionError("fingerprint must not run"),
        ):
            context = prepare_worker.prepare_qc_analysis(
                case_id="case",
                study_uid="1.2.3",
                detected_series_map={},
                available_series=[],
                derived_dir=Path("/unused"),
                qc_resolution=resolve_qc_evidence(None, host_enabled=False),
            )
        self.assertEqual(context["status"], "disabled")


class TestQcDomain(unittest.TestCase):
    def test_fingerprint_ignores_paths_and_detects_content_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = root / "first.dcm"
            second = root / "renamed.dcm"
            first.write_bytes(b"dicom-a")
            second.write_bytes(b"dicom-a")
            left = {
                "1.2.3": {
                    "files": [first],
                    "SOPInstanceUIDs": ["1.2.3.1"],
                }
            }
            right = {
                "1.2.3": {
                    "files": [second],
                    "SOPInstanceUIDs": ["1.2.3.1"],
                }
            }
            self.assertEqual(study_content_fingerprint(left), study_content_fingerprint(right))
            second.write_bytes(b"dicom-b")
            self.assertNotEqual(study_content_fingerprint(left), study_content_fingerprint(right))

    def test_groups_equivalent_ct_reconstructions_and_selects_wider_thin_series(self):
        raw = []
        converted = []
        for uid, coverage, spacing in (("1.2.1", 400, 1), ("1.2.2", 390, 3)):
            raw.append(
                {
                    "SeriesInstanceUID": uid,
                    "SeriesNumber": uid[-1],
                    "Modality": "CT",
                    "SliceCount": 200,
                    "FrameOfReferenceUID": "9.8.7",
                    "AcquisitionNumber": "4",
                    "AcquisitionDateTime": "20260713120000",
                    "ImageOrientationPatient": [1, 0, 0, 0, 1, 0],
                    "ImageType": ["ORIGINAL", "PRIMARY"],
                    "GeometryConfidence": "position",
                    "GeometryMinPositionMm": 0,
                    "GeometryMaxPositionMm": coverage,
                    "CoverageMm": coverage,
                    "ZSpacingMm": spacing,
                }
            )
            converted.append(
                {
                    "SeriesInstanceUID": uid,
                    "DerivedNiftiPath": f"series/{uid}.nii.gz",
                    "DetectedPhase": "portal_venous",
                }
            )
        series, acquisitions = build_inventory(
            raw,
            converted,
            policy={
                "acquisition_time_tolerance_seconds": 30,
                "orientation_tolerance_degrees": 5,
                "minimum_spatial_overlap_ratio": 0.8,
            },
        )
        self.assertEqual(len(acquisitions), 1)
        self.assertEqual(acquisitions[0]["representative_series_uid"], "1.2.1")
        self.assertEqual(sum(bool(item["duplicate_reconstruction"]) for item in series), 1)

    def test_mr_is_inventory_only(self):
        series, acquisitions = build_inventory(
            [
                {
                    "SeriesInstanceUID": "1.2.3",
                    "Modality": "MR",
                    "SliceCount": 50,
                    "GeometryConfidence": "position",
                    "GeometryMinPositionMm": 0,
                    "GeometryMaxPositionMm": 100,
                }
            ],
            [{"SeriesInstanceUID": "1.2.3", "DerivedNiftiPath": "series/mr.nii.gz"}],
            policy={},
        )
        self.assertFalse(series[0]["segmentable"])
        self.assertIn("no_configured_mr_segmenter", series[0]["classification_reasons"])
        self.assertEqual(acquisitions[0]["segmentation_status"], "not_segmentable")

    def test_mask_inventory_distinguishes_complete_truncated_and_absent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reference_path = root / "reference.nii.gz"
            total_dir = root / "total"
            total_dir.mkdir()
            affine = np.eye(4)
            nib.save(nib.Nifti1Image(np.zeros((8, 8, 8)), affine), reference_path)
            complete = np.zeros((8, 8, 8))
            complete[2:6, 2:6, 2:6] = 1
            truncated = np.zeros((8, 8, 8))
            truncated[0:3, 2:6, 2:6] = 1
            nib.save(nib.Nifti1Image(complete, affine), total_dir / "liver.nii.gz")
            nib.save(nib.Nifti1Image(truncated, affine), total_dir / "spleen.nii.gz")
            nib.save(nib.Nifti1Image(np.zeros((8, 8, 8)), affine), total_dir / "pancreas.nii.gz")
            evidence = {item["anatomy_key"]: item for item in inventory_total_masks(total_dir, reference_path)}
        self.assertEqual(evidence["liver"]["state"], "anatomy_complete")
        self.assertEqual(evidence["spleen"]["state"], "anatomy_truncated")
        self.assertEqual(evidence["pancreas"]["state"], "anatomy_not_detected")

    def test_consolidation_never_turns_pending_into_absence(self):
        coverage = consolidate_coverage(
            [
                {"acquisition_id": "a", "representative_series_uid": "s1", "segmentation_status": "done"},
                {
                    "acquisition_id": "b",
                    "representative_series_uid": "s2",
                    "segmentation_status": "segmentation_pending",
                },
            ],
            [
                {
                    "acquisition_id": "a",
                    "series_uid": "s1",
                    "anatomy_key": "liver",
                    "state": "anatomy_not_detected",
                }
            ],
        )
        self.assertEqual(coverage["anatomies"]["liver"]["state"], "unknown")

    def test_consolidation_never_promotes_truncation_while_an_acquisition_is_pending(self):
        coverage = consolidate_coverage(
            [
                {"acquisition_id": "a", "representative_series_uid": "s1", "segmentation_status": "done"},
                {
                    "acquisition_id": "b",
                    "representative_series_uid": "s2",
                    "segmentation_status": "segmentation_pending",
                },
            ],
            [
                {
                    "acquisition_id": "a",
                    "series_uid": "s1",
                    "anatomy_key": "liver",
                    "state": "anatomy_truncated",
                }
            ],
        )
        self.assertEqual(coverage["anatomies"]["liver"]["state"], "unknown")


class TestQcPersistenceAndApi(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        store.ensure_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_versioning_is_idempotent_and_api_exposes_history(self):
        first, created = store.register_qc_analysis(
            self.conn,
            analysis_id="analysis-1",
            study_uid="1.2.840",
            case_id="case",
            fingerprint="fingerprint-1",
            policy_signature="policy",
            qc_resolution={"effective": True},
            pipeline_version="test",
        )
        self.assertTrue(created)
        repeated, created = store.register_qc_analysis(
            self.conn,
            analysis_id="ignored",
            study_uid="1.2.840",
            case_id="case",
            fingerprint="fingerprint-1",
            policy_signature="policy",
            qc_resolution={"effective": True},
        )
        self.assertFalse(created)
        self.assertEqual(repeated["analysis_id"], first["analysis_id"])
        store.persist_qc_inventory(
            self.conn,
            analysis_id="analysis-1",
            series=[{"series_uid": "1.2.840.1", "segmentation_status": "not_segmentable"}],
            acquisitions=[
                {
                    "acquisition_id": "a1",
                    "representative_series_uid": None,
                    "segmentation_status": "not_segmentable",
                }
            ],
        )
        store.update_qc_analysis_summary(
            self.conn,
            analysis_id="analysis-1",
            coverage={"schema_version": 1, "anatomies": {}},
        )

        app = create_app()
        app.dependency_overrides[get_db] = lambda: self.conn
        client = TestClient(app)
        response = client.get("/api/v1/studies/1.2.840/analysis")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["analysis_id"], "analysis-1")
        self.assertEqual(response.json()["series"][0]["series_uid"], "1.2.840.1")
        missing = client.get("/api/v1/studies/9.9/coverage")
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing.json()["detail"]["code"], "qc_analysis_not_available")

    def test_qc_worker_runs_total_only_and_completes_analysis(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "qc.db"

            def connect():
                connection = sqlite3.connect(db_path, check_same_thread=False)
                connection.row_factory = sqlite3.Row
                return connection

            conn = connect()
            store.ensure_schema(conn)
            store.register_qc_analysis(
                conn,
                analysis_id="analysis-worker",
                study_uid="1.2.worker",
                case_id="case-worker",
                fingerprint="fingerprint",
                policy_signature="policy",
                qc_resolution={"effective": True},
            )
            store.persist_qc_inventory(
                conn,
                analysis_id="analysis-worker",
                series=[
                    {
                        "series_uid": "series-worker",
                        "acquisition_id": "acq-worker",
                        "segmentation_status": "segmentation_pending",
                    }
                ],
                acquisitions=[
                    {
                        "acquisition_id": "acq-worker",
                        "representative_series_uid": "series-worker",
                        "segmentation_status": "segmentation_pending",
                    }
                ],
            )
            input_path = root / "input.nii.gz"
            nib.save(nib.Nifti1Image(np.zeros((8, 8, 8)), np.eye(4)), input_path)
            output_path = root / "evidence"
            store.enqueue_qc_segmentation(
                conn,
                analysis_id="analysis-worker",
                acquisition_id="acq-worker",
                case_id="case-worker",
                series_uid="series-worker",
                input_path=str(input_path),
                output_path=str(output_path),
            )
            queue_item = dict(store.claim_next_pending_qc_segmentation(conn))
            conn.close()
            calls = []

            def fake_run_task(task_name, input_file, output_folder, **kwargs):
                calls.append((task_name, kwargs["extra_args"]))
                mask = np.zeros((8, 8, 8))
                mask[2:6, 2:6, 2:6] = 1
                nib.save(nib.Nifti1Image(mask, np.eye(4)), Path(output_folder) / "liver.nii.gz")

            with (
                patch.object(segmentation_worker, "db_connect", side_effect=connect),
                patch.object(
                    segmentation_worker,
                    "_qc_total_task",
                    return_value=("test", {"name": "total", "extra_args": ["--fast", "--device", "cpu"]}),
                ),
                patch.object(segmentation_worker, "run_task", side_effect=fake_run_task),
                patch.object(segmentation_worker, "_qc_model_versions", return_value={"total": {"version": "test"}}),
            ):
                self.assertTrue(segmentation_worker.segment_qc_queue_item(queue_item))

            self.assertEqual(calls, [("total", ["--fast", "--device", "cpu"])])
            conn = connect()
            analysis = store.get_qc_analysis(conn, "1.2.worker", "analysis-worker")
            evidence = store.list_qc_anatomy(conn, "analysis-worker")
            consolidated = conn.execute(
                "SELECT * FROM qc_consolidated_provenance WHERE analysis_id = ?",
                ("analysis-worker",),
            ).fetchall()
            conn.close()
            self.assertEqual(analysis["status"], "complete")
            self.assertEqual(evidence[0]["state"], "anatomy_complete")
            self.assertEqual(json.loads(evidence[0]["payload_json"])["model"]["version"], "test")
            self.assertEqual(consolidated[0]["state"], "anatomy_complete")


if __name__ == "__main__":
    unittest.main()
