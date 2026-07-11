import json
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pydicom
from pydicom.uid import CTImageStorage, generate_uid

from heimdallr.metrics.jobs._dicom_secondary_capture import create_secondary_capture_from_rgb
from heimdallr.metrics.worker import (
    MetricsLogger,
    _record_metrics_pipeline_state,
    _apply_artifact_dicom_policy,
    _apply_artifact_locale,
    _artifact_dicom_policy_from_metadata,
    _effective_artifact_dicom_policy,
    _requested_metrics_modules_from_metadata,
    _execute_jobs,
    _harmonize_secondary_capture_series,
    _remove_result,
    _validate_case_against_profile,
    _resolve_enabled_jobs,
    _resolve_job_module_name,
    _resolve_max_parallel_jobs,
    _validate_job_dependency_graph,
    segment_case_metrics,
)
from heimdallr.shared.automatic_ct import filter_jobs_by_inventory


class TestMetricsWorker(unittest.TestCase):
    def test_example_profile_limits_secondary_capture_1024_to_validated_jobs(self):
        config_path = Path(__file__).resolve().parents[1] / "config" / "metrics_pipeline.example.json"
        config = json.loads(config_path.read_text(encoding="utf-8"))
        jobs = {job["name"]: job for job in config["profiles"]["ct_automatic_metrics"]["jobs"]}

        self.assertEqual(jobs["l3_muscle_area"]["secondary_capture_max_dimension"], 1024)
        self.assertEqual(jobs["vat_sat_ratio"]["secondary_capture_max_dimension"], 1024)
        self.assertNotIn("secondary_capture_max_dimension", jobs["parenchymal_organ_volumetry"])
        self.assertNotIn("secondary_capture_max_dimension", jobs["brain_volumetry"])

    def test_automatic_ct_profile_declares_inventory_requirements(self):
        config_path = Path(__file__).resolve().parents[1] / "config" / "metrics_pipeline.example.json"
        config = json.loads(config_path.read_text(encoding="utf-8"))

        self.assertEqual(config["default_profile"], "ct_automatic_metrics")
        profile = config["profiles"]["ct_automatic_metrics"]
        jobs = {job["name"]: job for job in profile["jobs"]}

        self.assertEqual(profile["planning"]["mode"], "automatic_ct")
        self.assertEqual(jobs["head_complete_qc"]["requires_inventory"], ["brain.complete"])
        self.assertEqual(jobs["l3_muscle_area"]["requires_inventory"], ["vertebrae_L3.complete"])
        self.assertEqual(jobs["bone_health_l1_hu"]["requires_inventory"], ["vertebrae_L1.complete"])
        self.assertEqual(jobs["vat_sat_ratio"]["requires_inventory"], ["vertebrae_L3.complete"])
        self.assertEqual(
            jobs["parenchymal_organ_volumetry"]["requires_inventory"],
            ["parenchymal_organs.any_present"],
        )
        self.assertEqual(jobs["lung_nodules"]["requires_inventory"], ["lungs.any_present"])
        self.assertEqual(jobs["lung_nodules"]["requires_segmentation_tasks"], ["total", "lung_nodules"])
        self.assertTrue(jobs["lung_nodules"]["automatic"])
        self.assertEqual(
            jobs["pleural_pericard_effusion"]["requires_inventory"],
            ["lungs.any_present"],
        )
        self.assertEqual(
            jobs["pleural_pericard_effusion"]["requires_segmentation_tasks"],
            ["total", "pleural_pericard_effusion"],
        )
        self.assertTrue(jobs["pleural_pericard_effusion"]["automatic"])

    def test_filter_jobs_by_inventory_selects_compatible_ct_jobs(self):
        jobs = _resolve_enabled_jobs(
            {
                "jobs": [
                    {"name": "l3_muscle_area", "requires_inventory": ["vertebrae_L3.complete"]},
                    {"name": "vat_sat_ratio", "requires_inventory": ["vertebrae_L3.complete"]},
                    {"name": "head_complete_qc", "requires_inventory": ["brain.complete"]},
                    {
                        "name": "parenchymal_organ_volumetry",
                        "requires_inventory": ["parenchymal_organs.any_present"],
                    },
                    {"name": "lung_nodules", "requires_inventory": ["lungs.any_present"]},
                    {
                        "name": "pleural_pericard_effusion",
                        "requires_inventory": ["lungs.any_present"],
                    },
                ]
            }
        )
        inventory = {
            "brain": {"complete": False},
            "vertebrae_L3": {"complete": True},
            "parenchymal_organs": {"any_present": True},
            "lungs": {"any_present": True},
        }

        selected, skipped = filter_jobs_by_inventory(jobs, inventory)

        self.assertEqual(
            [job["name"] for job in selected],
            [
                "l3_muscle_area",
                "vat_sat_ratio",
                "parenchymal_organ_volumetry",
                "lung_nodules",
                "pleural_pericard_effusion",
            ],
        )
        self.assertEqual([job["name"] for job in skipped], ["head_complete_qc"])

    def test_filter_jobs_by_inventory_skips_lung_nodules_without_lung(self):
        jobs = _resolve_enabled_jobs(
            {
                "jobs": [
                    {"name": "lung_nodules", "requires_inventory": ["lungs.any_present"]},
                ]
            }
        )
        selected, skipped = filter_jobs_by_inventory(jobs, {"lungs": {"any_present": False}})

        self.assertEqual(selected, [])
        self.assertEqual([job["name"] for job in skipped], ["lung_nodules"])

    def test_remove_result_deletes_stale_positive_only_metric(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            results_path = Path(tmpdir) / "resultados.json"
            results_path.write_text(
                json.dumps(
                    {
                        "metrics": {
                            "pleural_pericard_effusion": {"status": "done"},
                            "lung_nodules": {"status": "done"},
                        }
                    }
                ),
                encoding="utf-8",
            )
            with patch("heimdallr.metrics.worker.study_results_json", return_value=results_path):
                _remove_result("case-1", "pleural_pericard_effusion", {})

            results = json.loads(results_path.read_text(encoding="utf-8"))
            self.assertNotIn("pleural_pericard_effusion", results["metrics"])
            self.assertIn("lung_nodules", results["metrics"])

    def test_execute_jobs_keeps_non_published_result_out_of_public_results(self):
        payload = {
            "metric_key": "pleural_pericard_effusion",
            "status": "not_present",
            "publish_result": False,
            "measurement": {"notification_bool": False},
            "artifacts": {},
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                patch("heimdallr.metrics.worker._run_job", return_value=payload),
                patch("heimdallr.metrics.worker._upsert_results") as upsert,
                patch("heimdallr.metrics.worker._remove_result") as remove,
            ):
                completed, exports = _execute_jobs(
                    "case-1",
                    [{"name": "pleural_pericard_effusion", "needs": []}],
                    max_parallel_jobs=1,
                    log_dir=Path(tmpdir),
                    logger=MetricsLogger(None),
                    metadata={},
                )

        upsert.assert_not_called()
        remove.assert_called_once_with("case-1", "pleural_pericard_effusion", {})
        self.assertEqual(completed[0]["status"], "not_present")
        self.assertEqual(exports, [])

    def test_record_metrics_pipeline_state_closes_failed_stage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            id_json_path = Path(tmpdir) / "id.json"
            id_json_path.write_text(
                json.dumps(
                    {
                        "CaseID": "case-1",
                        "StudyInstanceUID": "1.2.3",
                        "Pipeline": {
                            "metrics_start_time": "2026-04-10T17:00:00-03:00",
                        },
                    }
                ),
                encoding="utf-8",
            )

            conn = MagicMock()
            with (
                patch("heimdallr.metrics.worker.study_id_json", return_value=id_json_path),
                patch("heimdallr.metrics.worker.db_connect", return_value=conn),
                patch("heimdallr.metrics.worker.store.update_id_json"),
            ):
                _record_metrics_pipeline_state(
                    "case-1",
                    status="error",
                    end_dt=datetime.fromisoformat("2026-04-10T17:01:15-03:00"),
                    error="job failed",
                )

            payload = json.loads(id_json_path.read_text(encoding="utf-8"))
            pipeline = payload["Pipeline"]
            self.assertEqual(pipeline["metrics_status"], "error")
            self.assertEqual(pipeline["metrics_error"], "job failed")
            self.assertEqual(pipeline["metrics_end_time"], "2026-04-10T17:01:15-03:00")
            self.assertEqual(pipeline["metrics_elapsed_time"], "0:01:15")

    def test_resolve_job_module_name_uses_conventional_module_path(self):
        module_name = _resolve_job_module_name({"name": "l3_muscle_area"})

        self.assertEqual(module_name, "heimdallr.metrics.jobs.l3_muscle_area")

    def test_resolve_job_module_name_accepts_explicit_module_override(self):
        module_name = _resolve_job_module_name(
            {
                "name": "custom_alias",
                "module": "heimdallr.metrics.analysis.opportunistic_osteoporosis_composite",
            }
        )

        self.assertEqual(module_name, "heimdallr.metrics.analysis.opportunistic_osteoporosis_composite")

    def test_resolve_job_module_name_rejects_invalid_names(self):
        with self.assertRaisesRegex(RuntimeError, "invalid"):
            _resolve_job_module_name({"name": "../parenchymal_organ_volumetry"})

    def test_resolve_job_module_name_rejects_modules_outside_jobs_namespace(self):
        with self.assertRaisesRegex(RuntimeError, "must resolve inside"):
            _resolve_job_module_name(
                {
                    "name": "l3_muscle_area",
                    "module": "heimdallr.shared.settings",
                }
            )

    def test_resolve_job_module_name_rejects_missing_modules(self):
        with self.assertRaisesRegex(RuntimeError, "module not found"):
            _resolve_job_module_name({"name": "job_that_does_not_exist"})

    def test_validate_job_dependency_graph_rejects_missing_dependency(self):
        jobs = _resolve_enabled_jobs(
            {
                "jobs": [
                    {"name": "l3_muscle_area", "needs": ["bone_health_l1_hu"]},
                ]
            }
        )

        with self.assertRaisesRegex(RuntimeError, "depends on missing job"):
            _validate_job_dependency_graph(jobs)

    def test_validate_job_dependency_graph_rejects_cycle(self):
        jobs = _resolve_enabled_jobs(
            {
                "jobs": [
                    {"name": "l3_muscle_area", "needs": ["bone_health_l1_hu"]},
                    {"name": "bone_health_l1_hu", "needs": ["l3_muscle_area"]},
                ]
            }
        )

        with self.assertRaisesRegex(RuntimeError, "cycle detected"):
            _validate_job_dependency_graph(jobs)

    def test_resolve_enabled_jobs_filters_to_requested_jobs_and_dependencies(self):
        jobs = _resolve_enabled_jobs(
            {
                "jobs": [
                    {"name": "l3_muscle_area"},
                    {"name": "parenchymal_organ_volumetry"},
                    {
                        "name": "opportunistic_osteoporosis_composite",
                        "needs": ["l3_muscle_area", "parenchymal_organ_volumetry"],
                    },
                ]
            },
            requested_job_names=["opportunistic_osteoporosis_composite"],
        )

        self.assertEqual(
            [job["name"] for job in jobs],
            ["l3_muscle_area", "parenchymal_organ_volumetry", "opportunistic_osteoporosis_composite"],
        )

    def test_resolve_enabled_jobs_includes_automatic_jobs_with_requested_subset(self):
        jobs = _resolve_enabled_jobs(
            {
                "jobs": [
                    {"name": "bone_health_l1_hu"},
                    {"name": "head_complete_qc", "automatic": True},
                ]
            },
            requested_job_names=["bone_health_l1_hu"],
        )

        self.assertEqual(
            [job["name"] for job in jobs],
            ["bone_health_l1_hu", "head_complete_qc"],
        )

    def test_resolve_enabled_jobs_rejects_unknown_requested_job(self):
        with self.assertRaisesRegex(RuntimeError, "Requested metrics job"):
            _resolve_enabled_jobs(
                {"jobs": [{"name": "l3_muscle_area"}]},
                requested_job_names=["does_not_exist"],
            )

    def test_requested_metrics_modules_from_metadata_reads_external_delivery(self):
        names = _requested_metrics_modules_from_metadata(
            {
                "ExternalDelivery": {
                    "requested_metrics_modules": ["l3_muscle_area", "bone_health_l1_hu", "l3_muscle_area"]
                }
            }
        )

        self.assertEqual(names, ["l3_muscle_area", "bone_health_l1_hu"])

    def test_apply_artifact_locale_sets_job_locale_from_contract(self):
        jobs = [
            {"name": "bone_health_l1_hu"},
            {"name": "vat_sat_ratio", "locale": "en_US"},
        ]

        configured = _apply_artifact_locale(jobs, "pt_BR")

        self.assertEqual(configured[0]["locale"], "pt_BR")
        self.assertEqual(configured[1]["locale"], "pt_BR")
        self.assertNotIn("locale", jobs[0])

    def test_artifact_dicom_policy_from_metadata_reads_external_delivery(self):
        policy = _artifact_dicom_policy_from_metadata(
            {
                "ExternalDelivery": {
                    "artifact_dicom_policy": {
                        "secondary_capture_transfer_syntax": "jpeg_2000_lossless"
                    }
                }
            }
        )

        self.assertEqual(policy, {"secondary_capture_transfer_syntax": "jpeg_2000_lossless"})

    def test_effective_artifact_dicom_policy_defaults_to_separate(self):
        self.assertEqual(
            _effective_artifact_dicom_policy({"execution": {}}, {}),
            {"secondary_capture_series_mode": "separate"},
        )

    def test_effective_artifact_dicom_policy_merges_api_over_profile(self):
        policy = _effective_artifact_dicom_policy(
            {
                "execution": {
                    "artifact_dicom_policy": {
                        "secondary_capture_transfer_syntax": "jpeg_ls_lossless",
                        "secondary_capture_series_mode": "separate",
                    }
                }
            },
            {
                "ExternalDelivery": {
                    "artifact_dicom_policy": {
                        "secondary_capture_series_mode": "single_series",
                    }
                }
            },
        )

        self.assertEqual(
            policy,
            {
                "secondary_capture_transfer_syntax": "jpeg_ls_lossless",
                "secondary_capture_series_mode": "single_series",
            },
        )

    def test_apply_artifact_dicom_policy_sets_job_transfer_syntax(self):
        jobs = [
            {"name": "l3_muscle_area"},
            {"name": "vat_sat_ratio", "secondary_capture_transfer_syntax": "original"},
        ]

        configured = _apply_artifact_dicom_policy(
            jobs,
            {"secondary_capture_transfer_syntax": "rle_lossless"},
        )

        self.assertEqual(configured[0]["secondary_capture_transfer_syntax"], "rle_lossless")
        self.assertEqual(configured[1]["secondary_capture_transfer_syntax"], "rle_lossless")
        self.assertNotIn("secondary_capture_transfer_syntax", jobs[0])

    def test_harmonize_secondary_capture_series_rewrites_only_secondary_capture(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_root = Path(tmpdir)
            metrics_dir = case_root / "artifacts" / "metrics"
            sc_a = metrics_dir / "job_a" / "a.dcm"
            sc_b = metrics_dir / "job_b" / "b.dcm"
            ct_path = metrics_dir / "head" / "derived_ct.dcm"
            metadata = {"StudyInstanceUID": "1.2.826.0.1.3680043.10.543.1", "PatientName": "Test^Case"}
            rgb = np.zeros((8, 8, 3), dtype=np.uint8)

            for path, series_number, location in ((sc_a, 9001, 20.0), (sc_b, 9002, 10.0)):
                path.parent.mkdir(parents=True, exist_ok=True)
                create_secondary_capture_from_rgb(
                    rgb,
                    path,
                    metadata,
                    series_description="Original",
                    series_number=series_number,
                    instance_number=1,
                    derivation_description="Test artifact",
                    image_position_patient=[0.0, 0.0, location],
                    image_orientation_patient=[1.0, 0.0, 0.0, 0.0, 1.0, 0.0],
                    transfer_syntax="original",
                )
            ct_path.parent.mkdir(parents=True, exist_ok=True)
            create_secondary_capture_from_rgb(
                rgb,
                ct_path,
                metadata,
                series_description="Derived CT Stand-in",
                series_number=9300,
                instance_number=1,
                derivation_description="Test artifact",
                transfer_syntax="original",
            )
            ct_dataset = pydicom.dcmread(str(ct_path))
            original_ct_series_uid = str(ct_dataset.SeriesInstanceUID)
            ct_dataset.SOPClassUID = CTImageStorage
            ct_dataset.file_meta.MediaStorageSOPClassUID = CTImageStorage
            ct_dataset.SOPInstanceUID = str(generate_uid())
            ct_dataset.file_meta.MediaStorageSOPInstanceUID = ct_dataset.SOPInstanceUID
            ct_dataset.save_as(str(ct_path), write_like_original=False)

            series_uid = _harmonize_secondary_capture_series(
                case_root,
                [
                    {"path": "artifacts/metrics/job_a/a.dcm", "kind": "secondary_capture"},
                    {"path": "artifacts/metrics/head/derived_ct.dcm", "kind": "derived_ct"},
                    {"path": "artifacts/metrics/job_b/b.dcm", "kind": "secondary_capture"},
                ],
                None,
                series_instance_uid="1.2.826.0.1.3680043.10.543.999",
            )

            self.assertEqual(series_uid, "1.2.826.0.1.3680043.10.543.999")
            first = pydicom.dcmread(str(sc_a))
            second = pydicom.dcmread(str(sc_b))
            derived = pydicom.dcmread(str(ct_path))
            self.assertEqual(first.SeriesInstanceUID, series_uid)
            self.assertEqual(second.SeriesInstanceUID, series_uid)
            self.assertEqual(first.SeriesNumber, 9900)
            self.assertEqual(second.SeriesNumber, 9900)
            self.assertEqual(first.SeriesDescription, "Heimdallr Artifact Series")
            self.assertEqual(second.SeriesDescription, "Heimdallr Artifact Series")
            self.assertEqual(first.InstanceNumber, 2)
            self.assertEqual(second.InstanceNumber, 1)
            self.assertEqual(str(derived.SeriesInstanceUID), original_ct_series_uid)
            self.assertEqual(str(derived.SOPClassUID), str(CTImageStorage))

    def test_resolve_max_parallel_jobs_uses_profile_execution(self):
        self.assertEqual(_resolve_max_parallel_jobs({"execution": {"max_parallel_jobs": 3}}), 3)

    def test_validate_case_against_profile_accepts_portal_venous_fallback(self):
        metadata = {
            "Modality": "CT",
            "Pipeline": {
                "series_selection": {
                    "SelectedPhase": "portal_venous",
                }
            },
        }

        _validate_case_against_profile(
            "case-1",
            metadata,
            "ct_native_basic_metrics",
            {"required": {"modality": "CT", "selected_phase": ["native"]}},
        )

    def test_validate_case_against_profile_accepts_any_contrast_fallback(self):
        metadata = {
            "Modality": "CT",
            "Pipeline": {
                "series_selection": {
                    "SelectedPhase": "arterial",
                }
            },
        }

        _validate_case_against_profile(
            "case-1",
            metadata,
            "ct_native_basic_metrics",
            {"required": {"modality": "CT", "selected_phase": ["native"]}},
        )

    def test_execute_jobs_runs_independent_jobs_before_dependents(self):
        jobs = _resolve_enabled_jobs(
            {
                "jobs": [
                    {"name": "l3_muscle_area"},
                    {"name": "parenchymal_organ_volumetry"},
                    {
                        "name": "opportunistic_osteoporosis_composite",
                        "needs": ["l3_muscle_area", "parenchymal_organ_volumetry"],
                    },
                ]
            }
        )
        _validate_job_dependency_graph(jobs)

        event_log: list[tuple[str, str, float]] = []

        def fake_run_job(case_id, job, log_dir):
            start = time.perf_counter()
            event_log.append((job["name"], "start", start))
            if job["name"] != "opportunistic_osteoporosis_composite":
                time.sleep(0.05)
            end = time.perf_counter()
            event_log.append((job["name"], "end", end))
            return {
                "status": "done",
                "measurement": {"job_status": "complete"},
                "artifacts": {"result_json": f"artifacts/metrics/{job['name']}/result.json"},
                "dicom_exports": [],
            }

        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir)
            logger = MetricsLogger(None)
            with (
                patch("heimdallr.metrics.worker._run_job", side_effect=fake_run_job),
                patch("heimdallr.metrics.worker._upsert_results"),
            ):
                completed_jobs, dicom_exports = _execute_jobs(
                    "case-1",
                    jobs,
                    max_parallel_jobs=2,
                    log_dir=log_dir,
                    logger=logger,
                    metadata={},
                )

        self.assertEqual([job["name"] for job in completed_jobs], [job["name"] for job in jobs])
        self.assertEqual(dicom_exports, [])

        starts = {(name, stage): timestamp for name, stage, timestamp in event_log}
        self.assertLess(
            abs(starts[("l3_muscle_area", "start")] - starts[("parenchymal_organ_volumetry", "start")]),
            0.04,
        )
        self.assertGreaterEqual(
            starts[("opportunistic_osteoporosis_composite", "start")],
            starts[("l3_muscle_area", "end")],
        )
        self.assertGreaterEqual(
            starts[("opportunistic_osteoporosis_composite", "start")],
            starts[("parenchymal_organ_volumetry", "end")],
        )

    def test_segment_case_metrics_groups_secondary_capture_exports_when_configured(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "CaseSC_20260415_1"
            case_dir = root / case_id
            metadata_dir = case_dir / "metadata"
            logs_dir = case_dir / "logs"
            sc_dir = case_dir / "artifacts" / "metrics" / "mock"
            metadata_dir.mkdir(parents=True, exist_ok=True)
            logs_dir.mkdir(parents=True, exist_ok=True)
            id_json_path = metadata_dir / "id.json"
            results_json_path = metadata_dir / "resultados.json"
            case_metadata = {
                "CaseID": case_id,
                "StudyInstanceUID": "1.2.826.0.1.3680043.10.543.2",
                "PatientName": "Alice Example",
                "AccessionNumber": "123",
                "StudyDate": "20260415",
                "Modality": "CT",
                "Pipeline": {"series_selection": {"SelectedPhase": "native"}},
            }
            id_json_path.write_text(json.dumps(case_metadata), encoding="utf-8")
            results_json_path.write_text("{}", encoding="utf-8")
            rgb = np.zeros((8, 8, 3), dtype=np.uint8)
            first_path = sc_dir / "first.dcm"
            second_path = sc_dir / "second.dcm"

            def fake_execute_jobs(*args, **kwargs):
                sc_dir.mkdir(parents=True, exist_ok=True)
                create_secondary_capture_from_rgb(
                    rgb,
                    first_path,
                    case_metadata,
                    series_description="First",
                    series_number=9001,
                    instance_number=1,
                    derivation_description="First artifact",
                    transfer_syntax="original",
                )
                create_secondary_capture_from_rgb(
                    rgb,
                    second_path,
                    case_metadata,
                    series_description="Second",
                    series_number=9002,
                    instance_number=1,
                    derivation_description="Second artifact",
                    transfer_syntax="original",
                )
                return (
                    [{"name": "mock_sc", "status": "done"}],
                    [
                        {"path": "artifacts/metrics/mock/first.dcm", "kind": "secondary_capture"},
                        {"path": "artifacts/metrics/mock/second.dcm", "kind": "secondary_capture"},
                    ],
                )

            with (
                patch("heimdallr.metrics.worker.study_id_json", return_value=id_json_path),
                patch("heimdallr.metrics.worker.study_results_json", return_value=results_json_path),
                patch("heimdallr.metrics.worker.study_logs_dir", return_value=logs_dir),
                patch("heimdallr.metrics.worker.study_dir", return_value=case_dir),
                patch(
                    "heimdallr.metrics.worker.load_metrics_pipeline_profile",
                    return_value=(
                        "test_profile",
                        {
                            "execution": {
                                "artifact_dicom_policy": {
                                    "secondary_capture_series_mode": "single_series",
                                }
                            },
                            "jobs": [{"name": "mock_sc"}],
                        },
                    ),
                ),
                patch("heimdallr.metrics.worker._validate_case_against_profile"),
                patch("heimdallr.metrics.worker._resolve_enabled_jobs", return_value=[{"name": "mock_sc"}]),
                patch("heimdallr.metrics.worker._validate_job_dependency_graph"),
                patch("heimdallr.metrics.worker._resolve_max_parallel_jobs", return_value=1),
                patch("heimdallr.metrics.worker._execute_jobs", side_effect=fake_execute_jobs),
                patch("heimdallr.metrics.worker._generate_instruction_pdf_artifact", return_value=None),
                patch("heimdallr.metrics.worker._enqueue_case_dicom_exports", return_value=2),
                patch("heimdallr.metrics.worker.settings.ARTIFACTS_LOCALE", "en_US"),
                patch("heimdallr.metrics.worker.db_connect") as mock_connect,
                patch("heimdallr.metrics.worker.store.update_metrics_completion"),
                patch("heimdallr.metrics.worker.store.update_calculation_results"),
                patch("heimdallr.metrics.worker.store.update_id_json"),
            ):
                mock_connect.return_value = MagicMock()
                ok = segment_case_metrics(case_dir)

            self.assertTrue(ok)
            first = pydicom.dcmread(str(first_path))
            second = pydicom.dcmread(str(second_path))
            self.assertEqual(first.SeriesInstanceUID, second.SeriesInstanceUID)
            self.assertEqual(first.SeriesDescription, "Heimdallr Artifact Series")
            self.assertEqual(second.SeriesDescription, "Heimdallr Artifact Series")
            self.assertEqual(first.InstanceNumber, 1)
            self.assertEqual(second.InstanceNumber, 2)
            metadata_payload = json.loads(id_json_path.read_text(encoding="utf-8"))
            policy = metadata_payload["Pipeline"]["metrics_pipeline"]["artifact_dicom_policy"]
            self.assertEqual(policy["secondary_capture_series_mode"], "single_series")
            self.assertEqual(policy["secondary_capture_series_instance_uid"], str(first.SeriesInstanceUID))

    def test_segment_case_metrics_generates_instruction_sc_by_default(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "CasePDF_20260415_1"
            case_dir = root / case_id
            metadata_dir = case_dir / "metadata"
            logs_dir = case_dir / "logs"
            artifacts_dir = case_dir / "artifacts" / "metrics" / "instructions"
            metadata_dir.mkdir(parents=True, exist_ok=True)
            logs_dir.mkdir(parents=True, exist_ok=True)
            id_json_path = metadata_dir / "id.json"
            results_json_path = metadata_dir / "resultados.json"
            id_json_path.write_text(
                json.dumps(
                    {
                        "CaseID": case_id,
                        "StudyInstanceUID": "1.2.3",
                        "PatientName": "Alice Example",
                        "AccessionNumber": "123",
                        "StudyDate": "20260415",
                        "Modality": "CT",
                        "Pipeline": {"series_selection": {"SelectedPhase": "native"}},
                    }
                ),
                encoding="utf-8",
            )
            results_json_path.write_text("{}", encoding="utf-8")
            pdf_path = artifacts_dir / "artifact_instructions.pdf"
            sc_dir = artifacts_dir / "dicom_sc"
            sc_paths = [sc_dir / f"page_{idx:02d}.dcm" for idx in range(1, 3)]

            def fake_build_pdf(_case_id: str) -> Path:
                artifacts_dir.mkdir(parents=True, exist_ok=True)
                pdf_path.write_bytes(b"%PDF-1.4\n%mock\n")
                return pdf_path

            def fake_build_sc(_case_id: str):
                sc_dir.mkdir(parents=True, exist_ok=True)
                for path in sc_paths:
                    path.write_bytes(b"DICM")
                return {
                    "series_instance_uid": "1.2.3.4",
                    "paths": sc_paths,
                }

            with (
                patch("heimdallr.metrics.worker.study_id_json", return_value=id_json_path),
                patch("heimdallr.metrics.worker.study_results_json", return_value=results_json_path),
                patch("heimdallr.metrics.worker.study_logs_dir", return_value=logs_dir),
                patch("heimdallr.metrics.worker.study_dir", return_value=case_dir),
                patch("heimdallr.metrics.worker.load_metrics_pipeline_profile", return_value=("test_profile", {"jobs": [{"name": "l3_muscle_area"}]})),
                patch("heimdallr.metrics.worker._validate_case_against_profile"),
                patch("heimdallr.metrics.worker._resolve_enabled_jobs", return_value=[{"name": "l3_muscle_area"}]),
                patch("heimdallr.metrics.worker._validate_job_dependency_graph"),
                patch("heimdallr.metrics.worker._resolve_max_parallel_jobs", return_value=1),
                patch(
                    "heimdallr.metrics.worker._execute_jobs",
                    return_value=([{"name": "l3_muscle_area", "status": "done"}], []),
                ),
                patch("heimdallr.metrics.worker._enqueue_case_dicom_exports", return_value=0) as enqueue_mock,
                patch("heimdallr.metrics.worker.build_artifact_instructions_pdf", side_effect=fake_build_pdf),
                patch("heimdallr.metrics.worker.build_artifact_instructions_secondary_capture", side_effect=fake_build_sc),
                patch("heimdallr.metrics.worker.settings.ARTIFACTS_LOCALE", "en_US"),
                patch("heimdallr.metrics.worker.db_connect") as mock_connect,
                patch("heimdallr.metrics.worker.store.update_metrics_completion"),
                patch("heimdallr.metrics.worker.store.update_calculation_results"),
                patch("heimdallr.metrics.worker.store.update_id_json"),
            ):
                mock_connect.return_value = MagicMock()
                ok = segment_case_metrics(case_dir)

            self.assertTrue(ok)
            self.assertTrue(pdf_path.exists())
            for path in sc_paths:
                self.assertTrue(path.exists())
            enqueue_call = enqueue_mock.call_args
            self.assertIsNotNone(enqueue_call)
            enqueued_exports = enqueue_call.args[2]
            self.assertEqual(enqueued_exports, [])

            results_payload = json.loads(results_json_path.read_text(encoding="utf-8"))
            self.assertEqual(
                results_payload["artifacts"]["artifact_instructions_pdf"],
                {
                    "path": "artifacts/metrics/instructions/artifact_instructions.pdf",
                    "kind": "pdf",
                    "locale": "en_US",
                },
            )
            self.assertEqual(
                results_payload["artifacts"]["artifact_instructions_sc"],
                {
                    "paths": [
                        "artifacts/metrics/instructions/dicom_sc/page_01.dcm",
                        "artifacts/metrics/instructions/dicom_sc/page_02.dcm",
                    ],
                    "kind": "secondary_capture",
                    "series_instance_uid": "1.2.3.4",
                    "locale": "en_US",
                },
            )

            metadata_payload = json.loads(id_json_path.read_text(encoding="utf-8"))
            self.assertEqual(
                metadata_payload["Pipeline"]["metrics_pipeline"]["instruction_pdf"],
                {
                    "path": "artifacts/metrics/instructions/artifact_instructions.pdf",
                    "kind": "pdf",
                    "locale": "en_US",
                },
            )
            self.assertEqual(
                metadata_payload["Pipeline"]["metrics_pipeline"]["instruction_dicom"],
                {
                    "paths": [
                        "artifacts/metrics/instructions/dicom_sc/page_01.dcm",
                        "artifacts/metrics/instructions/dicom_sc/page_02.dcm",
                    ],
                    "kind": "secondary_capture",
                    "series_instance_uid": "1.2.3.4",
                    "locale": "en_US",
                },
            )

    def test_segment_case_metrics_allows_encapsulated_pdf_instruction_export(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_id = "CasePDF_20260415_2"
            case_dir = root / case_id
            metadata_dir = case_dir / "metadata"
            logs_dir = case_dir / "logs"
            artifacts_dir = case_dir / "artifacts" / "metrics" / "instructions"
            metadata_dir.mkdir(parents=True, exist_ok=True)
            logs_dir.mkdir(parents=True, exist_ok=True)
            id_json_path = metadata_dir / "id.json"
            results_json_path = metadata_dir / "resultados.json"
            id_json_path.write_text(
                json.dumps(
                    {
                        "CaseID": case_id,
                        "StudyInstanceUID": "1.2.4",
                        "PatientName": "Alice Example",
                        "AccessionNumber": "123",
                        "StudyDate": "20260415",
                        "Modality": "CT",
                        "Pipeline": {"series_selection": {"SelectedPhase": "native"}},
                    }
                ),
                encoding="utf-8",
            )
            results_json_path.write_text("{}", encoding="utf-8")
            pdf_path = artifacts_dir / "artifact_instructions.pdf"
            dicom_path = artifacts_dir / "artifact_instructions.dcm"

            def fake_build_pdf(_case_id: str) -> Path:
                artifacts_dir.mkdir(parents=True, exist_ok=True)
                pdf_path.write_bytes(b"%PDF-1.4\n%mock\n")
                return pdf_path

            def fake_create_dicom(**kwargs):
                dicom_path.parent.mkdir(parents=True, exist_ok=True)
                Path(kwargs["output_path"]).write_bytes(b"DICM")

            with (
                patch("heimdallr.metrics.worker.study_id_json", return_value=id_json_path),
                patch("heimdallr.metrics.worker.study_results_json", return_value=results_json_path),
                patch("heimdallr.metrics.worker.study_logs_dir", return_value=logs_dir),
                patch("heimdallr.metrics.worker.study_dir", return_value=case_dir),
                patch(
                    "heimdallr.metrics.worker.load_metrics_pipeline_profile",
                    return_value=("test_profile", {"execution": {"instruction_dicom_kind": "encapsulated_pdf"}, "jobs": [{"name": "l3_muscle_area"}]}),
                ),
                patch("heimdallr.metrics.worker._validate_case_against_profile"),
                patch("heimdallr.metrics.worker._resolve_enabled_jobs", return_value=[{"name": "l3_muscle_area"}]),
                patch("heimdallr.metrics.worker._validate_job_dependency_graph"),
                patch("heimdallr.metrics.worker._resolve_max_parallel_jobs", return_value=1),
                patch(
                    "heimdallr.metrics.worker._execute_jobs",
                    return_value=([{"name": "l3_muscle_area", "status": "done"}], []),
                ),
                patch("heimdallr.metrics.worker._enqueue_case_dicom_exports", return_value=0) as enqueue_mock,
                patch("heimdallr.metrics.worker.build_artifact_instructions_pdf", side_effect=fake_build_pdf),
                patch("heimdallr.metrics.worker.create_encapsulated_pdf_dicom", side_effect=fake_create_dicom),
                patch("heimdallr.metrics.worker.settings.ARTIFACTS_LOCALE", "en_US"),
                patch("heimdallr.metrics.worker.db_connect") as mock_connect,
                patch("heimdallr.metrics.worker.store.update_metrics_completion"),
                patch("heimdallr.metrics.worker.store.update_calculation_results"),
                patch("heimdallr.metrics.worker.store.update_id_json"),
            ):
                mock_connect.return_value = MagicMock()
                ok = segment_case_metrics(case_dir)

            self.assertTrue(ok)
            self.assertTrue(pdf_path.exists())
            self.assertTrue(dicom_path.exists())
            enqueued_exports = enqueue_mock.call_args.args[2]
            self.assertEqual(enqueued_exports, [])

            results_payload = json.loads(results_json_path.read_text(encoding="utf-8"))
            self.assertEqual(
                results_payload["artifacts"]["artifact_instructions_dicom"],
                {
                    "path": "artifacts/metrics/instructions/artifact_instructions.dcm",
                    "kind": "encapsulated_pdf",
                    "locale": "en_US",
                },
            )


if __name__ == "__main__":
    unittest.main()
