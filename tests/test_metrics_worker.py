import json
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from heimdallr.metrics.worker import (
    MetricsLogger,
    _record_metrics_pipeline_state,
    _requested_metrics_modules_from_metadata,
    _execute_jobs,
    _validate_case_against_profile,
    _resolve_enabled_jobs,
    _resolve_job_module_name,
    _resolve_max_parallel_jobs,
    _validate_job_dependency_graph,
    segment_case_metrics,
)


class TestMetricsWorker(unittest.TestCase):
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
                    "locale": "pt_BR",
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
                    "locale": "pt_BR",
                },
            )

            metadata_payload = json.loads(id_json_path.read_text(encoding="utf-8"))
            self.assertEqual(
                metadata_payload["Pipeline"]["metrics_pipeline"]["instruction_pdf"],
                {
                    "path": "artifacts/metrics/instructions/artifact_instructions.pdf",
                    "kind": "pdf",
                    "locale": "pt_BR",
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
                    "locale": "pt_BR",
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
                    "locale": "pt_BR",
                },
            )


if __name__ == "__main__":
    unittest.main()
