import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from heimdallr.metrics.worker import (
    MetricsLogger,
    _execute_jobs,
    _resolve_enabled_jobs,
    _resolve_job_module_name,
    _resolve_max_parallel_jobs,
    _validate_job_dependency_graph,
)


class TestMetricsWorker(unittest.TestCase):
    def test_resolve_job_module_name_uses_conventional_module_path(self):
        module_name = _resolve_job_module_name({"name": "l3_muscle_area"})

        self.assertEqual(module_name, "heimdallr.metrics.jobs.l3_muscle_area")

    def test_resolve_job_module_name_accepts_explicit_module_override(self):
        module_name = _resolve_job_module_name(
            {
                "name": "custom_alias",
                "module": "heimdallr.metrics.jobs.l3_muscle_area",
            }
        )

        self.assertEqual(module_name, "heimdallr.metrics.jobs.l3_muscle_area")

    def test_resolve_job_module_name_rejects_invalid_names(self):
        with self.assertRaisesRegex(RuntimeError, "invalid"):
            _resolve_job_module_name({"name": "../parenchymal_organ_volumetry"})

    def test_resolve_job_module_name_rejects_modules_outside_jobs_namespace(self):
        with self.assertRaisesRegex(RuntimeError, "must resolve inside"):
            _resolve_job_module_name(
                {
                    "name": "l3_muscle_area",
                    "module": "heimdallr.metrics.analysis.body_fat",
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

    def test_resolve_max_parallel_jobs_uses_profile_execution(self):
        self.assertEqual(_resolve_max_parallel_jobs({"execution": {"max_parallel_jobs": 3}}), 3)

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


if __name__ == "__main__":
    unittest.main()
