import unittest

from heimdallr.metrics.worker import _resolve_job_module_name


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


if __name__ == "__main__":
    unittest.main()
