import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "check_host_stack_manifest.py"
SPEC = importlib.util.spec_from_file_location("check_host_stack_manifest", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
check_host_stack_manifest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check_host_stack_manifest)


def write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


class TestHostStackManifest(unittest.TestCase):
    def base_manifest(self, *, kind="mps", allowed_devices=None, segmentation_limit=1, metrics_limit=4):
        if allowed_devices is None:
            allowed_devices = ["mps", "cpu"] if kind == "mps" else [kind]
            if kind == "cuda":
                allowed_devices = ["gpu"]
        preferred = "gpu" if kind == "cuda" else kind
        return {
            "schema_version": 1,
            "host": "testhost",
            "role": "test",
            "accelerator": {
                "kind": kind,
                "preferred_device": preferred,
                "allowed_devices": allowed_devices,
            },
            "limits": {
                "segmentation_max_parallel_cases": segmentation_limit,
                "metrics_max_parallel_jobs": metrics_limit,
                "retroactive_workers": 1,
            },
            "profiles": {
                "segmentation": "ct_native_segmentation_only",
                "metrics": "ct_native_basic_metrics",
            },
        }

    def segmentation_config(self, *, device="mps", max_cases=1):
        return {
            "default_profile": "ct_native_segmentation_only",
            "execution": {"max_parallel_cases": max_cases},
            "profiles": {
                "ct_native_segmentation_only": {
                    "tasks": [
                        {
                            "name": "total",
                            "enabled": True,
                            "extra_args": ["--device", device],
                        }
                    ]
                }
            },
        }

    def metrics_config(self, *, max_jobs=4):
        return {
            "default_profile": "ct_native_basic_metrics",
            "profiles": {
                "ct_native_basic_metrics": {
                    "execution": {"max_parallel_jobs": max_jobs},
                    "jobs": [{"name": "l3_muscle_area", "enabled": True}],
                }
            },
        }

    def test_mps_manifest_rejects_gpu_segmentation_task(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            segmentation_path = write_json(
                tmp_path / "segmentation.json",
                self.segmentation_config(device="gpu"),
            )
            metrics_path = write_json(tmp_path / "metrics.json", self.metrics_config())

            result = check_host_stack_manifest.validate_runtime_configs(
                self.base_manifest(kind="mps"),
                segmentation_config_path=segmentation_path,
                metrics_config_path=metrics_path,
            )

        self.assertFalse(result.ok)
        self.assertTrue(any("uses device 'gpu'" in error for error in result.errors))

    def test_cpu_manifest_rejects_parallel_segmentation_limit(self):
        result = check_host_stack_manifest.validate_manifest_shape(
            self.base_manifest(kind="cpu", allowed_devices=["cpu"], segmentation_limit=2),
            current_host="testhost",
            require_current_host=True,
        )

        self.assertFalse(result.ok)
        self.assertTrue(
            any("must be 1 for cpu hosts" in error for error in result.errors)
        )

    def test_metrics_parallel_jobs_must_not_exceed_host_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            segmentation_path = write_json(
                tmp_path / "segmentation.json",
                self.segmentation_config(device="cpu"),
            )
            metrics_path = write_json(tmp_path / "metrics.json", self.metrics_config(max_jobs=4))

            result = check_host_stack_manifest.validate_runtime_configs(
                self.base_manifest(kind="cpu", allowed_devices=["cpu"], metrics_limit=2),
                segmentation_config_path=segmentation_path,
                metrics_config_path=metrics_path,
            )

        self.assertFalse(result.ok)
        self.assertTrue(any("metrics max_parallel_jobs=4" in error for error in result.errors))


if __name__ == "__main__":
    unittest.main()
