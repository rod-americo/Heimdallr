#!/usr/bin/env python3
"""Validate host-local Heimdallr stack manifests and runtime guardrails."""

from __future__ import annotations

import argparse
import json
import os
import platform
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST_DIR = ROOT / "config" / "host_stack"
VALID_TASK_DEVICES = {"cpu", "gpu", "mps"}
ACCELERATOR_DEVICE_RULES = {
    "cpu": {"required": "cpu", "allowed": {"cpu"}},
    "mps": {"required": "mps", "allowed": {"cpu", "mps"}},
    "cuda": {"required": "gpu", "allowed": {"cpu", "gpu"}},
}


class ValidationResult:
    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def error(self, message: str) -> None:
        self.errors.append(message)

    def warn(self, message: str) -> None:
        self.warnings.append(message)

    @property
    def ok(self) -> bool:
        return not self.errors


def short_hostname(value: str | None = None) -> str:
    raw = value or platform.node() or "unknown"
    return raw.split(".", 1)[0].strip().lower()


def load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise RuntimeError(f"{path} does not exist") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{path} is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{path} must contain a JSON object")
    return payload


def positive_int(value: Any, field_name: str, result: ValidationResult) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        result.error(f"{field_name} must be an integer")
        return None
    if parsed < 1:
        result.error(f"{field_name} must be >= 1")
        return None
    return parsed


def resolve_repo_path(raw_path: str | None) -> Path | None:
    if not raw_path:
        return None
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return ROOT / path


def default_manifest_path() -> Path:
    env_path = os.getenv("HEIMDALLR_HOST_STACK_MANIFEST")
    if env_path:
        return resolve_repo_path(env_path) or Path(env_path)
    return DEFAULT_MANIFEST_DIR / f"{short_hostname()}.json"


def validate_manifest_shape(
    manifest: dict[str, Any],
    *,
    current_host: str,
    require_current_host: bool,
) -> ValidationResult:
    result = ValidationResult()
    if manifest.get("schema_version") != 1:
        result.error("schema_version must be 1")

    host = str(manifest.get("host", "") or "").strip().lower()
    if not host:
        result.error("host is required")
    elif require_current_host and host != current_host:
        result.error(f"manifest host '{host}' does not match current host '{current_host}'")

    accelerator = manifest.get("accelerator")
    if not isinstance(accelerator, dict):
        result.error("accelerator must be an object")
        accelerator = {}

    kind = str(accelerator.get("kind", "") or "").strip().lower()
    if kind not in ACCELERATOR_DEVICE_RULES:
        result.error("accelerator.kind must be one of: cpu, mps, cuda")
        allowed_by_kind = set()
        required_device = None
    else:
        allowed_by_kind = ACCELERATOR_DEVICE_RULES[kind]["allowed"]
        required_device = ACCELERATOR_DEVICE_RULES[kind]["required"]

    raw_allowed = accelerator.get("allowed_devices")
    if not isinstance(raw_allowed, list) or not raw_allowed:
        result.error("accelerator.allowed_devices must be a non-empty array")
        allowed_devices: set[str] = set()
    else:
        allowed_devices = {str(item).strip().lower() for item in raw_allowed}
        invalid = sorted(allowed_devices - VALID_TASK_DEVICES)
        if invalid:
            result.error(f"accelerator.allowed_devices contains invalid device(s): {', '.join(invalid)}")
        outside_kind = sorted(allowed_devices - allowed_by_kind)
        if outside_kind:
            result.error(
                f"accelerator.allowed_devices is incompatible with kind '{kind}': "
                f"{', '.join(outside_kind)}"
            )
        if required_device and required_device not in allowed_devices:
            result.error(
                f"accelerator.allowed_devices for kind '{kind}' must include '{required_device}'"
            )

    preferred = str(accelerator.get("preferred_device", "") or "").strip().lower()
    if preferred and preferred not in allowed_devices:
        result.error("accelerator.preferred_device must be included in allowed_devices")

    limits = manifest.get("limits")
    if not isinstance(limits, dict):
        result.error("limits must be an object")
        limits = {}
    segmentation_limit = positive_int(
        limits.get("segmentation_max_parallel_cases"),
        "limits.segmentation_max_parallel_cases",
        result,
    )
    positive_int(
        limits.get("prepare_max_parallel_cases", 1),
        "limits.prepare_max_parallel_cases",
        result,
    )
    positive_int(
        limits.get("metrics_max_parallel_cases", 1),
        "limits.metrics_max_parallel_cases",
        result,
    )
    positive_int(
        limits.get("metrics_max_parallel_jobs"),
        "limits.metrics_max_parallel_jobs",
        result,
    )
    positive_int(limits.get("retroactive_workers", 1), "limits.retroactive_workers", result)
    if kind in {"cpu", "mps"} and segmentation_limit is not None and segmentation_limit > 1:
        result.error(
            f"limits.segmentation_max_parallel_cases must be 1 for {kind} hosts"
        )

    profiles = manifest.get("profiles")
    if profiles is not None and not isinstance(profiles, dict):
        result.error("profiles must be an object when present")

    config_paths = manifest.get("config_paths")
    if config_paths is not None and not isinstance(config_paths, dict):
        result.error("config_paths must be an object when present")

    return result


def _profile_name(
    config: dict[str, Any],
    explicit_profile: str | None,
    env_var_name: str,
) -> tuple[str | None, str]:
    env_profile = os.getenv(env_var_name)
    if explicit_profile:
        return explicit_profile, "manifest"
    if env_profile:
        return env_profile, "environment"
    default_profile = config.get("default_profile")
    return str(default_profile).strip() if default_profile else None, "config"


def _profile(config: dict[str, Any], profile_name: str | None, result: ValidationResult, label: str) -> dict[str, Any] | None:
    if not profile_name:
        result.error(f"{label} profile is not configured")
        return None
    profiles = config.get("profiles")
    if not isinstance(profiles, dict):
        result.error(f"{label} config is missing profiles object")
        return None
    raw_profile = profiles.get(profile_name)
    if not isinstance(raw_profile, dict):
        result.error(f"{label} profile '{profile_name}' is not defined")
        return None
    return raw_profile


def _task_device(task: dict[str, Any]) -> tuple[str | None, str | None]:
    extra_args = task.get("extra_args", [])
    if not isinstance(extra_args, list):
        return None, "extra_args is not an array"
    for index, value in enumerate(extra_args):
        if str(value) != "--device":
            continue
        if index + 1 >= len(extra_args):
            return None, "--device is missing a value"
        device = str(extra_args[index + 1]).strip().lower()
        if device not in VALID_TASK_DEVICES:
            return device, f"unsupported device '{device}'"
        return device, None
    return None, "missing --device"


def validate_runtime_configs(
    manifest: dict[str, Any],
    *,
    intake_config_path: Path | None = None,
    segmentation_config_path: Path | None,
    metrics_config_path: Path | None,
    qc_evidence_config_path: Path | None = None,
) -> ValidationResult:
    result = ValidationResult()
    accelerator = manifest.get("accelerator", {})
    allowed_devices = {
        str(item).strip().lower()
        for item in accelerator.get("allowed_devices", [])
        if str(item).strip()
    }
    preferred_device = str(accelerator.get("preferred_device", "") or "").strip().lower()
    limits = manifest.get("limits", {})
    profile_expectations = manifest.get("profiles", {})
    segmentation_limit = positive_int(
        limits.get("segmentation_max_parallel_cases", 1),
        "limits.segmentation_max_parallel_cases",
        result,
    ) or 1
    metrics_limit = positive_int(
        limits.get("metrics_max_parallel_jobs", 1),
        "limits.metrics_max_parallel_jobs",
        result,
    ) or 1
    prepare_case_limit = positive_int(
        limits.get("prepare_max_parallel_cases", 1),
        "limits.prepare_max_parallel_cases",
        result,
    ) or 1
    metrics_case_limit = positive_int(
        limits.get("metrics_max_parallel_cases", 1),
        "limits.metrics_max_parallel_cases",
        result,
    ) or 1

    if qc_evidence_config_path is not None and qc_evidence_config_path.exists():
        try:
            qc_config = load_json(qc_evidence_config_path)
        except RuntimeError as exc:
            result.error(str(exc))
        else:
            if qc_config.get("schema_version") != 1:
                result.error("qc_evidence.schema_version must be 1")
            if not isinstance(qc_config.get("enabled"), bool):
                result.error("qc_evidence.enabled must be boolean")
            max_attempts = qc_config.get("execution", {}).get("max_attempts", 2)
            positive_int(max_attempts, "qc_evidence.execution.max_attempts", result)

    if intake_config_path is not None:
        try:
            intake_config = load_json(intake_config_path)
        except RuntimeError as exc:
            result.error(str(exc))
        else:
            prepare_config = intake_config.get("prepare_watchdog", {})
            max_prepare_cases = positive_int(
                prepare_config.get("max_parallel_cases", 1),
                "prepare_watchdog.max_parallel_cases",
                result,
            )
            if max_prepare_cases is not None and max_prepare_cases > prepare_case_limit:
                result.error(
                    f"prepare max_parallel_cases={max_prepare_cases} exceeds host limit "
                    f"{prepare_case_limit}"
                )

    if segmentation_config_path is not None:
        try:
            segmentation_config = load_json(segmentation_config_path)
        except RuntimeError as exc:
            result.error(str(exc))
        else:
            expected_profile = profile_expectations.get("segmentation")
            profile_name, profile_source = _profile_name(
                segmentation_config,
                str(expected_profile).strip() if expected_profile else None,
                "HEIMDALLR_SEGMENTATION_PIPELINE_PROFILE",
            )
            segmentation_profile = _profile(segmentation_config, profile_name, result, "segmentation")
            if segmentation_profile is not None:
                max_cases = positive_int(
                    segmentation_config.get("execution", {}).get("max_parallel_cases", 1),
                    "segmentation.execution.max_parallel_cases",
                    result,
                )
                if max_cases is not None and max_cases > segmentation_limit:
                    result.error(
                        f"segmentation max_parallel_cases={max_cases} exceeds host limit {segmentation_limit}"
                    )
                _ = profile_source
                for task in segmentation_profile.get("tasks", []):
                    if not isinstance(task, dict) or not task.get("enabled", True):
                        continue
                    task_name = str(task.get("name", "") or "<unnamed>").strip()
                    device, device_error = _task_device(task)
                    if device_error:
                        result.error(f"segmentation task '{task_name}' has invalid device config: {device_error}")
                        continue
                    if device not in allowed_devices:
                        result.error(
                            f"segmentation task '{task_name}' uses device '{device}', "
                            f"not allowed on host '{manifest.get('host')}'"
                        )
                    elif preferred_device and device != preferred_device:
                        result.warn(
                            f"segmentation task '{task_name}' uses fallback device '{device}' "
                            f"instead of preferred '{preferred_device}'"
                        )

    if metrics_config_path is not None:
        try:
            metrics_config = load_json(metrics_config_path)
        except RuntimeError as exc:
            result.error(str(exc))
        else:
            max_metric_cases = positive_int(
                metrics_config.get("execution", {}).get("max_parallel_cases", 1),
                "metrics.execution.max_parallel_cases",
                result,
            )
            if max_metric_cases is not None and max_metric_cases > metrics_case_limit:
                result.error(
                    f"metrics max_parallel_cases={max_metric_cases} exceeds host limit "
                    f"{metrics_case_limit}"
                )
            expected_profile = profile_expectations.get("metrics")
            profile_name, profile_source = _profile_name(
                metrics_config,
                str(expected_profile).strip() if expected_profile else None,
                "HEIMDALLR_METRICS_PIPELINE_PROFILE",
            )
            metrics_profile = _profile(metrics_config, profile_name, result, "metrics")
            if metrics_profile is not None:
                max_jobs = positive_int(
                    metrics_profile.get("execution", {}).get("max_parallel_jobs", 1),
                    "metrics.execution.max_parallel_jobs",
                    result,
                )
                if max_jobs is not None and max_jobs > metrics_limit:
                    result.error(
                        f"metrics max_parallel_jobs={max_jobs} exceeds host limit {metrics_limit}"
                    )
                _ = profile_source

    return result


def check_accelerator_runtime(manifest: dict[str, Any]) -> ValidationResult:
    result = ValidationResult()
    kind = str(manifest.get("accelerator", {}).get("kind", "") or "").strip().lower()
    if kind == "cpu":
        return result
    try:
        import torch  # type: ignore
    except Exception as exc:
        result.error(f"cannot import torch to validate {kind} runtime: {exc}")
        return result
    if kind == "mps" and not bool(torch.backends.mps.is_available()):
        result.error("torch.backends.mps.is_available() is false")
    if kind == "cuda" and not bool(torch.cuda.is_available()):
        result.error("torch.cuda.is_available() is false")
    return result


def merge_results(*results: ValidationResult) -> ValidationResult:
    merged = ValidationResult()
    for result in results:
        merged.errors.extend(result.errors)
        merged.warnings.extend(result.warnings)
    return merged


def print_result(manifest_path: Path, manifest: dict[str, Any], result: ValidationResult) -> None:
    print(f"Manifest: {manifest_path}")
    print(f"Host: {manifest.get('host', '<missing>')} (current: {short_hostname()})")
    print(
        "Summary: "
        f"errors={len(result.errors)} warnings={len(result.warnings)}"
    )
    if result.errors:
        print("")
        print("Errors:")
        for error in result.errors:
            print(f"- {error}")
    if result.warnings:
        print("")
        print("Warnings:")
        for warning in result.warnings:
            print(f"- {warning}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a host-local Heimdallr stack manifest.",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="host stack manifest path; defaults to config/host_stack/<hostname>.json",
    )
    parser.add_argument(
        "--intake-config",
        type=Path,
        default=None,
        help="override intake pipeline JSON path",
    )
    parser.add_argument(
        "--segmentation-config",
        type=Path,
        default=None,
        help="override segmentation pipeline JSON path",
    )
    parser.add_argument(
        "--metrics-config",
        type=Path,
        default=None,
        help="override metrics pipeline JSON path",
    )
    parser.add_argument(
        "--qc-evidence-config",
        type=Path,
        default=None,
        help="override optional QC evidence host JSON path",
    )
    parser.add_argument(
        "--manifest-only",
        action="store_true",
        help="validate only the manifest shape and host accelerator policy",
    )
    parser.add_argument(
        "--skip-hostname-check",
        action="store_true",
        help="allow validating a manifest for a host other than this machine",
    )
    parser.add_argument(
        "--check-accelerator-runtime",
        action="store_true",
        help="also import torch and check CUDA/MPS runtime availability",
    )
    parser.add_argument(
        "--warnings-as-errors",
        action="store_true",
        help="return non-zero when warnings are present",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest_path = args.manifest or default_manifest_path()
    if not manifest_path.is_absolute():
        manifest_path = ROOT / manifest_path

    try:
        manifest = load_json(manifest_path)
    except RuntimeError as exc:
        print(f"ERROR: {exc}")
        return 1

    shape_result = validate_manifest_shape(
        manifest,
        current_host=short_hostname(),
        require_current_host=not args.skip_hostname_check,
    )
    results = [shape_result]

    if not args.manifest_only:
        config_paths = manifest.get("config_paths", {})
        intake_config = args.intake_config or resolve_repo_path(
            config_paths.get("intake_pipeline", "config/intake_pipeline.json")
        )
        segmentation_config = args.segmentation_config or resolve_repo_path(
            config_paths.get("segmentation_pipeline", "config/segmentation_pipeline.json")
        )
        metrics_config = args.metrics_config or resolve_repo_path(
            config_paths.get("metrics_pipeline", "config/metrics_pipeline.json")
        )
        qc_evidence_config = args.qc_evidence_config or resolve_repo_path(
            config_paths.get("qc_evidence", "config/qc_evidence.json")
        )
        results.append(
            validate_runtime_configs(
                manifest,
                intake_config_path=intake_config,
                segmentation_config_path=segmentation_config,
                metrics_config_path=metrics_config,
                qc_evidence_config_path=qc_evidence_config,
            )
        )

    if args.check_accelerator_runtime:
        results.append(check_accelerator_runtime(manifest))

    result = merge_results(*results)
    print_result(manifest_path, manifest, result)
    if result.errors or (args.warnings_as_errors and result.warnings):
        return 1
    print("")
    print("Host stack manifest guardrails passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
