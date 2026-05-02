"""Build the final delivery package for an externally submitted case."""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from heimdallr.control_plane.case_pdf_report import build_case_report
from heimdallr.integration.submissions import normalize_requested_outputs
from heimdallr.metrics.jobs._dicom_encapsulated_pdf import create_encapsulated_pdf_dicom
from heimdallr.shared.paths import (
    study_artifacts_dir,
    study_dir,
    study_id_json,
    study_metadata_json,
    study_results_json,
)


_SAFE_NAME_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")
REPORT_DICOM_FILENAME = "report.dcm"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_package_stem(value: str) -> str:
    normalized = _SAFE_NAME_PATTERN.sub("_", value.strip()).strip("._")
    return normalized or "heimdallr_case"


def _artifact_file_count(artifacts_root: Path) -> int:
    if not artifacts_root.exists():
        return 0
    return sum(1 for path in artifacts_root.rglob("*") if path.is_file())


def _zip_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _add_file(zip_handle: zipfile.ZipFile, source_path: Path, archive_path: str) -> None:
    zip_handle.write(source_path, arcname=archive_path)


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _iter_metric_files(metrics_artifacts_root: Path, suffix: str) -> list[Path]:
    if not metrics_artifacts_root.exists():
        return []
    return sorted(
        path
        for path in metrics_artifacts_root.rglob(f"*{suffix}")
        if path.is_file()
    )


def _build_report_dicom(case_root: Path, report_path: Path, metadata: dict[str, Any]) -> Path:
    output_path = case_root / "metadata" / REPORT_DICOM_FILENAME
    create_encapsulated_pdf_dicom(
        pdf_path=report_path,
        output_path=output_path,
        case_metadata=metadata,
        series_description="Heimdallr Case Report",
        document_title="Heimdallr Case Report",
        series_number=942,
        instance_number=1,
    )
    return output_path


class DeliveryPackageBuilder:
    def __init__(self, *, zip_handle: zipfile.ZipFile, case_root: Path) -> None:
        self.zip_handle = zip_handle
        self.case_root = case_root
        self.added_paths: set[Path] = set()
        self.delivered_outputs: dict[str, list[str]] = {}
        self.missing_outputs: list[str] = []

    def add_output_file(self, output_key: str, source_path: Path, archive_path: str | None = None) -> None:
        if not source_path.exists() or not source_path.is_file():
            self.missing_outputs.append(output_key)
            return
        resolved_path = source_path.resolve()
        if resolved_path in self.added_paths:
            self.delivered_outputs.setdefault(output_key, []).append(
                archive_path or str(source_path.relative_to(self.case_root))
            )
            return
        if archive_path is None:
            archive_path = str(source_path.relative_to(self.case_root))
        _add_file(self.zip_handle, source_path, archive_path)
        self.added_paths.add(resolved_path)
        self.delivered_outputs.setdefault(output_key, []).append(archive_path)

    def add_optional_files(self, output_key: str, paths: list[Path]) -> None:
        existing = [path for path in paths if path.exists() and path.is_file()]
        if not existing:
            self.missing_outputs.append(output_key)
            return
        for path in existing:
            self.add_output_file(output_key, path)


def _write_manifest(zip_handle: zipfile.ZipFile, manifest_payload: dict[str, Any]) -> None:
    zip_handle.writestr("manifest.json", json.dumps(manifest_payload, indent=2, ensure_ascii=False))


def build_delivery_package(
    *,
    case_id: str,
    job_id: str,
    client_case_id: str | None,
    source_system: str | None,
    requested_outputs: dict[str, Any] | None,
) -> tuple[dict[str, Any], Path]:
    case_root = study_dir(case_id)
    id_json_path = study_id_json(case_id)
    if not id_json_path.exists():
        raise FileNotFoundError(f"Missing id.json for case {case_id}")

    metadata = _load_json(id_json_path)
    requested = normalize_requested_outputs(requested_outputs)
    results_path = study_results_json(case_id)
    metadata_json_path = study_metadata_json(case_id)
    report_path = case_root / "metadata" / "report.pdf"
    report_dicom_path = case_root / "metadata" / REPORT_DICOM_FILENAME
    metrics_artifacts_root = study_artifacts_dir(case_id) / "metrics"
    instructions_root = metrics_artifacts_root / "instructions"

    if requested.get("report_pdf", True) or requested.get("report_pdf_dicom", False):
        report_path = build_case_report(case_root)
    if requested.get("report_pdf_dicom", False):
        report_dicom_path = _build_report_dicom(case_root, report_path, metadata)

    package_stem = _safe_package_stem(client_case_id or case_id)
    package_name = f"heimdallr_{package_stem}.zip"

    temp_dir = Path(tempfile.mkdtemp(prefix="heimdallr-delivery-"))
    package_path = temp_dir / package_name

    with zipfile.ZipFile(package_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_handle:
        builder = DeliveryPackageBuilder(zip_handle=zip_handle, case_root=case_root)

        if requested.get("id_json", True):
            builder.add_output_file("id_json", id_json_path, "metadata/id.json")
        if requested.get("metadata_json", True):
            builder.add_output_file("metadata_json", metadata_json_path, "metadata/metadata.json")
        if requested.get("metrics_json", True):
            metric_result_paths = _iter_metric_files(metrics_artifacts_root, "result.json")
            builder.add_output_file("metrics_json", results_path, "metadata/resultados.json")
            builder.add_optional_files("metric_result_json", metric_result_paths)
        if requested.get("overlays_png", True):
            builder.add_optional_files("overlays_png", _iter_metric_files(metrics_artifacts_root, ".png"))
        if requested.get("overlays_dicom", True):
            overlay_dicom_paths = [
                path
                for path in _iter_metric_files(metrics_artifacts_root, ".dcm")
                if not _is_under(path, instructions_root)
            ]
            builder.add_optional_files("overlays_dicom", overlay_dicom_paths)
        if requested.get("report_pdf", True):
            builder.add_output_file("report_pdf", report_path, "metadata/report.pdf")
        if requested.get("report_pdf_dicom", False):
            builder.add_output_file("report_pdf_dicom", report_dicom_path, f"metadata/{REPORT_DICOM_FILENAME}")
        if requested.get("artifact_instructions_pdf", True):
            builder.add_output_file(
                "artifact_instructions_pdf",
                instructions_root / "artifact_instructions.pdf",
            )
        if requested.get("artifact_instructions_dicom", True):
            builder.add_optional_files(
                "artifact_instructions_dicom",
                _iter_metric_files(instructions_root, ".dcm"),
            )

        if requested.get("artifacts_tree", True) and metrics_artifacts_root.exists():
            for path in sorted(metrics_artifacts_root.rglob("*")):
                if not path.is_file():
                    continue
                builder.add_output_file("artifacts_tree", path)

        missing_outputs = sorted(set(builder.missing_outputs) - set(builder.delivered_outputs))
        delivered_outputs = {
            key: paths
            for key, paths in sorted(builder.delivered_outputs.items())
            if paths
        }
        contents = {
            "metadata_id_json": bool(id_json_path.exists()),
            "metadata_json": bool(metadata_json_path.exists()),
            "metrics_json": bool(results_path.exists()),
            "resultados_json": bool(results_path.exists()),
            "report_pdf": bool(report_path.exists()),
            "report_pdf_dicom": bool(report_dicom_path.exists()),
            "metrics_artifact_files": _artifact_file_count(metrics_artifacts_root),
            "delivered_file_count": len(builder.added_paths),
        }
        manifest_in_zip = {
            "event_type": "case.completed",
            "event_version": 1,
            "job_id": job_id,
            "case_id": case_id,
            "study_instance_uid": metadata.get("StudyInstanceUID"),
            "client_case_id": client_case_id,
            "source_system": source_system,
            "status": "done",
            "requested_outputs": requested,
            "delivered_outputs": delivered_outputs,
            "missing_outputs": missing_outputs,
            "contents": contents,
        }
        _write_manifest(zip_handle, manifest_in_zip)

    callback_manifest = {
        "event_type": "case.completed",
        "event_version": 1,
        "event_id": f"case.completed:{job_id}",
        "job_id": job_id,
        "case_id": case_id,
        "study_instance_uid": metadata.get("StudyInstanceUID"),
        "client_case_id": client_case_id,
        "source_system": source_system,
        "status": "done",
        "received_at": metadata.get("ExternalDelivery", {}).get("received_at"),
        "completed_at": metadata.get("Pipeline", {}).get("metrics_end_time")
        or metadata.get("Pipeline", {}).get("pipeline_end_time"),
        "package_name": package_name,
        "package_sha256": _zip_sha256(package_path),
        "package_size_bytes": package_path.stat().st_size,
        "contents": manifest_in_zip["contents"],
        "requested_outputs": requested,
        "delivered_outputs": manifest_in_zip["delivered_outputs"],
        "missing_outputs": manifest_in_zip["missing_outputs"],
    }
    return callback_manifest, package_path


def build_failed_delivery_manifest(
    *,
    job_id: str,
    case_id: str | None,
    study_uid: str | None,
    client_case_id: str | None,
    source_system: str | None,
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    failure = payload if isinstance(payload, dict) else {}
    failure_stage = str(failure.get("failure_stage", "unknown") or "unknown")
    error = str(failure.get("error", "") or "")[:2000]
    return {
        "event_type": "case.failed",
        "event_version": 1,
        "event_id": f"case.failed:{job_id}",
        "job_id": job_id,
        "case_id": str(case_id or "").strip() or None,
        "study_instance_uid": study_uid,
        "client_case_id": client_case_id,
        "source_system": source_system,
        "status": "failed",
        "failure_stage": failure_stage,
        "error": error,
        "received_at": failure.get("received_at"),
        "completed_at": failure.get("failed_at"),
        "package_name": None,
        "package_sha256": None,
        "package_size_bytes": 0,
        "contents": {},
        "requested_outputs": {},
        "delivered_outputs": {},
        "missing_outputs": [],
    }
