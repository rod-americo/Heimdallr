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
from heimdallr.shared.external_delivery import normalize_requested_outputs
from heimdallr.shared.paths import (
    study_artifacts_dir,
    study_dir,
    study_id_json,
    study_metadata_json,
    study_results_json,
)


_SAFE_NAME_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")


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
    metrics_artifacts_root = study_artifacts_dir(case_id) / "metrics"

    if requested.get("include_report_pdf", True):
        report_path = build_case_report(case_root)

    package_stem = _safe_package_stem(client_case_id or case_id)
    package_name = f"heimdallr_{package_stem}.zip"

    temp_dir = Path(tempfile.mkdtemp(prefix="heimdallr-delivery-"))
    package_path = temp_dir / package_name

    manifest_in_zip = {
        "event_type": "case.completed",
        "event_version": 1,
        "job_id": job_id,
        "case_id": case_id,
        "study_instance_uid": metadata.get("StudyInstanceUID"),
        "client_case_id": client_case_id,
        "source_system": source_system,
        "status": "done",
        "contents": {
            "metadata_id_json": bool(id_json_path.exists()),
            "metadata_json": bool(metadata_json_path.exists()),
            "resultados_json": bool(results_path.exists()),
            "report_pdf": bool(report_path.exists()),
            "metrics_artifact_files": _artifact_file_count(metrics_artifacts_root),
        },
    }

    with zipfile.ZipFile(package_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_handle:
        zip_handle.writestr("manifest.json", json.dumps(manifest_in_zip, indent=2, ensure_ascii=False))
        _add_file(zip_handle, id_json_path, "metadata/id.json")

        if requested.get("include_metadata_json", True) and metadata_json_path.exists():
            _add_file(zip_handle, metadata_json_path, "metadata/metadata.json")
        if requested.get("include_resultados_json", True) and results_path.exists():
            _add_file(zip_handle, results_path, "metadata/resultados.json")
        if requested.get("include_report_pdf", True) and report_path.exists():
            _add_file(zip_handle, report_path, "metadata/report.pdf")

        if requested.get("include_artifacts_tree", True) and metrics_artifacts_root.exists():
            for path in sorted(metrics_artifacts_root.rglob("*")):
                if not path.is_file():
                    continue
                _add_file(zip_handle, path, str(path.relative_to(case_root)))

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
    }
    return callback_manifest, package_path
