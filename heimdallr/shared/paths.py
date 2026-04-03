"""Path helpers for Heimdallr runtime storage."""

from __future__ import annotations

from pathlib import Path

from . import settings


def study_dir(case_id: str) -> Path:
    return settings.STUDIES_DIR / case_id


def study_metadata_dir(case_id: str) -> Path:
    return study_dir(case_id) / "metadata"


def study_artifacts_dir(case_id: str) -> Path:
    return study_dir(case_id) / "artifacts"


def study_logs_dir(case_id: str) -> Path:
    return study_dir(case_id) / "logs"


def study_source_dir(case_id: str) -> Path:
    return study_dir(case_id) / "source"


def study_derived_dir(case_id: str) -> Path:
    return study_dir(case_id) / "derived"


def study_results_json(case_id: str) -> Path:
    return study_metadata_dir(case_id) / "resultados.json"


def study_id_json(case_id: str) -> Path:
    return study_metadata_dir(case_id) / "id.json"


def study_metadata_json(case_id: str) -> Path:
    return study_metadata_dir(case_id) / "metadata.json"


def study_nifti(case_id: str) -> Path:
    return study_derived_dir(case_id) / f"{case_id}.nii.gz"
