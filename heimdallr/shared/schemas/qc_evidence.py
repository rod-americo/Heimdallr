"""Version 1 response schemas for study QC evidence."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class QcAnalysisHeader(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    analysis_id: str
    analysis_version: int
    study_instance_uid: str
    fingerprint: str
    policy_signature: str
    status: str
    qc_resolution: dict[str, Any]
    pipeline_version: str | None = None
    model_versions: dict[str, Any]
    artifacts_purged: bool
    created_at: str
    completed_at: str | None = None
    error: str | None = None


class QcAnalysesResponse(BaseModel):
    schema_version: Literal[1]
    analyses: list[QcAnalysisHeader]


class QcAnalysisResponse(QcAnalysisHeader):
    coverage: dict[str, Any]
    acquisitions: list[dict[str, Any]]
    series: list[dict[str, Any]]


class QcSeriesListResponse(QcAnalysisHeader):
    series: list[dict[str, Any]]


class QcSeriesResponse(QcAnalysisHeader):
    series: dict[str, Any]
    acquisition: dict[str, Any] | None = None


class QcCoverageResponse(QcAnalysisHeader):
    coverage: dict[str, Any]
