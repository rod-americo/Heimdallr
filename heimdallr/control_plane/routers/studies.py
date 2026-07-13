"""Versioned read-only study evidence API."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from ...shared import store
from ...shared.dependencies import get_db
from ...shared.schemas import (
    QcAnalysesResponse,
    QcAnalysisResponse,
    QcCoverageResponse,
    QcSeriesListResponse,
    QcSeriesResponse,
)


router = APIRouter(prefix="/api/v1/studies", tags=["study-evidence"])


def _json(value: Any, default: Any) -> Any:
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        return default
    return parsed if parsed is not None else default


def _analysis_header(row) -> dict[str, Any]:
    return {
        "schema_version": int(row["schema_version"]),
        "analysis_id": row["analysis_id"],
        "analysis_version": int(row["analysis_version"]),
        "study_instance_uid": row["study_uid"],
        "fingerprint": row["fingerprint"],
        "policy_signature": row["policy_signature"],
        "status": row["status"],
        "qc_resolution": _json(row["qc_resolution_json"], {}),
        "pipeline_version": row["pipeline_version"],
        "model_versions": _json(row["model_versions_json"], {}),
        "artifacts_purged": bool(row["artifacts_purged"]),
        "created_at": row["created_at"],
        "completed_at": row["completed_at"],
        "error": row["error"],
    }


def _analysis_or_404(db, study_uid: str, analysis_id: str | None):
    row = store.get_qc_analysis(db, study_uid, analysis_id)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "qc_analysis_not_available",
                "study_instance_uid": study_uid,
                "analysis_id": analysis_id,
            },
        )
    return row


def _series_payload(row) -> dict[str, Any]:
    payload = _json(row["payload_json"], {})
    payload.pop("derived_nifti_path", None)
    payload["segmentation_status"] = row["segmentation_status"]
    return payload


def _acquisition_payload(row, anatomy: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    payload = _json(row["payload_json"], {})
    payload["segmentation_status"] = row["segmentation_status"]
    payload["error"] = row["error"]
    if anatomy is not None:
        payload["anatomies"] = anatomy
    return payload


def _anatomy_by_acquisition(db, analysis_id: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in store.list_qc_anatomy(db, analysis_id):
        payload = _json(row["payload_json"], {})
        payload.pop("mask_path", None)
        payload["state"] = row["state"]
        grouped.setdefault(str(row["acquisition_id"]), []).append(payload)
    return grouped


@router.get("/{study_uid}/analyses", response_model=QcAnalysesResponse)
async def list_analyses(
    study_uid: str,
    analysis_id: str | None = Query(None),
    db=Depends(get_db),
):
    rows = (
        [_analysis_or_404(db, study_uid, analysis_id)]
        if analysis_id
        else store.list_qc_analyses(db, study_uid)
    )
    if not rows:
        _analysis_or_404(db, study_uid, None)
    return {"schema_version": 1, "analyses": [_analysis_header(row) for row in rows]}


@router.get("/{study_uid}/analysis", response_model=QcAnalysisResponse)
async def get_analysis(
    study_uid: str,
    analysis_id: str | None = Query(None),
    db=Depends(get_db),
):
    analysis = _analysis_or_404(db, study_uid, analysis_id)
    anatomy = _anatomy_by_acquisition(db, analysis["analysis_id"])
    payload = _analysis_header(analysis)
    payload["coverage"] = _json(analysis["coverage_json"], {})
    payload["acquisitions"] = [
        _acquisition_payload(row, anatomy.get(str(row["acquisition_id"]), []))
        for row in store.list_qc_acquisitions(db, analysis["analysis_id"])
    ]
    payload["series"] = [
        _series_payload(row)
        for row in store.list_qc_series(db, analysis["analysis_id"])
    ]
    return payload


@router.get("/{study_uid}/series", response_model=QcSeriesListResponse)
async def list_series(
    study_uid: str,
    analysis_id: str | None = Query(None),
    db=Depends(get_db),
):
    analysis = _analysis_or_404(db, study_uid, analysis_id)
    return {
        **_analysis_header(analysis),
        "series": [
            _series_payload(row)
            for row in store.list_qc_series(db, analysis["analysis_id"])
        ],
    }


@router.get("/{study_uid}/series/{series_uid}", response_model=QcSeriesResponse)
async def get_series(
    study_uid: str,
    series_uid: str,
    analysis_id: str | None = Query(None),
    db=Depends(get_db),
):
    analysis = _analysis_or_404(db, study_uid, analysis_id)
    row = store.get_qc_series(db, analysis["analysis_id"], series_uid)
    if row is None:
        raise HTTPException(status_code=404, detail={"code": "qc_series_not_found"})
    payload = _series_payload(row)
    acquisition = None
    anatomy = _anatomy_by_acquisition(db, analysis["analysis_id"])
    if row["acquisition_id"]:
        acquisition = next(
            (
                _acquisition_payload(
                    item,
                    anatomy.get(str(item["acquisition_id"]), []),
                )
                for item in store.list_qc_acquisitions(db, analysis["analysis_id"])
                if item["acquisition_id"] == row["acquisition_id"]
            ),
            None,
        )
    payload["anatomies"] = (
        acquisition.get("anatomies", [])
        if acquisition and acquisition.get("representative_series_uid") == series_uid
        else []
    )
    return {**_analysis_header(analysis), "series": payload, "acquisition": acquisition}


@router.get("/{study_uid}/coverage", response_model=QcCoverageResponse)
async def get_coverage(
    study_uid: str,
    analysis_id: str | None = Query(None),
    db=Depends(get_db),
):
    analysis = _analysis_or_404(db, study_uid, analysis_id)
    return {
        **_analysis_header(analysis),
        "coverage": _json(analysis["coverage_json"], {}),
    }
