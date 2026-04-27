"""Upload intake route for the control plane."""

from __future__ import annotations

import json

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from ...shared import settings
from ...shared.external_delivery import (
    build_external_submission_payload,
    new_external_job_id,
    normalize_requested_metrics_modules,
    write_external_submission_sidecar,
)
from ...shared.spool import atomic_copy_stream

router = APIRouter(tags=["upload"])


@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only .zip files are allowed.")

    upload_name = f"study_{settings.local_timestamp('%Y%m%d%H%M%S')}.zip"
    file_path = settings.UPLOAD_EXTERNAL_DIR / upload_name
    if file_path.exists():
        upload_name = f"study_{settings.local_timestamp('%Y%m%d%H%M%S_%f')}.zip"
        file_path = settings.UPLOAD_EXTERNAL_DIR / upload_name

    try:
        atomic_copy_stream(file_path, file.file)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {exc}") from exc

    return {
        "status": "Accepted",
        "message": "File upload accepted. Study queued for prepare watchdog.",
        "original_file": file.filename,
        "stored_file": upload_name,
    }


@router.post("/jobs")
async def submit_job(
    study_file: UploadFile = File(...),
    client_case_id: str = Form(...),
    callback_url: str = Form(...),
    source_system: str | None = Form(None),
    requested_outputs: str | None = Form(None),
    requested_metrics_modules: str | None = Form(None),
):
    if not study_file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only .zip files are allowed.")

    normalized_client_case_id = str(client_case_id or "").strip()
    normalized_callback_url = str(callback_url or "").strip()
    if not normalized_client_case_id:
        raise HTTPException(status_code=400, detail="client_case_id is required.")
    if not normalized_callback_url:
        raise HTTPException(status_code=400, detail="callback_url is required.")

    parsed_requested_outputs = None
    if requested_outputs:
        try:
            parsed_requested_outputs = json.loads(requested_outputs)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="requested_outputs must be valid JSON.") from exc
        if not isinstance(parsed_requested_outputs, dict):
            raise HTTPException(status_code=400, detail="requested_outputs must be a JSON object.")

    parsed_requested_metrics_modules: list[str] | None = None
    if requested_metrics_modules:
        try:
            candidate = json.loads(requested_metrics_modules)
        except json.JSONDecodeError:
            candidate = requested_metrics_modules
        try:
            parsed_requested_metrics_modules = normalize_requested_metrics_modules(candidate)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail="requested_metrics_modules must be a JSON array or CSV string.",
            ) from exc

    upload_name = f"study_{settings.local_timestamp('%Y%m%d%H%M%S')}.zip"
    file_path = settings.UPLOAD_EXTERNAL_DIR / upload_name
    if file_path.exists():
        upload_name = f"study_{settings.local_timestamp('%Y%m%d%H%M%S_%f')}.zip"
        file_path = settings.UPLOAD_EXTERNAL_DIR / upload_name

    job_id = new_external_job_id()
    submission_payload = build_external_submission_payload(
        job_id=job_id,
        client_case_id=normalized_client_case_id,
        callback_url=normalized_callback_url,
        source_system=source_system,
        requested_outputs=parsed_requested_outputs,
        requested_metrics_modules=parsed_requested_metrics_modules,
    )

    try:
        atomic_copy_stream(file_path, study_file.file)
        write_external_submission_sidecar(file_path, submission_payload)
    except Exception as exc:
        if file_path.exists():
            file_path.unlink()
        raise HTTPException(status_code=500, detail=f"Failed to save job payload: {exc}") from exc

    return {
        "accepted": True,
        "job_id": job_id,
        "client_case_id": normalized_client_case_id,
        "status": "queued",
        "received_at": submission_payload["received_at"],
        "stored_file": upload_name,
        "requested_metrics_modules": submission_payload["requested_metrics_modules"],
    }
