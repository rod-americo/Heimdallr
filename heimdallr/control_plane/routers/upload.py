"""Upload intake route for the control plane."""

from __future__ import annotations

from fastapi import APIRouter, File, HTTPException, UploadFile

from ...shared import settings
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
