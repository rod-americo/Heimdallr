from fastapi import APIRouter, File, UploadFile, HTTPException
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
import config

router = APIRouter(tags=["upload"])

@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only .zip files are allowed.")

    upload_name = f"study_{datetime.now().strftime('%Y%m%d%H%M%S')}.zip"
    file_path = config.UPLOAD_DIR / upload_name
    if file_path.exists():
        upload_name = f"study_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}.zip"
    file_path = config.UPLOAD_DIR / upload_name
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")

    try:
        # Use Python executable based on config
        python_cmd = str(config.BASE_DIR / "venv" / "bin" / "python")
        subprocess.Popen(
            [python_cmd, str(config.PREPARE_SCRIPT), str(file_path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True
        )
        return {
            "status": "Accepted",
            "message": "File upload accepted. Processing started in background.",
            "original_file": file.filename,
            "stored_file": upload_name
        }
    except Exception as e:
        if file_path.exists():
            file_path.unlink()
        raise HTTPException(status_code=500, detail=f"Error launching process: {str(e)}")
