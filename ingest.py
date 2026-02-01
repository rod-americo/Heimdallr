import os
import shutil
import subprocess
from pathlib import Path
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI(title="Sarcopenia Ingestion Service")

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)
PREPARE_SCRIPT = BASE_DIR / "prepare.py"
PYTHON_EXE = BASE_DIR / "venv" / "bin" / "python"

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only .zip files are allowed.")

    file_path = UPLOAD_DIR / file.filename
    
    # Save uploaded file
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")

    # Call prepare.py in background
    try:
        python_cmd = str(PYTHON_EXE) if PYTHON_EXE.exists() else "python3"
        
        # Fire and forget - using Popen without waiting
        subprocess.Popen(
            [python_cmd, str(PREPARE_SCRIPT), str(file_path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True # Detach from parent
        )
        
        return {
            "status": "Accepted",
            "message": "File upload accepted. Processing started in background.",
            "original_file": file.filename
        }

    except Exception as e:
        # Only cleanup if we failed to even launch
        if file_path.exists():
            file_path.unlink()
        raise HTTPException(status_code=500, detail=f"Error launching process: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
