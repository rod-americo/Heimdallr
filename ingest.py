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

    # Call prepare.py
    try:
        # We use current env python if venv python not found, but prefer venv
        python_cmd = str(PYTHON_EXE) if PYTHON_EXE.exists() else "python3"
        
        result = subprocess.run(
            [python_cmd, str(PREPARE_SCRIPT), str(file_path)],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Parse output to find the generated file path (last printed line)
        lines = result.stdout.strip().splitlines()
        generated_file = lines[-1] if lines else "Unknown"
        
        return {
            "message": "File processed successfully",
            "original_file": file.filename,
            "generated_nifti": os.path.basename(generated_file),
            "full_path": generated_file
        }

    except subprocess.CalledProcessError as e:
        # Cleanup uploaded file on failure
        if file_path.exists():
            file_path.unlink()
        raise HTTPException(status_code=500, detail=f"Processing failed: {e.stderr}")
    finally:
        # Cleanup uploaded zip after processing (optional, keeping for now or removing?)
        # User didn't specify, but typical for ingest to clean up source if processed.
        # Let's keep it for debug or remove? The plan said "Save to uploads/" but logic implies transient.
        # I'll enable cleanup of the zip to save space.
        if file_path.exists():
            file_path.unlink()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="thor", port=8001)
