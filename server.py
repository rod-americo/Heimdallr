import os
import shutil
import subprocess
import json
from pathlib import Path
from fastapi import FastAPI, File, UploadFile, HTTPException, Response
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Heimdallr - Radiology AI Pipeline")

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
NII_DIR = BASE_DIR / "nii"
OUTPUT_DIR = BASE_DIR / "output"
STATIC_DIR = BASE_DIR / "static"

UPLOAD_DIR.mkdir(exist_ok=True)
PREPARE_SCRIPT = BASE_DIR / "prepare.py"
PYTHON_EXE = BASE_DIR / "venv" / "bin" / "python"

# ============================================================
# API ENDPOINTS
# ============================================================

@app.get("/api/patients")
async def list_patients():
    """List all patients with NIfTI files available for download."""
    patients = []
    
    if not NII_DIR.exists():
        return {"patients": []}
    
    # Get all .nii.gz files
    nii_files = list(NII_DIR.glob("*.nii.gz"))
    
    for nii_path in nii_files:
        filename = nii_path.name
        # Skip files without full clinical name pattern (Name_Date_Accession)
        parts = filename.replace(".nii.gz", "").split("_")
        if len(parts) < 3:
            continue
            
        case_id = filename.replace(".nii.gz", "")
        file_size = nii_path.stat().st_size
        
        # Try to get metadata from output folder
        metadata = {}
        results = {}
        output_folder = OUTPUT_DIR / case_id
        
        if output_folder.exists():
            id_json = output_folder / "id.json"
            if id_json.exists():
                try:
                    with open(id_json, 'r') as f:
                        metadata = json.load(f)
                except:
                    pass
            
            results_json = output_folder / "resultados.json"
            if results_json.exists():
                try:
                    with open(results_json, 'r') as f:
                        results = json.load(f)
                except:
                    pass
        
        patients.append({
            "case_id": case_id,
            "filename": filename,
            "file_size_bytes": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2),
            "patient_name": metadata.get("PatientName", case_id.split("_")[0]),
            "study_date": metadata.get("StudyDate", parts[1] if len(parts) > 1 else ""),
            "accession": metadata.get("AccessionNumber", parts[2] if len(parts) > 2 else ""),
            "modality": metadata.get("Modality", ""),
            "has_results": bool(results),
            "body_regions": results.get("body_regions", []),
            "has_hemorrhage": results.get("hemorrhage_vol_cm3", 0.0) > 0
        })
    
    # Sort by date descending
    patients.sort(key=lambda x: x.get("study_date", ""), reverse=True)
    
    return {"patients": patients}


@app.get("/api/patients/{case_id}/nifti")
async def download_nifti(case_id: str):
    """Download the NIfTI file for a patient."""
    nii_path = NII_DIR / f"{case_id}.nii.gz"
    
    if not nii_path.exists():
        raise HTTPException(status_code=404, detail="NIfTI file not found")
    
    return FileResponse(
        path=nii_path,
        filename=f"{case_id}.nii.gz",
        media_type="application/gzip"
    )
@app.get("/api/patients/{case_id}/download/{folder_name}")
async def download_folder(case_id: str, folder_name: str):
    """Download a specific result folder as a ZIP file."""
    allowed_folders = ["bleed", "tissue_types", "total"]
    if folder_name not in allowed_folders:
        raise HTTPException(status_code=400, detail="Invalid folder name")
    
    folder_path = OUTPUT_DIR / case_id / folder_name
    if not folder_path.exists() or not folder_path.is_dir():
        raise HTTPException(status_code=404, detail=f"Folder {folder_name} not found")

    # Create a zip file in memory
    import io
    import zipfile
    
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = Path(root) / file
                arcname = file_path.relative_to(folder_path)
                zip_file.write(file_path, arcname)
    
    zip_buffer.seek(0)
    
    return Response(
        content=zip_buffer.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={case_id}_{folder_name}.zip"}
    )


@app.get("/api/patients/{case_id}/results")
async def get_results(case_id: str):
    """Get the analysis results for a patient."""
    case_folder = OUTPUT_DIR / case_id
    results_path = case_folder / "resultados.json"
    
    if not results_path.exists():
        raise HTTPException(status_code=404, detail="Results not found")
    
    try:
        with open(results_path, 'r') as f:
            results = json.load(f)
            
        # Add available images
        images = []
        if case_folder.exists():
            for img in case_folder.glob("*.png"):
                images.append(img.name)
        results["images"] = sorted(images)
            
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading results: {str(e)}")


@app.get("/api/patients/{case_id}/images/{filename}")
async def get_result_image(case_id: str, filename: str):
    """Serve a result image (overlay)."""
    image_path = OUTPUT_DIR / case_id / filename
    if not image_path.exists():
        raise HTTPException(status_code=404, detail="Image not found")
    
    return FileResponse(image_path)


@app.get("/api/patients/{case_id}/metadata")
async def get_metadata(case_id: str):
    """Get the patient/study metadata."""
    meta_path = OUTPUT_DIR / case_id / "id.json"
    
    if not meta_path.exists():
        raise HTTPException(status_code=404, detail="Metadata not found")
    
    try:
        with open(meta_path, 'r') as f:
            metadata = json.load(f)
        return metadata
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading metadata: {str(e)}")


@app.get("/api/tools/uploader")
async def download_uploader():
    """Download the CLI uploader script."""
    script_path = BASE_DIR / "uploader.py"
    return FileResponse(
        path=script_path,
        filename="uploader.py",
        media_type="text/x-python"
    )

# ============================================================
# UPLOAD ENDPOINT (existing)
# ============================================================

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


# ============================================================
# STATIC FILES & DASHBOARD
# ============================================================

# Mount static files if directory exists
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the dashboard HTML."""
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        with open(index_path, 'r') as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(content="""
        <html>
            <head><title>Heimdallr</title></head>
            <body style="font-family: sans-serif; padding: 40px; background: #1a1a2e; color: #eee;">
                <h1>🔭 Heimdallr</h1>
                <p>Dashboard not found. Please create <code>static/index.html</code></p>
                <p><a href="/docs" style="color: #4cc9f0;">API Documentation →</a></p>
            </body>
        </html>
        """)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
