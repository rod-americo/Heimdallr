# Copyright (c) 2026 Rodrigo Americo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Heimdallr Unified FastAPI Server
# Combines upload ingestion with web dashboard and RESTful API
# Port: 8001

import os
import shutil
import subprocess
import json
from pathlib import Path
from fastapi import FastAPI, File, UploadFile, HTTPException, Response, Form
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from pydantic import BaseModel, Field
import sqlite3
import httpx  # For microservice communication

# Import centralized configuration
import config

# Initialize FastAPI application
app = FastAPI(title=config.SERVER_TITLE)

# Directory paths from config
BASE_DIR = config.BASE_DIR
UPLOAD_DIR = config.UPLOAD_DIR
NII_DIR = config.NII_DIR
OUTPUT_DIR = config.OUTPUT_DIR
STATIC_DIR = config.STATIC_DIR

# Ensure required directories exist
config.ensure_directories()

# Scripts and executables
PREPARE_SCRIPT = config.PREPARE_SCRIPT
PYTHON_EXE = BASE_DIR / "venv" / "bin" / "python"  # Virtual environment Python

# ============================================================
# API ENDPOINTS - Patient Data and Results
# ============================================================

@app.get("/api/patients")
async def list_patients():
    """
    List all patients with NIfTI files available for download.
    
    Returns:
        JSON with patient list including:
        - case_id: Clinical naming format (FirstNameInitials_YYYYMMDD_AccessionNumber)
        - file_size: NIfTI file size in MB
        - patient_name: Full patient name from DICOM
        - study_date: Study date (YYYYMMDD)
        - accession: Accession number
        - modality: CT or MR
        - elapsed_seconds: Processing time in seconds
        - has_results: Whether results JSON exists
        - body_regions: List of detected body regions
        - has_hemorrhage: Whether hemorrhage was detected (>0.1 cm³)
    """
    patients = []
    
    # Return empty list if NIfTI directory doesn't exist yet
    if not NII_DIR.exists():
        return {"patients": []}
    
    # Scan all NIfTI files in the archive directory
    nii_files = list(NII_DIR.glob("*.nii.gz"))
    
    # Process each NIfTI file and extract metadata
    for nii_path in nii_files:
        filename = nii_path.name
        
        # Skip files that don't follow the clinical naming convention
        # Expected format: FirstNameInitials_YYYYMMDD_AccessionNumber.nii.gz
        parts = filename.replace(".nii.gz", "").split("_")
        if len(parts) < 3:
            continue
            
        case_id = filename.replace(".nii.gz", "")
        file_size = nii_path.stat().st_size
        
        # Attempt to load metadata and results from output folder
        metadata = {}
        results = {}
        output_folder = OUTPUT_DIR / case_id
        
        # Load patient metadata (id.json) and analysis results (resultados.json)
        if output_folder.exists():
            # Patient and study metadata
            id_json = output_folder / "id.json"
            if id_json.exists():
                try:
                    with open(id_json, 'r') as f:
                        metadata = json.load(f)
                except:
                    pass  # Silently skip if JSON is malformed
            
            # Analysis results (metrics, volumes, etc.)
            results_json = output_folder / "resultados.json"
            if results_json.exists():
                try:
                    with open(results_json, 'r') as f:
                        results = json.load(f)
                except:
                    pass  # Silently skip if JSON is malformed
        
        # Parse processing time from metadata
        # Format: "H:MM:SS.microseconds" e.g., "0:02:26.517938"
        pipeline = metadata.get("Pipeline", {})
        elapsed_str = pipeline.get("elapsed_time", "")
        elapsed_seconds = 0
        
        if elapsed_str and ":" in elapsed_str:
            try:
                h, m, s = elapsed_str.split(':')
                seconds = int(h) * 3600 + int(m) * 60 + float(s)
                elapsed_seconds = int(seconds)
            except:
                pass  # If parsing fails, default to 0

        # Build patient record
        patients.append({
            "case_id": case_id,
            "filename": filename,
            "file_size_bytes": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2),
            "patient_name": metadata.get("PatientName", case_id.split("_")[0]),
            "study_date": metadata.get("StudyDate", parts[1] if len(parts) > 1 else ""),
            "accession": metadata.get("AccessionNumber", parts[2] if len(parts) > 2 else ""),
            "modality": metadata.get("Modality", ""),
            "elapsed_seconds": elapsed_seconds,
            "has_results": bool(results),
            "body_regions": results.get("body_regions", []),
            # Flag hemorrhage if volume > 0.1 cm³ (threshold to filter noise)
            "has_hemorrhage": results.get("hemorrhage_vol_cm3", 0.0) > 0.1
        })
    
    # Sort patients by study date (newest first)
    patients.sort(key=lambda x: x.get("study_date", ""), reverse=True)
    
    return {"patients": patients}


@app.get("/api/patients/{case_id}/nifti")
async def download_nifti(case_id: str):
    """
    Download the NIfTI file for a specific patient.
    
    Args:
        case_id: Patient identifier (format: FirstNameInitials_YYYYMMDD_AccessionNumber)
    
    Returns:
        FileResponse with the .nii.gz file
    """
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
    """
    Download a specific result folder as a ZIP file.
    Allows downloading segmentation masks and hemorrhage results.
    
    Args:
        case_id: Patient identifier
        folder_name: One of ['bleed', 'tissue_types', 'total']
    
    Returns:
        ZIP file containing all files from the requested folder
    """
    # Security: only allow specific result folders
    allowed_folders = ["bleed", "tissue_types", "total"]
    if folder_name not in allowed_folders:
        raise HTTPException(status_code=400, detail="Invalid folder name")
    
    folder_path = OUTPUT_DIR / case_id / folder_name
    if not folder_path.exists() or not folder_path.is_dir():
        raise HTTPException(status_code=404, detail=f"Folder {folder_name} not found")

    # Create a zip file in memory (no disk I/O)
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
    """
    Get the analysis results (metrics) for a patient.
    
    Returns JSON with:
    - Organ volumes (liver, spleen, kidneys)
    - Hounsfield Unit densities (CT only)
    - Sarcopenia metrics (L3 SMA, muscle HU)
    - Hemorrhage quantification (if detected)
    - Available overlay images
    """
    case_folder = OUTPUT_DIR / case_id
    results_path = case_folder / "resultados.json"
    
    if not results_path.exists():
        raise HTTPException(status_code=404, detail="Results not found")
    
    try:
        with open(results_path, 'r') as f:
            results = json.load(f)
            
        # Enumerate available overlay images (L3_overlay.png, bleed_overlay_*.png)
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
    """
    Serve a result overlay image (e.g., L3_overlay.png, bleed_overlay_*.png).
    
    Args:
        case_id: Patient identifier
        filename: Image filename (PNG)
    
    Returns:
        FileResponse with the image
    """
    image_path = OUTPUT_DIR / case_id / filename
    if not image_path.exists():
        raise HTTPException(status_code=404, detail="Image not found")
    
    return FileResponse(image_path)


@app.get("/api/patients/{case_id}/metadata")
async def get_metadata(case_id: str):
    """
    Get patient and study metadata from DICOM.
    
    Returns JSON with:
    - PatientName, AccessionNumber, StudyInstanceUID
    - Modality, StudyDate
    - Pipeline processing times
    - Selected DICOM series information
    - Weight and Height (if available)
    """
    meta_path = OUTPUT_DIR / case_id / "id.json"
    
    if not meta_path.exists():
        raise HTTPException(status_code=404, detail="Metadata not found")
    
    try:
        with open(meta_path, 'r') as f:
            metadata = json.load(f)
        
        # Try to get weight and height from database
        try:
            db_path = config.DB_PATH
            study_uid = metadata.get("StudyInstanceUID")
            if study_uid and db_path.exists():
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT Weight, Height FROM dicom_metadata WHERE StudyInstanceUID = ?",
                    (study_uid,)
                )
                row = cursor.fetchone()
                conn.close()
                
                if row:
                    if row[0] is not None:
                        metadata["Weight"] = row[0]
                    if row[1] is not None:
                        metadata["Height"] = row[1]
        except Exception as db_err:
            # Don't fail if database lookup fails, just log and continue
            print(f"Warning: Could not fetch biometric data from database: {db_err}")
        
        return metadata
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading metadata: {str(e)}")


# Pydantic model for biometric data
class BiometricData(BaseModel):
    weight: float = Field(gt=0, le=500, description="Patient weight in kilograms")
    height: float = Field(gt=0, le=3.0, description="Patient height in meters")


@app.patch("/api/patients/{case_id}/biometrics")
async def update_biometrics(case_id: str, data: BiometricData):
    """
    Update patient biometric data (weight and height).
    
    Args:
        case_id: Patient identifier
        data: BiometricData with weight (kg) and height (m)
    
    Returns:
        Updated metadata including calculated BMI
    """
    case_folder = OUTPUT_DIR / case_id
    id_json_path = case_folder / "id.json"
    
    if not id_json_path.exists():
        raise HTTPException(status_code=404, detail="Patient not found")
    
    try:
        # Load existing metadata
        with open(id_json_path, 'r') as f:
            metadata = json.load(f)
        
        # Update biometric data
        metadata["Weight"] = data.weight
        metadata["Height"] = data.height
        
        # Save updated metadata to id.json
        with open(id_json_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        # Update database
        study_uid = metadata.get("StudyInstanceUID")
        if study_uid:
            db_path = config.DB_PATH
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Update both Weight/Height and IdJson in one query
            cursor.execute(
                "UPDATE dicom_metadata SET Weight = ?, Height = ?, IdJson = ? WHERE StudyInstanceUID = ?",
                (data.weight, data.height, json.dumps(metadata), study_uid)
            )
            conn.commit()
            conn.close()
        
        # Calculate BMI for response
        bmi = data.weight / (data.height ** 2)
        
        return {
            "status": "success",
            "weight": data.weight,
            "height": data.height,
            "bmi": round(bmi, 2)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating biometrics: {str(e)}")


# Pydantic model for SMI data
class SMIData(BaseModel):
    smi: float = Field(gt=0, le=200, description="Skeletal Muscle Index in cm²/m²")


@app.patch("/api/patients/{case_id}/smi")
async def update_smi(case_id: str, data: SMIData):
    """
    Update patient SMI (Skeletal Muscle Index) calculation.
    
    This endpoint is called after weight/height are saved and SMI is calculated
    in the frontend. It saves the SMI to both resultados.json and the database.
    
    Args:
        case_id: Patient identifier
        data: SMIData with calculated SMI value
    
    Returns:
        Confirmation of saved SMI
    """
    case_folder = OUTPUT_DIR / case_id
    results_json_path = case_folder / "resultados.json"
    id_json_path = case_folder / "id.json"
    
    if not results_json_path.exists():
        raise HTTPException(status_code=404, detail="Results file not found")
    
    try:
        # Load existing results
        with open(results_json_path, 'r') as f:
            results = json.load(f)
        
        # Add SMI to results
        results["SMI_cm2_m2"] = round(data.smi, 2)
        
        # Save updated results to resultados.json
        with open(results_json_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Update database
        if id_json_path.exists():
            with open(id_json_path, 'r') as f:
                metadata = json.load(f)
            
            study_uid = metadata.get("StudyInstanceUID")
            if study_uid:
                db_path = config.DB_PATH
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Update SMI and CalculationResults in database
                cursor.execute(
                    "UPDATE dicom_metadata SET SMI = ?, CalculationResults = ? WHERE StudyInstanceUID = ?",
                    (data.smi, json.dumps(results), study_uid)
                )
                conn.commit()
                conn.close()
        
        return {
            "status": "success",
            "smi": round(data.smi, 2),
            "saved_to": ["resultados.json", "database"]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating SMI: {str(e)}")


@app.get("/api/tools/uploader")
async def download_uploader():
    """
    Download the CLI uploader script for convenient exam submission.
    Allows users to download uploader.py directly from the web UI.
    """
    script_path = BASE_DIR / "uploader.py"
    return FileResponse(
        path=script_path,
        filename="uploader.py",
        media_type="text/x-python"
    )



# ============================================================
# MEDGEMMA MICROSERVICE (Proxy)
# ============================================================

@app.post("/api/medgemma/ap-thorax-xray")
async def analyze_xray(
    file: UploadFile = File(..., description="Image file"),
    age: str = Form("unknown age", description="Patient age (e.g. '45-year-old')")
):
    """
    Proxy request to the MedGemma Analysis Service.
    Supports Multipart Upload (file + age).
    """
    service_url = config.MEDGEMMA_SERVICE_URL
    
    try:
        # Read file content to forward
        file_content = await file.read()
        files = {'file': (file.filename, file_content, file.content_type)}
        data = {'age': age}
        
        async with httpx.AsyncClient(timeout=180.0) as client:
            response = await client.post(
                service_url, 
                files=files,
                data=data,
                timeout=180.0
            )
            
            # Check for errors from the microservice
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code, 
                    detail=f"MedGemma Service Error: {response.text}"
                )
                
            return response.json()
            
    except httpx.ConnectError:
        raise HTTPException(
            status_code=503, 
            detail="MedGemma Service is unavailable."
        )
    except httpx.ReadTimeout:
        raise HTTPException(
            status_code=504, 
            detail="Model inference timed out."
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Proxy Error: {str(e)}")


# ============================================================
# UPLOAD ENDPOINT - DICOM Ingestion
# ============================================================

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Upload a DICOM ZIP file for processing.
    
    Flow:
    1. Receive ZIP file from uploader.py client
    2. Save to uploads/ directory
    3. Trigger prepare.py asynchronously to:
       - Extract DICOM files
       - Select best series
       - Convert to NIfTI
       - Move to input/ queue
    4. run.py daemon picks up from input/ and processes
    
    Returns:
        JSON confirmation that processing has started
    """
    # Validate file type
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only .zip files are allowed.")

    file_path = UPLOAD_DIR / file.filename
    
    # Save uploaded file to disk
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")

    # Trigger prepare.py asynchronously (fire and forget)
    try:
        # Use virtual environment Python if available
        python_cmd = str(PYTHON_EXE) if PYTHON_EXE.exists() else "python3"
        
        # Launch prepare.py as detached background process
        # This prevents blocking the API response
        subprocess.Popen(
            [python_cmd, str(PREPARE_SCRIPT), str(file_path)],
            stdout=subprocess.DEVNULL,  # Suppress output
            stderr=subprocess.DEVNULL,  # Suppress errors
            close_fds=True,              # Close file descriptors
            start_new_session=True       # Detach from parent process
        )
        
        return {
            "status": "Accepted",
            "message": "File upload accepted. Processing started in background.",
            "original_file": file.filename
        }

    except Exception as e:
        # Cleanup: delete uploaded file if we failed to launch preparation
        if file_path.exists():
            file_path.unlink()
        raise HTTPException(status_code=500, detail=f"Error launching process: {str(e)}")


# ============================================================
# STATIC FILES & WEB DASHBOARD
# ============================================================

# Mount static assets (CSS, JS, images) if directory exists
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """
    Serve the web dashboard (main entry point).
    
    The dashboard provides:
    - Real-time patient list with auto-refresh
    - Quick search functionality
    - Interactive results viewer
    - Download capabilities for NIfTI files and segmentations
    """
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


# Entry point when running directly (python server.py)
if __name__ == "__main__":
    import uvicorn
    # Run server on all interfaces, port 8001
    # Access: http://localhost:8001 (dashboard) or http://localhost:8001/docs (API)
    uvicorn.run(app, host="0.0.0.0", port=8001)