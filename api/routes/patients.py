from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import FileResponse, Response
from typing import List
import sqlite3
from pathlib import Path
import os
import json

from core.database import get_db
import config
from services.patient_service import PatientService
from api.schemas.patient import PatientListResponse, BiometricData, SMIData

router = APIRouter(prefix="/api/patients", tags=["patients"])

@router.get("", response_model=PatientListResponse)
async def list_patients(db: sqlite3.Connection = Depends(get_db)):
    patients = PatientService.get_all_patients(db)
    return {"patients": patients}

@router.get("/{case_id}/nifti")
async def download_nifti(case_id: str):
    nii_path = config.NII_DIR / f"{case_id}.nii.gz"
    if not nii_path.exists():
        raise HTTPException(status_code=404, detail="NIfTI file not found")
    return FileResponse(path=nii_path, filename=f"{case_id}.nii.gz", media_type="application/gzip")

@router.get("/{case_id}/download/{folder_name}")
async def download_folder(case_id: str, folder_name: str):
    allowed_folders = ["bleed", "tissue_types", "total"]
    if folder_name not in allowed_folders:
        raise HTTPException(status_code=400, detail="Invalid folder name")
    
    folder_path = config.OUTPUT_DIR / case_id / folder_name
    if not folder_path.exists() or not folder_path.is_dir():
        raise HTTPException(status_code=404, detail=f"Folder {folder_name} not found")

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

@router.get("/{case_id}/results")
async def get_results(case_id: str, db: sqlite3.Connection = Depends(get_db)):
    # Results come from output folder or DB. Using DB as main truth for metrics.
    cursor = db.cursor()
    cursor.execute("SELECT CalculationResults FROM dicom_metadata")
    results = None
    for row in cursor.fetchall():
        try:
            res = json.loads(row["CalculationResults"]) if row["CalculationResults"] else {}
            # Quick check if it belongs to case. Since CalculationResults has very limited ID info
            # we check if output folder exists or query via IdJson CaseID
        except:
            continue
    
    # Actually, fetching from file is simpler for images. 
    # Let's read from disk, but we could migrate fully to DB later.
    case_folder = config.OUTPUT_DIR / case_id
    results_path = case_folder / "resultados.json"
    
    if not results_path.exists():
        raise HTTPException(status_code=404, detail="Results not found")
    
    try:
        with open(results_path, 'r') as f:
            results = json.load(f)
            
        images = []
        if case_folder.exists():
            for img in case_folder.glob("*.png"):
                images.append(img.name)
        results["images"] = sorted(images)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading results: {str(e)}")

@router.get("/{case_id}/images/{filename}")
async def get_result_image(case_id: str, filename: str):
    image_path = config.OUTPUT_DIR / case_id / filename
    if not image_path.exists():
        raise HTTPException(status_code=404, detail="Image not found")
    return FileResponse(image_path)

@router.get("/{case_id}/metadata")
async def get_metadata(case_id: str, db: sqlite3.Connection = Depends(get_db)):
    meta = PatientService.get_patient_metadata(db, case_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Metadata not found")
    return meta

@router.patch("/{case_id}/biometrics")
async def update_biometrics(case_id: str, data: BiometricData, db: sqlite3.Connection = Depends(get_db)):
    meta = PatientService.update_biometrics(db, case_id, data.weight, data.height)
    if not meta:
        raise HTTPException(status_code=404, detail="Patient not found or could not update")
    
    # Also write to file for backward compatibility during transition
    case_folder = config.OUTPUT_DIR / case_id
    id_json_path = case_folder / "id.json"
    if id_json_path.exists():
        with open(id_json_path, 'w') as f:
            json.dump(meta, f, indent=2)

    bmi = data.weight / (data.height ** 2)
    return {
        "status": "success",
        "weight": data.weight,
        "height": data.height,
        "bmi": round(bmi, 2)
    }

@router.patch("/{case_id}/smi")
async def update_smi(case_id: str, data: SMIData, db: sqlite3.Connection = Depends(get_db)):
    results = PatientService.update_smi(db, case_id, data.smi)
    if not results:
        raise HTTPException(status_code=404, detail="Patient or results not found")
        
    # Backward compatibility
    case_folder = config.OUTPUT_DIR / case_id
    results_json_path = case_folder / "resultados.json"
    if results_json_path.exists():
        with open(results_json_path, 'w') as f:
            json.dump(results, f, indent=2)

    return {
        "status": "success",
        "smi": round(data.smi, 2),
        "saved_to": ["database", "resultados.json"]
    }
