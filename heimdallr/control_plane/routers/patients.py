"""Patient and case management routes for the control plane."""

from __future__ import annotations

import io
import json
import os
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, Response

from ...shared.dependencies import get_db
from ...shared.paths import study_artifacts_dir, study_derived_dir, study_dir, study_id_json, study_results_json
from ...shared.schemas import BiometricData, PatientListResponse, SMIData
from ..case_pdf_report import build_case_report
from ..patient_service import PatientService

router = APIRouter(prefix="/api/patients", tags=["patients"])


@router.get("", response_model=PatientListResponse)
async def list_patients(db=Depends(get_db)):
    patients = PatientService.get_all_patients(db)
    return {"patients": patients}


@router.get("/{case_id}/nifti")
async def download_nifti(case_id: str):
    nii_path = study_derived_dir(case_id) / f"{case_id}.nii.gz"
    if not nii_path.exists():
        raise HTTPException(status_code=404, detail="NIfTI file not found")
    return FileResponse(path=nii_path, filename=f"{case_id}.nii.gz", media_type="application/gzip")


@router.get("/{case_id}/report.pdf")
async def download_case_report(case_id: str):
    case_folder = study_dir(case_id)
    results_path = study_results_json(case_id)
    id_json_path = study_id_json(case_id)

    if not case_folder.exists() or not results_path.exists() or not id_json_path.exists():
        raise HTTPException(status_code=404, detail="Case report data not found")

    try:
        pdf_path = build_case_report(case_folder)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error generating PDF report: {exc}") from exc

    return FileResponse(path=pdf_path, filename=f"{case_id}_report.pdf", media_type="application/pdf")


@router.get("/{case_id}/download/{folder_name}")
async def download_folder(case_id: str, folder_name: str):
    allowed_folders = ["bleed", "tissue_types", "total", "urology"]
    if folder_name not in allowed_folders:
        raise HTTPException(status_code=400, detail="Invalid folder name")

    folder_path = study_artifacts_dir(case_id) / folder_name
    if not folder_path.exists() or not folder_path.is_dir():
        raise HTTPException(status_code=404, detail=f"Folder {folder_name} not found")

    zip_buffer = io.BytesIO()
    import zipfile

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = Path(root) / file
                arcname = file_path.relative_to(folder_path)
                zip_file.write(file_path, arcname)

    zip_buffer.seek(0)
    return Response(
        content=zip_buffer.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={case_id}_{folder_name}.zip"},
    )


@router.get("/{case_id}/results")
async def get_results(case_id: str, db=Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("SELECT CalculationResults FROM dicom_metadata")
    for row in cursor.fetchall():
        try:
            _ = json.loads(row["CalculationResults"]) if row["CalculationResults"] else {}
        except Exception:
            continue

    case_folder = study_dir(case_id)
    results_path = study_results_json(case_id)

    if not results_path.exists():
        raise HTTPException(status_code=404, detail="Results not found")

    try:
        with open(results_path, "r") as f:
            results = json.load(f)

        images = []
        if case_folder.exists():
            for img in case_folder.rglob("*.png"):
                images.append(str(img.relative_to(case_folder)))
        results["images"] = sorted(images)

        triage_report_path = study_artifacts_dir(case_id) / "urology" / "kidney_stone_triage.json"
        if triage_report_path.exists():
            with open(triage_report_path, "r") as f:
                triage_report = json.load(f)
            results["kidney_stone_triage_report"] = triage_report

        return results
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error reading results: {exc}") from exc


@router.get("/{case_id}/images/{filename}")
async def get_result_image(case_id: str, filename: str):
    case_folder = study_dir(case_id).resolve()
    image_path = (case_folder / filename).resolve()
    if case_folder not in image_path.parents:
        raise HTTPException(status_code=400, detail="Invalid image path")
    if not image_path.exists():
        raise HTTPException(status_code=404, detail="Image not found")
    return FileResponse(image_path)


@router.get("/{case_id}/artifacts/{artifact_path:path}")
async def get_case_artifact(case_id: str, artifact_path: str):
    case_folder = study_dir(case_id)
    artifact = (case_folder / artifact_path).resolve()
    case_root = case_folder.resolve()

    if case_root not in artifact.parents and artifact != case_root:
        raise HTTPException(status_code=400, detail="Invalid artifact path")
    if not artifact.exists() or not artifact.is_file():
        raise HTTPException(status_code=404, detail="Artifact not found")
    return FileResponse(artifact)


@router.get("/{case_id}/metadata")
async def get_metadata(case_id: str, db=Depends(get_db)):
    meta = PatientService.get_patient_metadata(db, case_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Metadata not found")
    return meta


@router.patch("/{case_id}/biometrics")
async def update_biometrics(case_id: str, data: BiometricData, db=Depends(get_db)):
    meta = PatientService.update_biometrics(db, case_id, data.weight, data.height)
    if not meta:
        raise HTTPException(status_code=404, detail="Patient not found or could not update")

    id_json_path = study_id_json(case_id)
    if id_json_path.exists():
        with open(id_json_path, "w") as f:
            json.dump(meta, f, indent=2)

    bmi = data.weight / (data.height**2)
    return {
        "status": "success",
        "weight": data.weight,
        "height": data.height,
        "bmi": round(bmi, 2),
    }


@router.patch("/{case_id}/smi")
async def update_smi(case_id: str, data: SMIData, db=Depends(get_db)):
    results = PatientService.update_smi(db, case_id, data.smi)
    if not results:
        raise HTTPException(status_code=404, detail="Patient or results not found")

    results_json_path = study_results_json(case_id)
    if results_json_path.exists():
        with open(results_json_path, "w") as f:
            json.dump(results, f, indent=2)

    return {
        "status": "success",
        "smi": round(data.smi, 2),
        "saved_to": ["database", "resultados.json"],
    }
