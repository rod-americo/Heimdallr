"""Proxy routes for assistant services."""

from __future__ import annotations

import httpx
from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from ...shared import settings

router = APIRouter(prefix="/api", tags=["proxy"])


@router.post("/medgemma/ap-thorax-xray")
async def analyze_xray(
    file: UploadFile = File(..., description="Image file"),
    age: str = Form("unknown age", description="Patient age (e.g. '45-year-old')"),
):
    service_url = settings.MEDGEMMA_SERVICE_URL
    try:
        file_content = await file.read()
        files = {"file": (file.filename, file_content, file.content_type)}
        data = {"age": age}

        async with httpx.AsyncClient(timeout=180.0) as client:
            response = await client.post(service_url, files=files, data=data, timeout=180.0)
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"MedGemma Service Error: {response.text}",
                )
            return response.json()
    except httpx.ConnectError as exc:
        raise HTTPException(status_code=503, detail="MedGemma Service is unavailable.") from exc
    except httpx.ReadTimeout as exc:
        raise HTTPException(status_code=504, detail="Model inference timed out.") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Proxy Error: {exc}") from exc


@router.post("/anthropic/ap-thorax-xray")
async def analyze_xray_anthropic(
    file: UploadFile = File(..., description="Image file (DICOM, JPG, PNG)"),
    age: str = Form("unknown", description="Patient age"),
    identificador: str = Form(..., description="Patient Identifier"),
):
    service_url = settings.ANTHROPIC_SERVICE_URL
    try:
        file_content = await file.read()
        files = {"file": (file.filename, file_content, file.content_type)}
        data = {"age": age, "identificador": identificador}

        async with httpx.AsyncClient(timeout=180.0) as client:
            response = await client.post(service_url, files=files, data=data, timeout=180.0)
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Anthropic Service Error: {response.text}",
                )
            return response.json()
    except httpx.ConnectError as exc:
        raise HTTPException(status_code=503, detail="Anthropic Service is unavailable.") from exc
    except httpx.ReadTimeout as exc:
        raise HTTPException(status_code=504, detail="Model inference timed out.") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Proxy Error: {exc}") from exc
