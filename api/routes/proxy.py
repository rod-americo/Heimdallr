from fastapi import APIRouter, File, UploadFile, Form, HTTPException
import httpx
import config

router = APIRouter(prefix="/api", tags=["proxy"])

@router.post("/medgemma/ap-thorax-xray")
async def analyze_xray(
    file: UploadFile = File(..., description="Image file"),
    age: str = Form("unknown age", description="Patient age (e.g. '45-year-old')")
):
    service_url = config.MEDGEMMA_SERVICE_URL
    try:
        file_content = await file.read()
        files = {'file': (file.filename, file_content, file.content_type)}
        data = {'age': age}
        
        async with httpx.AsyncClient(timeout=180.0) as client:
            response = await client.post(service_url, files=files, data=data, timeout=180.0)
            if response.status_code != 200:
                raise HTTPException(status_code=response.status_code, detail=f"MedGemma Service Error: {response.text}")
            return response.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail="MedGemma Service is unavailable.")
    except httpx.ReadTimeout:
        raise HTTPException(status_code=504, detail="Model inference timed out.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Proxy Error: {str(e)}")

@router.post("/anthropic/ap-thorax-xray")
async def analyze_xray_anthropic(
    file: UploadFile = File(..., description="Image file (DICOM, JPG, PNG)"),
    age: str = Form("unknown", description="Patient age"),
    identificador: str = Form(..., description="Patient Identifier")
):
    service_url = config.ANTHROPIC_SERVICE_URL
    try:
        file_content = await file.read()
        files = {'file': (file.filename, file_content, file.content_type)}
        data = {'age': age, 'identificador': identificador}
        
        async with httpx.AsyncClient(timeout=180.0) as client:
            response = await client.post(service_url, files=files, data=data, timeout=180.0)
            if response.status_code != 200:
                raise HTTPException(status_code=response.status_code, detail=f"Anthropic Service Error: {response.text}")
            return response.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail="Anthropic Service is unavailable.")
    except httpx.ReadTimeout:
        raise HTTPException(status_code=504, detail="Model inference timed out.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Proxy Error: {str(e)}")
