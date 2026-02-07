import os
import io
import json
import base64
import time
import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Any

from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from pydantic import BaseModel
import anthropic
from dotenv import load_dotenv

# Import conversion and parsing logic
# Assuming these files are in the same directory (Heimdallr)
from img_conversor import otimizar_imagem_para_api
try:
    from anthropic_report_builder import extrair_json_do_texto, montar_laudo_a_partir_json
except ImportError:
    # Fallback if file not found (will be verified next step)
    def extrair_json_do_texto(text): return {"raw": text}
    def montar_laudo_a_partir_json(data): return str(data)

# Load environment variables
load_dotenv()

# Configuration
PORT = int(os.getenv("ANTHROPIC_PORT", "8003"))
API_KEY = os.getenv("ANTHROPIC_API_KEY")
DATA_DIR = Path("data/dataset")

if not API_KEY:
    print("WARNING: ANTHROPIC_API_KEY not found in environment.")

# Initialize Anthropic Client
client = anthropic.Anthropic(api_key=API_KEY)

app = FastAPI(title="Anthropic X-Ray Analysis Service")

class AnalysisResponse(BaseModel):
    laudo_estruturado: str
    dados_json: Dict[str, Any]
    timings: Dict[str, float]
    usage: Dict[str, Any]

@app.post("/analyze", response_model=AnalysisResponse)
async def analyze_xray(
    file: UploadFile = File(...),
    age: str = Form(...),
    identificador: str = Form(...)
):
    """
    Analyze Chest X-Ray using Anthropic Claude.
    
    Steps:
    1. Save/Convert uploaded file to JPG
    2. Send to Anthropic
    3. Save response and return parsed data
    """
    start_time = time.time()
    timings = {}
    
    # 1. Prepare Directory
    case_dir = DATA_DIR / identificador
    case_dir.mkdir(parents=True, exist_ok=True)
    
    # 2. Save Uploaded File Temporarily (to handle DICOM/IMG conversion)
    # img_conversor expects a file path
    suffix = Path(file.filename).suffix
    if not suffix:
        suffix = ".tmp"
        
    with NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name
    
    try:
        # 3. Convert/Optimize Image
        # otimizar_imagem_para_api returns (bytes, mime_type)
        conv_start = time.time()
        binary_data, media_type = otimizar_imagem_para_api(tmp_path)
        timings["conversion"] = round(time.time() - conv_start, 3)
        
        # Save xray.jpg
        xray_path = case_dir / "xray.jpg"
        with open(xray_path, "wb") as f:
            f.write(binary_data)
            
        base64_image = base64.b64encode(binary_data).decode("utf-8")
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Image conversion failed: {str(e)}")
    finally:
        # Cleanup temp file
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    # 4. Prepare Prompt
    try:
        with open("prompts/rx_thorax_ap.txt", "r", encoding="utf-8") as f:
            system_instruction = f.read()
    except FileNotFoundError:
        system_instruction = "You are an expert Radiologist. Analyze this image."

    user_message = f"""
    Patient Metadata:
    - Age: {age}
    
    Task:
    Extract the radiological data from this image into the required JSON format.
    REMINDER: be extremely conservative with findings. If unsure, return false/null.
    """

    # 5. Call Anthropic API
    try:
        api_start = time.time()
        message = client.messages.create(
            model="claude-sonnet-4-5-20250929", # Using the model ID from poc_anthropic.py (2026 context)
            max_tokens=2048,
            temperature=0.0,
            system=system_instruction,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": media_type,
                                "data": base64_image,
                            },
                        },
                        {
                            "type": "text",
                            "text": user_message
                        }
                    ],
                }
            ],
        )
        timings["anthropic_api"] = round(time.time() - api_start, 3)
        
        raw_text = message.content[0].text
        
        # Usage stats
        usage = {
            "input_tokens": message.usage.input_tokens,
            "output_tokens": message.usage.output_tokens
        }
        
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Anthropic API Error: {str(e)}")

    # 6. Process Response
    try:
        # Save raw response
        with open(case_dir / "response.txt", "w", encoding="utf-8") as f:
            f.write(raw_text)
            
        # Parse JSON and Structure
        dados_json = extrair_json_do_texto(raw_text)
        laudo_estruturado = montar_laudo_a_partir_json(dados_json)
        
    except Exception as e:
        # Fallback if parsing fails
        dados_json = {"error": "Parsing failed", "details": str(e)}
        laudo_estruturado = raw_text

    timings["total"] = round(time.time() - start_time, 3)

    return AnalysisResponse(
        laudo_estruturado=laudo_estruturado,
        dados_json=dados_json,
        timings=timings,
        usage=usage
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
