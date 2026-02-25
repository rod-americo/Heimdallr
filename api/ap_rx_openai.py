import os
import io
import json
import base64
import time
import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Any, Optional

from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from pydantic import BaseModel
import openai
from dotenv import load_dotenv

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import config
# Import conversion and parsing logic
from utils.img_conversor import otimizar_imagem_para_api
try:
    from services.anthropic_report_builder import extrair_json_do_texto
except ImportError:
    def extrair_json_do_texto(text):
        # Basic fallback for JSON extraction
        try:
            start = text.find('{')
            end = text.rfind('}') + 1
            if start != -1 and end != 0:
                return json.loads(text[start:end])
        except:
            pass
        return {"raw": text}

# Load environment variables
load_dotenv()

# Configuration
# User requested port 8002 specifically
PORT = int(os.getenv("OPENAI_API_PORT", "8002"))
API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_MODEL", "gpt-5.2") # Default to gpt-5.2 per user preference
DATA_DIR = Path("data/dataset/ap_rx_openai")

if not API_KEY:
    print("WARNING: OPENAI_API_KEY not found in environment.")

# Initialize OpenAI Client
client = openai.OpenAI(api_key=API_KEY)

app = FastAPI(title="OpenAI X-Ray Analysis Service")

class AnalysisResponse(BaseModel):
    laudo_estruturado: str
    dados_json: Dict[str, Any]
    timings: Dict[str, float]
    usage: Dict[str, Any]

@app.post("/analyze", response_model=AnalysisResponse)
async def analyze_xray(
    file: UploadFile = File(..., description="Image file (DICOM, JPG, PNG)"),
    age: str = Form("unknown age", description="Patient age (e.g. '45-year-old')"),
    ict: Optional[str] = Form(None, description="Cardio-Thoracic Ratio (ICT) value"),
    identificador: Optional[str] = Form(None, description="Patient/Case identifier for storage")
):
    """
    Analyze Chest X-Ray using OpenAI GPT-4o.
    Follows the prompt in prompts/ap_rx_thorax_openai.txt strictly.
    """
    start_time = time.time()
    timings = {}
    
    # 1. Prepare Directory (if identificador provided)
    case_dir = None
    if identificador:
        case_dir = DATA_DIR / identificador
        case_dir.mkdir(parents=True, exist_ok=True)
    
    # 2. Save Uploaded File Temporarily
    suffix = Path(file.filename).suffix if file.filename else ".tmp"
    if not suffix:
        suffix = ".tmp"
        
    with NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name
    
    try:
        # 3. Save Original and Convert/Optimize Image
        if case_dir:
            # Save original file
            original_path = case_dir / f"original{suffix}"
            file.file.seek(0)
            with open(original_path, "wb") as f:
                shutil.copyfileobj(file.file, f)
            file.file.seek(0) # Reset pointer for optimization

        conv_start = time.time()
        binary_data, media_type = otimizar_imagem_para_api(tmp_path)
        timings["conversion"] = round(time.time() - conv_start, 3)
        
        # Save xray.jpg (optimized) if case_dir exists
        if case_dir:
            xray_path = case_dir / "xray.jpg"
            with open(xray_path, "wb") as f:
                f.write(binary_data)
            
        base64_image = base64.b64encode(binary_data).decode("utf-8")
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Image conversion failed: {str(e)}")
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    # 4. Prepare Prompt
    try:
        prompt_path = "prompts/ap_rx_thorax_openai.txt"
        with open(prompt_path, "r", encoding="utf-8") as f:
            system_instruction = f.read()
        
        # Substitute placeholders
        ict_value = ict if (ict and ict.strip()) else "não fornecido"
        system_instruction = system_instruction.replace("{age}", age).replace("{ICT}", ict_value)
        
    except FileNotFoundError:
        system_instruction = f"Analise este RX de tórax. Paciente: {age}. ICT: {ict if ict else 'não fornecido'}."

    # 5. Call OpenAI API
    try:
        api_start = time.time()
        print(f"Calling OpenAI with model: {MODEL}")
        
        # Using the prompt as the system role and sending image in user role.
        # No extra text in user message to avoid interference with the system instructions.
        # Prepare OpenAI API parameters
        openai_params = {
            "model": MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": system_instruction
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{media_type};base64,{base64_image}"
                            }
                        }
                    ]
                }
            ]
        }
        
        # Newer models (o-series, gpt-5) use max_completion_tokens and don't support temperature=0.0 normally
        if MODEL.startswith("o") or MODEL.startswith("gpt-5"):
            openai_params["max_completion_tokens"] = 2048
        else:
            openai_params["temperature"] = 0.0
            openai_params["max_tokens"] = 2048
            
        response = client.chat.completions.create(**openai_params)
        timings["openai_api"] = round(time.time() - api_start, 3)
        
        raw_text = response.choices[0].message.content
        
        usage = {
            "prompt_tokens": response.usage.prompt_tokens,
            "completion_tokens": response.usage.completion_tokens,
            "total_tokens": response.usage.total_tokens
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=502, detail=f"OpenAI API Error: {str(e)}")

    # 6. Process Response
    if case_dir:
        # Save raw text
        with open(case_dir / "response_openai.txt", "w", encoding="utf-8") as f:
            f.write(raw_text)
            
        # Save full structured JSON
        response_data = {
            "laudo_estruturado": raw_text,
            "dados_json": {},
            "timings": timings,
            "usage": usage
        }
        
        # Try to extract JSON if it exists in the text
        try:
            response_data["dados_json"] = extrair_json_do_texto(raw_text)
        except:
            pass
            
        with open(case_dir / "response_openai.json", "w", encoding="utf-8") as f:
            json.dump(response_data, f, indent=4, ensure_ascii=False)
            
    # Return as API Response
    laudo_estruturado = raw_text
    dados_json = response_data["dados_json"] if case_dir else {}
    if not case_dir:
        try:
            dados_json = extrair_json_do_texto(raw_text)
        except:
            pass

    timings["total"] = round(time.time() - start_time, 3)

    return AnalysisResponse(
        laudo_estruturado=laudo_estruturado,
        dados_json=dados_json,
        timings=timings,
        usage=usage
    )

@app.get("/health")
def health_check():
    return {"status": "ok", "api_key_configured": bool(API_KEY)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
