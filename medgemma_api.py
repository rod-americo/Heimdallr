import os
import io
import base64
import asyncio
import contextlib
import time
from typing import Dict, Any, List

import torch
import numpy as np
import pydicom
from PIL import Image
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from pydantic import BaseModel, Field
from transformers import pipeline
import openai
from dotenv import load_dotenv
import medgemma_prompts

# Load environment variables (expecting .env in the same directory or passed via env)
load_dotenv()

# Configuration
PORT = int(os.getenv("MEDGEMMA_PORT", "8002"))
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
MODEL_ID = "google/medgemma-1.5-4b-it"
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Global state
class AppState:
    pipe = None
    lock = asyncio.Lock()

state = AppState()

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Load the MedGemma model on startup to avoid latency on first request.
    """
    print(f"Loading MedGemma model ({MODEL_ID}) on {DEVICE}...")
    try:
        state.pipe = pipeline(
            "image-text-to-text",
            model=MODEL_ID,
            torch_dtype=torch.bfloat16,
            device=DEVICE,
        )
        print("MedGemma model loaded successfully.")
    except Exception as e:
        print(f"FATAL: Failed to load MedGemma model: {e}")
        # We don't raise here to allow API to start and report health status,
        # but usage will fail.
    
    yield
    
    # Clean up (if needed)
    if state.pipe:
        del state.pipe
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

app = FastAPI(title="MedGemma Analysis Service", lifespan=lifespan)



class AnalysisResponse(BaseModel):
    medgemma_output: str
    final_report: str
    timings: Dict[str, float] = Field(..., description="Processing times in seconds")


def dicom_to_pil(file_object) -> Image.Image:
    """
    Convert DICOM file object to PIL Image.
    Logic adapted from medgemma_rx.py (validated).
    """
    try:
        ds = pydicom.dcmread(file_object)
        arr = ds.pixel_array.astype(np.float32)

        slope = float(getattr(ds, "RescaleSlope", 1.0))
        intercept = float(getattr(ds, "RescaleIntercept", 0.0))
        arr = arr * slope + intercept

        wc = getattr(ds, "WindowCenter", None)
        ww = getattr(ds, "WindowWidth", None)
        if wc is not None and ww is not None:
            wc = float(wc[0] if hasattr(wc, "__len__") else wc)
            ww = float(ww[0] if hasattr(ww, "__len__") else ww)
            lo, hi = wc - ww/2, wc + ww/2
            arr = np.clip(arr, lo, hi)

        arr -= arr.min()
        arr /= (arr.max() + 1e-6)
        arr = (arr * 255).astype(np.uint8)
        return Image.fromarray(arr).convert("RGB")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing DICOM: {str(e)}")

def load_image_file(file_content: bytes) -> Image.Image:
    """Load image from bytes (tries DICOM first, then standard formats)"""
    file_obj = io.BytesIO(file_content)
    
    # Try DICOM first
    try:
        # Check header (DICM at offset 128)
        file_obj.seek(128)
        if file_obj.read(4) == b"DICM":
            file_obj.seek(0)
            return dicom_to_pil(file_obj)
        # Some DICOMs lack preamble, try catch-all pydicom read
        file_obj.seek(0)
        try:
            return dicom_to_pil(file_obj)
        except:
            pass # Not a DICOM
            
    except Exception:
        pass
        
    # Try standard image (JPG, PNG)
    try:
        file_obj.seek(0)
        image = Image.open(file_obj)
        return image.convert("RGB")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid file format (not DICOM or Image): {str(e)}")


@app.post("/analyze", response_model=AnalysisResponse)
async def analyze(
    file: UploadFile = File(..., description="Image file (DICOM, JPG, PNG)"),
    age: str = Form("unknown age", description="Patient age (e.g. '45-year-old')")
):
    """
    Analyze X-ray image using MedGemma + OpenAI.
    Accepts DICOM or standard images and Age via Multipart upload.
    """
    if state.pipe is None:
        raise HTTPException(status_code=503, detail="MedGemma model not initialized")

    start_total = time.time()
    timings = {}

    # Read file content
    try:
        content = await file.read()
        
        start_conversion = time.time()
        image = load_image_file(content)
        timings["dcm_conversion"] = round(time.time() - start_conversion, 3)
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to load file: {e}")

    # Acquire lock for GPU inference
    async with state.lock:
        try:
            start_medgemma = time.time()
            
            # 1. Run MedGemma
            # Construct the full prompt using the single template
            full_prompt = medgemma_prompts.MEDGEMMA_PROMPT_TEMPLATE.format(age=age)
            
            messages = [{
                "role": "user",
                "content": [
                    {"type": "image", "image": image},
                    {"type": "text", "text": full_prompt}
                ]
            }]

            # Run inference
            out = state.pipe(text=messages, max_new_tokens=800) # Increased tokens for longer report
            medgemma_output = out[0]["generated_text"][-1]["content"]
            
            timings["medgemma_inference"] = round(time.time() - start_medgemma, 3)
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"MedGemma inference failed: {str(e)}")

    # 2. Run OpenAI (No lock needed as it's an external API call)
    try:
        start_openai = time.time()
        client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        # Determine strictness of following the user prompt vs using the findings
        # For now, we provide the findings as context.
        # Prepare the full prompt using the Portuguese template
        full_openai_prompt = medgemma_prompts.OPENAI_PROMPT_TEMPLATE.format(saida_medgemma=medgemma_output)

        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "user", "content": full_openai_prompt}
            ]
        )
        final_report = response.choices[0].message.content
        
        timings["openai_generation"] = round(time.time() - start_openai, 3)
        
    except Exception as e:
        # Fallback if OpenAI fails
        raise HTTPException(status_code=502, detail=f"OpenAI API failed: {str(e)}")

    timings["total"] = round(time.time() - start_total, 3)

    return AnalysisResponse(
        medgemma_output=medgemma_output,
        final_report=final_report,
        timings=timings
    )

@app.get("/health")
def health_check():
    return {"status": "ok", "model_loaded": state.pipe is not None}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
