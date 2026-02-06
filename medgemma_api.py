import os
import io
import base64
import asyncio
import contextlib
import time
from typing import Dict, Any, List

import torch
from PIL import Image
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from transformers import pipeline
import openai
from dotenv import load_dotenv

# Load environment variables (expecting .env in the same directory or passed via env)
load_dotenv()

# Configuration
PORT = int(os.getenv("MEDGEMMA_PORT", "8002"))
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
MODEL_ID = "google/medgemma-1.5-4b-it"

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

class AnalysisRequest(BaseModel):
    image: str = Field(..., description="Base64 encoded image (PNG/JPG)")
    prompt: str = Field(..., description="User prompt for synthesis")

class AnalysisResponse(BaseModel):
    medgemma_output: str
    final_report: str
    timings: Dict[str, float] = Field(..., description="Processing times in seconds")

def decode_image(base64_string: str) -> Image.Image:
    try:
        # Remove header if present (e.g., "data:image/png;base64,")
        if "," in base64_string:
            base64_string = base64_string.split(",")[1]
        
        image_data = base64.b64decode(base64_string)
        image = Image.open(io.BytesIO(image_data))
        return image.convert("RGB")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid image data: {str(e)}")

@app.post("/analyze", response_model=AnalysisResponse)
async def analyze(request: AnalysisRequest):
    """
    Analyze X-ray image using MedGemma + OpenAI.
    1. MedGemma generates findings/impression.
    2. OpenAI refines/answers based on User Prompt + MedGemma Output.
    """
    if state.pipe is None:
        raise HTTPException(status_code=503, detail="MedGemma model not initialized")

    start_total = time.time()
    timings = {}

    # Decode image
    image = decode_image(request.image)

    # Acquire lock for GPU inference
    async with state.lock:
        try:
            start_medgemma = time.time()
            
            # 1. Run MedGemma
            # Fixed prompt for MedGemma to extract findings
            medgemma_prompt = "Chest X-ray: write findings and impression."
            
            messages = [{
                "role": "user",
                "content": [
                    {"type": "image", "image": image},
                    {"type": "text", "text": medgemma_prompt}
                ]
            }]

            # Run inference in thread pool if needed, but pipeline is usually efficient enough
            # Blocking call here, but lock protects concurrency
            out = state.pipe(text=messages, max_new_tokens=600)
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
        response = client.chat.completions.create(
            model="gpt-4o", 
            messages=[
                {"role": "system", "content": "You are a radiologist assistant. You will receive raw findings from an AI model (MedGemma) analyzing a Chest X-ray, and a user prompt/question. Answer the user prompt based on the AI findings."},
                {"role": "user", "content": f"MedGemma Findings:\n{medgemma_output}\n\nUser Request:\n{request.prompt}"}
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
