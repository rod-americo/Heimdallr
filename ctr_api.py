# ---------------------------------------------------------------------------
# CTR Extraction API — Cardiothoracic Ratio (ICT) via CXAS
#
# Based on ChestXRayAnatomySegmentation (CXAS) by Constantin Seibold et al.
# Licensed under Creative Commons Attribution-NonCommercial-ShareAlike 4.0
# International (CC BY-NC-SA 4.0).
# https://github.com/ConstantinSeibold/ChestXRayAnatomySegmentation
#
# This integration is used for personal and experimental purposes in
# radiology.  No commercial distribution or monetization is permitted.
# Derivatives of this module must carry the same CC BY-NC-SA 4.0 license.
#
# Atribuição: "Baseado em ChestXRayAnatomySegmentation de Constantin
# Seibold, CC BY-NC-SA 4.0"
# ---------------------------------------------------------------------------

import os
import sys
import json
import shutil
import tempfile
from pathlib import Path
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, UploadFile, File
from pydantic import BaseModel

import torch
import argparse

import colorcet as cc
if not hasattr(cc.cm, "glasbey_bw_minc_20") or not callable(getattr(cc.cm, "glasbey_bw_minc_20", None)):
    # CXAS incorrectly assumes this is a callable colormap object in colorcet 1.0.0. 
    # It crashes uvicorn on import. We mock it.
    cc.cm.glasbey_bw_minc_20 = lambda x: (0, 0, 0, 1)

import cxas

PORT = int(os.getenv("CTR_PORT", "8003"))

# Configure centralized CXAS model weight caching
# These weights are ~841MB, so we keep them persistent.
os.environ["CXAS_PATH"] = str((Path(__file__).resolve().parent / "models" / "cxas").resolve())

class AppState:
    model: cxas.CXAS = None

state = AppState()

# PyTorch monkey-patches to resolve issues inside cxas loading
def apply_cxas_patches():
    # PyTorch 2.6+ compatibility for cxas loading argparse.Namespace within its checkpoints
    if hasattr(torch.serialization, "add_safe_globals"):
        torch.serialization.add_safe_globals([argparse.Namespace])

    orig_init = cxas.CXAS.__init__
    def patched_init(self, *args_i, **kwargs_i):
        orig_init(self, *args_i, **kwargs_i)
        # The segmentor internals always assume self.gpus is a list where [0] is the device ID.
        if hasattr(self, "gpus") and isinstance(self.gpus, str):
            if self.gpus == "cpu":
                self.gpus = ["cpu"]
            else:
                self.gpus = [self.gpus]
    cxas.CXAS.__init__ = patched_init

    orig_tensor_to = torch.Tensor.to
    def patched_tensor_to(self_tensor, *args_to, **kwargs_to):
        # Intercept string device targeting (fixes cuda:cuda:0 bugs)
        new_args = list(args_to)
        if len(new_args) > 0 and isinstance(new_args[0], str):
            dev_str = new_args[0]
            if "cuda:cuda:" in dev_str:
                new_args[0] = dev_str.replace("cuda:cuda:", "cuda:")
        
        if "device" in kwargs_to and isinstance(kwargs_to["device"], str):
            dev_str = kwargs_to["device"]
            if "cuda:cuda:" in dev_str:
                kwargs_to["device"] = dev_str.replace("cuda:cuda:", "cuda:")
                
        return orig_tensor_to(self_tensor, *tuple(new_args), **kwargs_to)
    torch.Tensor.to = patched_tensor_to


import contextlib

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Load the heavy CXAS U-Net/ResNet model on startup to completely
    avoid latency on single image extraction requests.
    """
    print("Applying PyTorch compatibility patches for CXAS...")
    apply_cxas_patches()
    
    # Try using GPU 0, fallback to CPU if not available
    selected_gpus = "0"
    if not torch.cuda.is_available():
        print("No GPU is available. Switching to CPU.")
        selected_gpus = "cpu"

    if selected_gpus == "cpu":
        torch.cuda.is_available = lambda: False  # Force cxas internally to drop to CPU branches
    
    print(f"Loading CXAS model (UNet_ResNet50_default) on {selected_gpus}...")
    try:
        state.model = cxas.CXAS(model_name="UNet_ResNet50_default", gpus=selected_gpus)
        print("CXAS model loaded successfully.")
    except Exception as e:
        print(f"FATAL: Failed to load CXAS model: {e}")
        
    yield
    
    print("Shutting down CXAS model...")
    if state.model:
        del state.model
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

app = FastAPI(title="CTR Extraction API", lifespan=lifespan)


class CTResponse(BaseModel):
    ctr: str
    cardiomegaly_flag: str
    

@app.post("/extract_ctr", response_model=CTResponse)
async def extract_ctr(
    file: UploadFile = File(..., description="Chest X-Ray image file (DICOM, JPG, PNG)")
):
    """
    Directly extracts Cardiothoracic Ratio (score) using models fully loaded in VRAM.
    Takes ~3s instead of ~50s.
    """
    if state.model is None:
        raise HTTPException(status_code=503, detail="CXAS model not initialized")

    # Localize payload input into a temp directory
    temp_dir = Path(tempfile.mkdtemp(prefix="api_ctr_"))
    temp_img_path = temp_dir / file.filename
    
    os.makedirs("/tmp/cxas_raw", exist_ok=True)
    work_dir = Path(tempfile.mkdtemp(prefix="cxas_run_", dir="/tmp/cxas_raw"))
    
    try:
        work_dir.mkdir(parents=True, exist_ok=True)
        # Write image bytes to disk
        content = await file.read()
        with open(temp_img_path, "wb") as f:
            f.write(content)
            
        # Run synchronous extraction against VRAM model
        # Disabling store_pred strictly returns JSON and doesn't pollute /tmp with visual masks
        feat_dict = state.model.extract_features_for_file(
            filename=str(temp_img_path),
            output_directory=str(work_dir),
            feat_to_extract="CTR",
            create=True,
            do_store=False,
            storage_type="png",
        )
        
        if not feat_dict or "score" not in feat_dict:
            raise HTTPException(status_code=500, detail="CXAS failed to return a score metric.")

        try:
            ctr_value = float(feat_dict["score"])
            flag = "1" if ctr_value > 0.50 else "0"
        except (ValueError, TypeError):
            raise HTTPException(status_code=500, detail=f"Unparseable CTR float data: {feat_dict['score']}")
                
        return CTResponse(
            ctr=f"{ctr_value:.6f}",
            cardiomegaly_flag=flag
        )
    finally:
        # Cleanup API garbage
        shutil.rmtree(temp_dir, ignore_errors=True)
        shutil.rmtree(work_dir, ignore_errors=True)

@app.get("/health")
def health_check():
    return {"status": "ok", "model_loaded": state.model is not None}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("ctr_api:app", host="0.0.0.0", port=PORT)
