import numpy as np
import pydicom
from PIL import Image
from transformers import pipeline
import torch

def dicom_to_pil(path: str) -> Image.Image:
    ds = pydicom.dcmread(path)
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

pipe = pipeline(
    "image-text-to-text",
    model="google/medgemma-1.5-4b-it",
    torch_dtype=torch.bfloat16,
    device="cuda",
)

img = dicom_to_pil("rx.dcm")

messages = [{
    "role": "user",
    "content": [
        {"type": "image", "image": img},
        {"type": "text", "text": "Chest X-ray: write findings and impression."}
    ]
}]

out = pipe(text=messages, max_new_tokens=600)
print("")
print(out[0]["generated_text"][-1]["content"])
