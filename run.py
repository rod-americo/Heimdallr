import os
import json
import subprocess
from pathlib import Path
import nibabel as nib
import numpy as np

# ============================================================
# CONFIGURAÇÕES
# ============================================================

LICENSE = "aca_VD42VF39LY0V20"
INPUT_CT = Path("/kaggle/input/ct-nifti-input/ct_original.nii.gz")
BASE = Path("/kaggle/working")
(BASE / "total").mkdir(exist_ok=True)
(BASE / "tissue_types").mkdir(exist_ok=True)

# ============================================================
# SEGMENTAÇÃO — TOTAL + TISSUE_TYPES
# ============================================================

subprocess.run([
    "TotalSegmentator",
    "-l", LICENSE,
    "-i", str(INPUT_CT),
    "-o", str(BASE / "total"),
    "--task", "total",
    "--fast"
], check=True)

subprocess.run([
    "TotalSegmentator",
    "-l", LICENSE,
    "-i", str(INPUT_CT),
    "-o", str(BASE / "tissue_types"),
    "--task", "tissue_types"
], check=True)

# ============================================================
# CÁLCULO L3
# ============================================================

vertebra_L3_file = BASE / "total" / "vertebrae_L3.nii.gz"
muscle_file      = BASE / "tissue_types" / "skeletal_muscle.nii.gz"

ct = nib.load(str(INPUT_CT)).get_fdata()

# detectar fatia L3
nii_L3 = nib.load(str(vertebra_L3_file))
mask_L3 = nii_L3.get_fdata()

slice_L3 = np.where(mask_L3.sum(axis=(0, 1)) > 0)[0]
if len(slice_L3) == 0:
    raise ValueError("nível L3 não encontrado.")

slice_idx = int(slice_L3[len(slice_L3)//2])

# área muscular
nii_muscle = nib.load(str(muscle_file))
muscle_data = nii_muscle.get_fdata()
spacing = nii_muscle.header.get_zooms()

mask_slice = muscle_data[:, :, slice_idx]

area_mm2 = np.sum(mask_slice > 0) * spacing[0] * spacing[1]
area_cm2 = area_mm2 / 100.0

# HU muscular
muscle_voxels = ct[:, :, slice_idx][mask_slice > 0]
muscle_HU_mean = float(np.mean(muscle_voxels))
muscle_HU_std = float(np.std(muscle_voxels))

# ============================================================
# SAÍDA JSON
# ============================================================

out = {
    "slice_L3": slice_idx,
    "SMA_cm2": round(area_cm2, 3),
    "muscle_HU_mean": round(muscle_HU_mean, 3),
    "muscle_HU_std": round(muscle_HU_std, 3)
}

with open(BASE / "resultados.json", "w") as f:
    json.dump(out, f, indent=2)

print("Pronto. Arquivo salvo em: /kaggle/working/resultados.json")