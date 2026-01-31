#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import shutil
import zipfile
import subprocess
from pathlib import Path
import pydicom

# ============================================================
# CONFIGURAÇÃO
# ============================================================

BASE = Path(__file__).resolve().parent
RAW_DIR = BASE / "dcm"
EXTRACT_DIR = BASE / "dcm_extracted"
EXTRACT_DIR.mkdir(exist_ok=True)
OUT_NIFTI = BASE / "ct_original.nii.gz"

# ============================================================
# EXPANSÃO DE ENTRADAS (ZIP, SUBPASTAS, ETC.)
# ============================================================

def expandir_entradas():
    """Extrai ZIPs e copia todo arquivo DICOM para dcm_extracted/."""
    for root, dirs, files in os.walk(RAW_DIR):
        for f in files:
            path = Path(root) / f

            # ZIP → extrair
            if path.suffix.lower() == ".zip":
                with zipfile.ZipFile(path, "r") as z:
                    z.extractall(EXTRACT_DIR)
                continue

            # tentar identificar DICOM
            try:
                ds = pydicom.dcmread(str(path), stop_before_pixels=True)
                shutil.copy(path, EXTRACT_DIR)
            except Exception:
                pass  # ignora não-DICOMs


expandir_entradas()

# coletar todos os dicoms extraídos
arquivos = list(EXTRACT_DIR.glob("**/*"))
if not arquivos:
    print("erro: nenhum dicom encontrado após expansão.")
    sys.exit(1)

# ============================================================
# DETECTAR TODAS AS SÉRIES
# ============================================================

def agrupar_por_serie():
    series = {}
    for f in arquivos:
        try:
            ds = pydicom.dcmread(str(f), stop_before_pixels=True)
        except:
            continue

        # precisa ter SeriesInstanceUID
        if not hasattr(ds, "SeriesInstanceUID"):
            continue

        uid = ds.SeriesInstanceUID
        if uid not in series:
            series[uid] = {
                "arquivos": [],
                "modality": getattr(ds, "Modality", None),
                "slice_thickness": getattr(ds, "SliceThickness", None),
                "pixel_spacing": getattr(ds, "PixelSpacing", None)
            }
        series[uid]["arquivos"].append(f)

    return series


series = agrupar_por_serie()
if not series:
    print("erro: nenhuma série dicom válida encontrada.")
    sys.exit(1)

# ============================================================
# SELEÇÃO DO MELHOR VOLUME
# ============================================================

def pontuar(serie):
    """Retorna escore para priorizar série mais adequada."""
    arquivos = serie["arquivos"]
    n = len(arquivos)

    modality = serie["modality"]
    if modality != "CT":
        return -1  # descarta não-CT

    score = 0

    # 1. mais fatias = melhor
    score += n

    # 2. penalizar séries muito finas (<0.3 mm) ou espessas (>7 mm)
    st = serie["slice_thickness"]
    if st is not None:
        try:
            st = float(st)
            if 0.3 <= st <= 7:
                score += 50
        except:
            pass

    # 3. penalizar reconstruções "bone"
    # exame Kernel
    try:
        ds = pydicom.dcmread(str(arquivos[0]), stop_before_pixels=True)
        kernel = getattr(ds, "ConvolutionKernel", "").lower()
        if "b" in kernel and any(k in kernel for k in ["60", "70", "one", "sharp"]):
            score -= 100  # provavelmente kernel ósseo
        if "lung" in kernel:
            score -= 100
    except:
        pass

    return score


# escolher melhor UID
melhor_uid = None
melhor_score = -999999

for uid, serie in series.items():
    s = pontuar(serie)
    if s > melhor_score:
        melhor_score = s
        melhor_uid = uid

melhor_serie = series[melhor_uid]

print(f"série escolhida: {melhor_uid}")
print(f"n arquivos: {len(melhor_serie['arquivos'])}")
print(f"modality: {melhor_serie['modality']}")
print(f"slice_thickness: {melhor_serie['slice_thickness']}")

# ============================================================
# CONVERSÃO PARA NIFTI
# ============================================================

def garantir_dcm2niix():
    try:
        subprocess.run(["dcm2niix", "-h"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return
    except FileNotFoundError:
        print("erro: dcm2niix não encontrado no sistema.")
        sys.exit(1)

garantir_dcm2niix()

# criar pasta temporária só com os arquivos da melhor série
TMP_SERIE = BASE / "tmp_serie"
if TMP_SERIE.exists():
    shutil.rmtree(TMP_SERIE)
TMP_SERIE.mkdir()

for f in melhor_serie["arquivos"]:
    shutil.copy(f, TMP_SERIE)

print("convertendo a melhor série para NIfTI...")

resultado = subprocess.run([
    "dcm2niix",
    "-z", "y",
    "-f", "ct_original",
    "-o", str(BASE),
    str(TMP_SERIE)
], capture_output=True, text=True)

if resultado.returncode != 0:
    print("erro na conversão:")
    print(resultado.stderr)
    sys.exit(1)

# localizar e renomear nifti final
gerados = list(BASE.glob("ct_original*.nii.gz"))
if not gerados:
    print("erro: conversão falhou, nenhum NIfTI encontrado.")
    sys.exit(1)

gerados[0].rename(OUT_NIFTI)
print(f"concluído: {OUT_NIFTI}")