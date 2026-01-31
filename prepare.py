#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import shutil
import zipfile
import subprocess
import argparse
import tempfile
import re
from pathlib import Path
import pydicom

# Ensure venv/bin is in PATH for subprocess calls
os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]

# ============================================================
# CONFIGURAÇÃO
# ============================================================

BASE = Path(__file__).resolve().parent
INPUT_DIR = BASE / "input"
INPUT_DIR.mkdir(exist_ok=True)

def clean_filename(s):
    # Remove caracteres inválidos para nome de arquivo
    s = str(s).strip()
    return re.sub(r'[^a-zA-Z0-9_-]', '', s)

def process_zip(zip_path):
    zip_path = Path(zip_path)
    if not zip_path.exists():
        print(f"Erro: Arquivo {zip_path} não encontrado.")
        sys.exit(1)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        extract_dir = temp_dir / "extracted"
        extract_dir.mkdir()
        
        print(f"Extraindo {zip_path} para {extract_dir}...")
        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(extract_dir)
        except zipfile.BadZipFile:
             print("Erro: Arquivo ZIP inválido.")
             sys.exit(1)

        # ---------------------------------------------------------
        # 1. Coletar DICOMS e identificar séries
        # ---------------------------------------------------------
        dicom_files = []
        for root, _, files in os.walk(extract_dir):
            for f in files:
                fpath = Path(root) / f
                try:
                    ds = pydicom.dcmread(str(fpath), stop_before_pixels=True)
                    if hasattr(ds, "SeriesInstanceUID"):
                         dicom_files.append((fpath, ds))
                except:
                    pass
        
        if not dicom_files:
            print("Erro: Nenhum DICOM válido encontrado no ZIP.")
            sys.exit(1)

        series = {}
        for fpath, ds in dicom_files:
            uid = ds.SeriesInstanceUID
            if uid not in series:
                series[uid] = {
                    "arquivos": [],
                    "modality": getattr(ds, "Modality", None),
                    "slice_thickness": getattr(ds, "SliceThickness", None),
                    "patient_name": getattr(ds, "PatientName", "Unknown"),
                    "accession_number": getattr(ds, "AccessionNumber", "000000")
                }
            series[uid]["arquivos"].append(fpath)

        # ---------------------------------------------------------
        # 2. Selecionar Melhor Série
        # ---------------------------------------------------------
        melhor_uid = None
        melhor_score = -999999

        for uid, s_data in series.items():
            # Critérios de pontuação (mantidos do original)
            score = 0
            if s_data["modality"] != "CT":
                score = -1000
            else:
                score += len(s_data["arquivos"])
                
                st = s_data["slice_thickness"]
                if st:
                    try:
                        if 0.3 <= float(st) <= 7: score += 50
                    except: pass
                
                # Checar kernel no primeiro arquivo
                try:
                    first_dcm = pydicom.dcmread(str(s_data["arquivos"][0]), stop_before_pixels=True)
                    kernel = getattr(first_dcm, "ConvolutionKernel", "").lower()
                    if "b" in kernel and any(k in kernel for k in ["60", "70", "one", "sharp"]):
                        score -= 100
                    if "lung" in kernel:
                         score -= 100
                except: pass
            
            if score > melhor_score:
                melhor_score = score
                melhor_uid = uid

        if not melhor_uid:
             print("Erro: Nenhuma série CT válida encontrada.")
             sys.exit(1)

        melhor_serie = series[melhor_uid]
        print(f"Série escolhida: {melhor_uid} ({len(melhor_serie['arquivos'])} arquivos)")

        # ---------------------------------------------------------
        # 3. Preparar Nome do Arquivo de Saída
        # ---------------------------------------------------------
        p_name = clean_filename(melhor_serie["patient_name"])
        acc_num = clean_filename(melhor_serie["accession_number"])
        
        # Fallback se nomes estiverem vazios
        if not p_name: p_name = "Patient"
        if not acc_num: acc_num = "0000"

        out_name = f"{p_name}_{acc_num}.nii.gz"
        out_path = INPUT_DIR / out_name

        # ---------------------------------------------------------
        # 4. Converter com dcm2niix
        # ---------------------------------------------------------
        # Copiar apenas os arquivos da série eleita para uma pasta limpa para conversão
        dcm2niix_input = temp_dir / "dcm2niix_input"
        dcm2niix_input.mkdir()
        for f in melhor_serie["arquivos"]:
            shutil.copy(f, dcm2niix_input)

        print(f"Convertendo para {out_path}...")
        
        # dcm2niix gera output com nome baseado no input, então usamos um temp output dir
        dcm2niix_output = temp_dir / "nii_out"
        dcm2niix_output.mkdir()

        subprocess.run([
            "dcm2niix",
            "-z", "y",
            "-f", "converted", # nome fixo temporário
            "-o", str(dcm2niix_output),
            str(dcm2niix_input)
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

        # Achar o arquivo gerado
        gerados = list(dcm2niix_output.glob("*.nii.gz"))
        if not gerados:
            print("Erro: dcm2niix falhou em gerar o arquivo.")
            sys.exit(1)
            
        # Mover e renomear para destino final
        shutil.move(str(gerados[0]), str(out_path))
        print(f"Sucesso: {out_path}")
        print(str(out_path)) # Imprimir caminho final para ser capturado por quem chamou

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Converte ZIP de DICOMs para NIfTI com nomenclatura padronizada.")
    parser.add_argument("zip_path", help="Caminho para o arquivo ZIP contendo DICOMs")
    args = parser.parse_args()

    process_zip(args.zip_path)