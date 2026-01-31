import os
import json
import shutil
import subprocess
import threading
import sys
from pathlib import Path
import nibabel as nib
import numpy as np

# Ensure venv/bin is in PATH for subprocess calls (TotalSegmentator, dcm2niix)
os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]

# ============================================================
# CONFIGURAÇÕES
# ============================================================

LICENSE = "aca_VD42VF39LY0V20"
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
ARCHIVE_DIR = BASE_DIR / "nii"

INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
ARCHIVE_DIR.mkdir(exist_ok=True)


def run_task(task_name, input_file, output_folder, extra_args=None):
    if extra_args is None:
        extra_args = []
    print(f"[{task_name}] Iniciando...")
    
    cmd = [
        "TotalSegmentator",
        "-l", LICENSE,
        "-i", str(input_file),
        "-o", str(output_folder),
        "--task", task_name
    ] + extra_args
    
    # Run with Popen to filter stdout and suppress citation
    process = subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT,
        text=True, 
        bufsize=1
    )
    
    for line in process.stdout:
        if "If you use this tool please cite" in line:
            continue
        # print(line, end="") # Suppress voluminous output unless needed
    
    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, cmd)
        
    print(f"[{task_name}] Finalizado.")


def process_case(nifti_path):
    # Identificar caso pelo nome do arquivo (ex: Paciente_1234.nii.gz -> Paciente_1234)
    case_id = nifti_path.name.replace("".join(nifti_path.suffixes), "")
    case_output = OUTPUT_DIR / case_id
    
    # Resetar pasta de output do caso
    if case_output.exists():
        shutil.rmtree(case_output)
    case_output.mkdir(parents=True)
    
    (case_output / "total").mkdir(exist_ok=True)
    (case_output / "tissue_types").mkdir(exist_ok=True)

    print(f"\n=== Processando Caso: {case_id} ===")

    # 1. Segmentação Paralela
    t1 = threading.Thread(target=run_task, args=("total", nifti_path, case_output / "total", ["--fast"]))
    t2 = threading.Thread(target=run_task, args=("tissue_types", nifti_path, case_output / "tissue_types"))

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # 2. Cálculo L3
    try:
        vertebra_L3_file = case_output / "total" / "vertebrae_L3.nii.gz"
        muscle_file      = case_output / "tissue_types" / "skeletal_muscle.nii.gz"
        
        if not vertebra_L3_file.exists():
             raise FileNotFoundError("Segmentação de L3 falhou (arquivo não encontrado).")

        ct = nib.load(str(nifti_path)).get_fdata()
        
        # Detectar fatia L3
        nii_L3 = nib.load(str(vertebra_L3_file))
        mask_L3 = nii_L3.get_fdata()
        
        slice_L3 = np.where(mask_L3.sum(axis=(0, 1)) > 0)[0]
        if len(slice_L3) == 0:
            raise ValueError("nível L3 não encontrado na máscara.")
        
        slice_idx = int(slice_L3[len(slice_L3)//2])
        
        # Área Muscular
        if not muscle_file.exists():
             raise FileNotFoundError("Segmentação muscular falhou.")

        nii_muscle = nib.load(str(muscle_file))
        muscle_data = nii_muscle.get_fdata()
        spacing = nii_muscle.header.get_zooms()
        
        mask_slice = muscle_data[:, :, slice_idx]
        
        area_mm2 = np.sum(mask_slice > 0) * spacing[0] * spacing[1]
        area_cm2 = area_mm2 / 100.0
        
        # HU Muscular
        muscle_voxels = ct[:, :, slice_idx][mask_slice > 0]
        if muscle_voxels.size > 0:
            muscle_HU_mean = float(np.mean(muscle_voxels))
            muscle_HU_std = float(np.std(muscle_voxels))
        else:
            muscle_HU_mean = 0.0
            muscle_HU_std = 0.0

        # Output JSON
        out = {
            "case_id": case_id,
            "slice_L3": slice_idx,
            "SMA_cm2": round(area_cm2, 3),
            "muscle_HU_mean": round(muscle_HU_mean, 3),
            "muscle_HU_std": round(muscle_HU_std, 3)
        }
        
        json_path = case_output / "resultados.json"
        with open(json_path, "w") as f:
            json.dump(out, f, indent=2)
            
        print(f"Sucesso. Resultados em: {json_path}")
        
    except Exception as e:
        print(f"Erro no cálculo de métricas para {case_id}: {e}")
        with open(case_output / "error.log", "w") as f:
            f.write(str(e))
        return False

    # 3. Arquivar Input
    dest_nifti = ARCHIVE_DIR / nifti_path.name
    shutil.move(str(nifti_path), str(dest_nifti))
    print(f"Input movido para: {dest_nifti}")
    return True

def main():
    files = list(INPUT_DIR.glob("*.nii.gz"))
    if not files:
        print("Nenhum arquivo .nii.gz encontrado em input/ para processar.")
        return

    print(f"Encontrados {len(files)} arquivos na fila.")
    for f in files:
        process_case(f)

if __name__ == "__main__":
    main()