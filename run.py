import os
import json
import shutil
import subprocess
import threading
import sys
import time
from pathlib import Path

# Import Metrics Logic
from metrics import calculate_all_metrics

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

    # 1. Segmentação Paralela (Revertido para paralelo COM --fast)
    t1 = threading.Thread(target=run_task, args=("total", nifti_path, case_output / "total", ["--fast"]))
    t2 = threading.Thread(target=run_task, args=("tissue_types", nifti_path, case_output / "tissue_types"))

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # 2. Cálculo de Métricas (via metrics.py)
    try:
        out = calculate_all_metrics(case_id, nifti_path, case_output)
        
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
    print("Iniciando monitoramento da pasta input/ (Ctrl+C para parar)...")
    while True:
        try:
            files = list(INPUT_DIR.glob("*.nii.gz"))
            if files:
                # Processa apenas o primeiro da fila
                f = files[0]
                process_case(f)
            
            # Aguarda antes de verificar novamente
            time.sleep(3)
        except KeyboardInterrupt:
            print("\nParando monitoramento.")
            break
        except Exception as e:
            print(f"Erro no loop principal: {e}")
            time.sleep(3)

if __name__ == "__main__":
    main()