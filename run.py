import os
import json
import shutil
import subprocess
import threading
import sys
import time
import datetime
import concurrent.futures # Added for parallel case processing
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
NII_DIR = ARCHIVE_DIR # Alias for compatibility
ERROR_DIR = BASE_DIR / "errors"

INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
ARCHIVE_DIR.mkdir(exist_ok=True)
ERROR_DIR.mkdir(exist_ok=True)


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
        # if "If you use this tool please cite" in line:
        #     continue
        print(line, end="") # Full logging enabled
    
    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, cmd)
        
    print(f"[{task_name}] Finalizado.")


def process_case(nifti_path):
    # Identificar caso pelo nome do arquivo (ex: Paciente_1234.nii.gz -> Paciente_1234)
    case_id = nifti_path.name.replace("".join(nifti_path.suffixes), "")
    case_output = OUTPUT_DIR / case_id
    
    # Resetar subdiretórios de output do caso
    if not case_output.exists():
        case_output.mkdir(parents=True)
    
    # Limpar apenas as pastas de resultado do TotalSegmentator
    for subdir in ["total", "tissue_types"]:
        p = case_output / subdir
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(exist_ok=True)

    print(f"\n=== Processando Caso: {case_id} ===")

    # Determinar Modalidade via id.json (Criado pelo prepare.py)
    # Default para CT se não encontrado (para compatibilidade legada)
    modality = "CT"
    id_json_path = case_output / "id.json"
    if id_json_path.exists():
        try:
            with open(id_json_path, 'r') as f:
                modality = json.load(f).get("Modality", "CT")
        except: 
            pass
            
    print(f"Modalidade detectada: {modality}")

    # 1. Segmentação Paralela
    # Thread 1: Anatomia Geral
    # Se MR -> total_mr, Se CT -> total
    # Nota: --fast funciona para ambos na versão recente do TotalSegmentator, 
    # mas total_mr é específico. Usaremos a flag --fast em ambos.
    
    task_gen = "total"
    if modality == "MR":
        task_gen = "total_mr"
        
    t1 = threading.Thread(target=run_task, args=(task_gen, nifti_path, case_output / "total", ["--fast"]))
    t1.start()
    
    t2 = None
    # Thread 2: Tecidos (Apenas se CT por enquanto)
    if modality == "CT":
        t2 = threading.Thread(target=run_task, args=("tissue_types", nifti_path, case_output / "tissue_types"))
        t2.start()

    t1.join()
    if t2:
        t2.join()

    # 1.5 Subtaferas de Especialidade (Condicional)
    # Se detectamos Crânio (Brain > 0) e é CT -> Rodar Hemorragia
    brain_file = case_output / "total" / "brain.nii.gz"
    if modality == "CT" and brain_file.exists():
        try:
             # Check if brain non-empty (simple file size check or just run it)
             # TotalSegmentator creates small empty files sometimes.
             if brain_file.stat().st_size > 1000: # 1KB arbitrary threshold for "something found"
                 print("[Condicional] Crânio detectado. Iniciando busca por hemorragia...")
                 bleed_output = case_output / "bleed"
                 bleed_output.mkdir(exist_ok=True)
                 run_task("cerebral_bleed", nifti_path, bleed_output)
        except Exception as e:
            print(f"Erro na execução condicional de hemorragia: {e}")

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
        
        # Mover para pasta de erro para evitar loop infinito
        error_dest = ERROR_DIR / nifti_path.name
        try:
            shutil.move(str(nifti_path), str(error_dest))
            print(f"Input movido para pasta de erro: {error_dest}")
        except Exception as move_err:
            print(f"Erro crítico: Não foi possível mover arquivo de erro {nifti_path}: {move_err}")
            
        return False


    # Atualizar id.json com End Time e Duration
    if id_json_path.exists():
        try:
            with open(id_json_path, 'r') as f:
                meta = json.load(f)
            
            pipeline_data = meta.get("Pipeline", {})
            start_str = pipeline_data.get("start_time")
            
            end_dt = datetime.datetime.now()
            pipeline_data["end_time"] = end_dt.isoformat()
            
            if start_str:
                try:
                    start_dt = datetime.datetime.fromisoformat(start_str)
                    delta = end_dt - start_dt
                    pipeline_data["elapsed_time"] = str(delta)
                except:
                    pipeline_data["elapsed_time"] = "Error parsing start_time"
            else:
                 pipeline_data["elapsed_time"] = "Unknown start_time"
                 
            meta["Pipeline"] = pipeline_data
            
            with open(id_json_path, 'w') as f:
                json.dump(meta, f, indent=2)
                
        except Exception as e:
            print(f"Erro atualizando tempo de pipeline: {e}")

    # 4. Mover input para pasta final (nii/)
    try:
        # Tentar ler ClinicalName do id.json
        final_name = case_id
        try:
            with open(case_output / "id.json", 'r') as f:
                idd = json.load(f)
                if "ClinicalName" in idd and idd["ClinicalName"] and idd["ClinicalName"] != "Unknown":
                    final_name = idd["ClinicalName"]
        except: pass
        
        final_nii_path = NII_DIR / f"{final_name}.nii.gz"
        shutil.move(str(nifti_path), str(final_nii_path))
        print(f"Input movido para: {final_nii_path}")
    except Exception as e:
        print(f"Erro ao mover input: {e}")

    return True

def main():
    print("Iniciando monitoramento da pasta input/ (Paralelo - 3 Casos)...")
    
    max_cases = 3
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_cases)
    
    processing_files = set()
    lock = threading.Lock()
    
    def on_complete(fut, f_path):
        with lock:
            if f_path in processing_files:
                processing_files.discard(f_path)
        try:
            fut.result()
        except Exception as e:
            print(f"Erro na thread do caso {f_path.name}: {e}")

    try:
        while True:
            try:
                # Listar arquivos
                current_files = sorted(list(INPUT_DIR.glob("*.nii.gz")))
                
                for f in current_files:
                    with lock:
                        # Se já estamos cheios, parar de submeter por agora
                        if len(processing_files) >= max_cases:
                            break
                        
                        # Se arquivo já está sendo processado, pular
                        if f in processing_files:
                            continue
                            
                        # Submeter novo caso
                        print(f"Submetendo novo caso: {f.name}")
                        processing_files.add(f)
                        future = executor.submit(process_case, f)
                        future.add_done_callback(lambda fut, p=f: on_complete(fut, p))
            
                time.sleep(2)
                
            except Exception as e:
                print(f"Erro no loop principal: {e}")
                time.sleep(2)
                
    except KeyboardInterrupt:
        print("\nParando monitoramento...")
        executor.shutdown(wait=False)
        print("Executor finalizado.")

if __name__ == "__main__":
    main()