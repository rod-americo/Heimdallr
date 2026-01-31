import nibabel as nib
import numpy as np
from pathlib import Path

def get_volume_cm3(path):
    """Calcula o volume em cm³ de uma máscara NIfTI."""
    if not path.exists():
        return 0.0
    try:
        nii = nib.load(str(path))
        data = nii.get_fdata()
        zooms = nii.header.get_zooms()
        # Volume do voxel em mm³
        voxel_vol_mm3 = zooms[0] * zooms[1] * zooms[2]
        # Soma váriáveis e converte para cm³
        vol_mm3 = np.sum(data > 0) * voxel_vol_mm3
        return round(vol_mm3 / 1000.0, 3)
    except Exception as ex:
        print(f"Erro calculando volume {path.name}: {ex}")
        return 0.0

def get_mean_hu(path, ct_data):
    """Calcula média e desvio padrão de HU dentro da máscara."""
    if not path.exists():
        return 0.0, 0.0
    try:
        nii = nib.load(str(path))
        mask = nii.get_fdata()
        
        # Ensure mask matches CT shape
        if mask.shape != ct_data.shape:
            print(f"Shape mismatch: Mask {mask.shape} vs CT {ct_data.shape}")
            return 0.0, 0.0
        
        voxels = ct_data[mask > 0]
        if voxels.size == 0:
            return 0.0, 0.0
            
        return round(float(np.mean(voxels)), 2), round(float(np.std(voxels)), 2)
    except Exception as ex:
        print(f"Erro calculando HU {path.name}: {ex}")
        return 0.0, 0.0

def calculate_all_metrics(case_id, nifti_path, case_output_folder):
    """
    Realiza todos os cálculos de métricas para um caso.
    Retorna um dicionário com os resultados.
    """
    vertebra_L3_file = case_output_folder / "total" / "vertebrae_L3.nii.gz"
    muscle_file      = case_output_folder / "tissue_types" / "skeletal_muscle.nii.gz"
    total_dir        = case_output_folder / "total"

    if not vertebra_L3_file.exists():
         raise FileNotFoundError("Segmentação de L3 falhou (arquivo não encontrado).")

    # Carregar TC Original
    ct = nib.load(str(nifti_path)).get_fdata()
    
    # 1. Detectar fatia L3
    nii_L3 = nib.load(str(vertebra_L3_file))
    mask_L3 = nii_L3.get_fdata()
    
    slice_L3_indices = np.where(mask_L3.sum(axis=(0, 1)) > 0)[0]
    if len(slice_L3_indices) == 0:
        raise ValueError("nível L3 não encontrado na máscara.")
    
    slice_idx = int(slice_L3_indices[len(slice_L3_indices)//2])
    
    # 2. Área Muscular (SMA) na fatia L3
    if not muscle_file.exists():
         raise FileNotFoundError("Segmentação muscular falhou.")

    nii_muscle = nib.load(str(muscle_file))
    muscle_data = nii_muscle.get_fdata()
    spacing = nii_muscle.header.get_zooms()
    
    mask_slice = muscle_data[:, :, slice_idx]
    
    area_mm2 = np.sum(mask_slice > 0) * spacing[0] * spacing[1]
    area_cm2 = area_mm2 / 100.0
    
    # 3. HU Muscular na fatia L3
    muscle_voxels = ct[:, :, slice_idx][mask_slice > 0]
    if muscle_voxels.size > 0:
        muscle_HU_mean = float(np.mean(muscle_voxels))
        muscle_HU_std = float(np.std(muscle_voxels))
    else:
        muscle_HU_mean = 0.0
        muscle_HU_std = 0.0

    # 4. Volumes de Órgãos
    vol_liver    = get_volume_cm3(total_dir / "liver.nii.gz")
    vol_spleen   = get_volume_cm3(total_dir / "spleen.nii.gz")
    vol_kidney_r = get_volume_cm3(total_dir / "kidney_right.nii.gz")
    vol_kidney_l = get_volume_cm3(total_dir / "kidney_left.nii.gz")

    # 5. Atenuação do Fígado
    liver_hu_mean, liver_hu_std = get_mean_hu(total_dir / "liver.nii.gz", ct)

    return {
        "case_id": case_id,
        "slice_L3": slice_idx,
        "SMA_cm2": round(area_cm2, 3),
        "muscle_HU_mean": round(muscle_HU_mean, 3),
        "muscle_HU_std": round(muscle_HU_std, 3),
        "liver_vol_cm3": vol_liver,
        "liver_HU_mean": liver_hu_mean,
        "liver_HU_std": liver_hu_std,
        "spleen_vol_cm3": vol_spleen,
        "kidney_right_vol_cm3": vol_kidney_r,
        "kidney_left_vol_cm3": vol_kidney_l
    }
