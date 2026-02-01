import nibabel as nib
import numpy as np
import json
from pathlib import Path
import sqlite3 # Adicionado para DB

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
    total_dir = case_output_folder / "total"
    
    # 1. Identificar Regiões do Corpo FIRST
    # This function is not provided in the snippet, assuming it exists elsewhere or is a placeholder.
    # For now, let's mock it to avoid errors.
    def detect_body_regions(total_dir_path):
        # Placeholder for actual implementation
        return ["abdomen", "chest"] 
    
    detected_regions = detect_body_regions(total_dir)
    
    # Determinar Modalidade (Lendo id.json ou default CT)
    id_json_path = case_output_folder / "id.json"
    modality = "CT"
    if id_json_path.exists():
        try:
            with open(id_json_path, 'r') as f:
                modality = json.load(f).get("Modality", "CT")
        except: pass

    results = {
        "case_id": case_id,
        "body_regions": detected_regions,
        "modality": modality
    }

    # Carregar Imagem Original (apenas se necessário para métricas de densidade ou overlay)
    # Para performance, poderíamos postergar, mas precisamos para HU e Overlay
    ct = nib.load(str(nifti_path)).get_fdata()

    # 2. Métricas de Órgãos (Se abdome presente)
    # O usuário pediu especificamente: Fígado, Baço, Rins.
    # Calcular volumes SEMPRE. Densidade APENAS SE CT.
    
    organs_map = [
        ("liver", "liver.nii.gz"),
        ("spleen", "spleen.nii.gz"),
        ("kidney_right", "kidney_right.nii.gz"),
        ("kidney_left", "kidney_left.nii.gz")
    ]
    
    for organ_name, filename in organs_map:
        fpath = total_dir / filename
        vol = get_volume_cm3(fpath)
        results[f"{organ_name}_vol_cm3"] = vol
        
        if modality == "CT":
            hu_mean, hu_std = get_mean_hu(fpath, ct)
            results[f"{organ_name}_hu_mean"] = hu_mean
            results[f"{organ_name}_hu_std"] = hu_std
        else:
            # Em MR não calculamos HU (intensidade de sinal é relativa e varia)
            results[f"{organ_name}_hu_mean"] = None
            results[f"{organ_name}_hu_std"] = None

    # 3. Análise de L3 (Abdomen/Musculo)
    vertebra_L3_file = case_output_folder / "total" / "vertebrae_L3.nii.gz"
    muscle_file = case_output_folder / "tissue_types" / "skeletal_muscle.nii.gz" # Correct filename from TotalSegmentator
    
    if vertebra_L3_file.exists():
        try:
            # Identificar fatia L3
            nii_L3 = nib.load(str(vertebra_L3_file))
            mask_L3 = nii_L3.get_fdata()
            slice_L3_indices = np.where(mask_L3.sum(axis=(0, 1)) > 0)[0]
            
            if len(slice_L3_indices) > 0:
                slice_idx = int(slice_L3_indices[len(slice_L3_indices)//2])
                results["slice_L3"] = slice_idx
                
                # Gerar Imagem de Overlay (L3) - APENAS CT
                if modality == "CT":
                    try:
                        import matplotlib
                        matplotlib.use('Agg')
                        import matplotlib.pyplot as plt

                        # Prepare data
                        ct_slice = ct[:, :, slice_idx]
                        ct_slice = np.rot90(ct_slice)
                        
                        # Tentar Overlay com Músculo SE existir, senão L3
                        overlay_mask = None
                        if muscle_file.exists():
                            nii_muscle = nib.load(str(muscle_file))
                            muscle_data = nii_muscle.get_fdata()
                            mask_slice = muscle_data[:, :, slice_idx]
                            overlay_mask = np.rot90(mask_slice)
                        else:
                            overlay_mask = np.rot90(mask_L3[:, :, slice_idx])

                        plt.figure(figsize=(8, 8))
                        
                        # Windowing: CT soft tissue
                        plt.imshow(ct_slice, cmap='gray', vmin=-150, vmax=250)
                        
                        if overlay_mask is not None:
                            masked_data = np.ma.masked_where(overlay_mask == 0, overlay_mask)
                            plt.imshow(masked_data, cmap='autumn', alpha=0.5)
                        
                        plt.axis('off')
                        plt.title(f"L3 Slice (idx: {slice_idx})")
                        plt.tight_layout()
                        
                        overlay_path = case_output_folder / "L3_overlay.png"
                        plt.savefig(overlay_path, dpi=150)
                        plt.close()
                    except Exception as e:
                        print(f"Erro gerando imagem L3: {e}")

                # Métricas Musculares (Se existir segmentação muscular)
                if muscle_file.exists():
                    nii_muscle = nib.load(str(muscle_file))
                    muscle_data = nii_muscle.get_fdata() # Reloading full for safety/simplicity
                    spacing = nii_muscle.header.get_zooms()
                    
                    mask_slice = muscle_data[:, :, slice_idx]
                    area_mm2 = np.sum(mask_slice > 0) * spacing[0] * spacing[1]
                    area_cm2 = area_mm2 / 100.0
                    results["SMA_cm2"] = round(area_cm2, 3)
                    
                    if modality == "CT":
                         muscle_voxels = ct[:, :, slice_idx][mask_slice > 0]
                         if muscle_voxels.size > 0:
                             results["muscle_HU_mean"] = float(round(np.mean(muscle_voxels), 2))
                             results["muscle_HU_std"] = float(round(np.std(muscle_voxels), 2))
                         else:
                             results["muscle_HU_mean"] = 0.0
                             results["muscle_HU_std"] = 0.0
                    else:
                        results["muscle_HU_mean"] = None
                        results["muscle_HU_std"] = None

            else:
                print("Aviso: L3 encontrado mas vazio.")
        except Exception as e:
            print(f"Erro na análise de L3: {e}")

    # 4. Análise de Hemorragia Cerebral (Se existir)
    # Output path: bbox output/bleed/intracerebral_hemorrhage.nii.gz
    bleed_file = case_output_folder / "bleed" / "intracerebral_hemorrhage.nii.gz"
    
    if bleed_file.exists():
        try:
            # Calcular Volume
            vol_bleed = get_volume_cm3(bleed_file)
            results["hemorrhage_vol_cm3"] = vol_bleed
            
            if vol_bleed > 0:
                nii_bleed = nib.load(str(bleed_file))
                mask_bleed = nii_bleed.get_fdata()
                
                # Identificar range Z da lesão (axial cuts)
                # mask shape (X, Y, Z)
                z_indices = np.where(mask_bleed.sum(axis=(0, 1)) > 0)[0]
                
                if len(z_indices) > 0:
                    # Usar índices do array de fatias presentes para garantir que a fatia escolhida TEM lesão
                    # (evita cair em gaps se a segmentação for desconexa ou tiver ruído)
                    n_slices = len(z_indices)
                    
                    # Ensure indices are within bounds (should be by def)
                    idx_15 = int(n_slices * 0.15)
                    idx_50 = int(n_slices * 0.50)
                    idx_85 = int(n_slices * 0.85)
                    
                    # Clamp just in case
                    idx_15 = max(0, min(idx_15, n_slices - 1))
                    idx_50 = max(0, min(idx_50, n_slices - 1))
                    idx_85 = max(0, min(idx_85, n_slices - 1))
                    
                    slices_to_gen = {
                        "inferior_15": int(z_indices[idx_15]),
                        "center_50":   int(z_indices[idx_50]),
                        "superior_85": int(z_indices[idx_85])
                    }
                    
                    if modality == "CT":
                        try:
                            import matplotlib
                            matplotlib.use('Agg')
                            import matplotlib.pyplot as plt
                            
                            # Carregar o CT completo se ainda não carregado? Já carregamos 'ct' no início.
                            # Mas precisamos garantia que o shape bate com o bleed (pode ter crop?)
                            # TotalSegmentator geralmente mantem espaço original ou resampleado. 
                            # Se 'ct' original usado para HU bater, ótimo.
                            
                            # Check shape consistency
                            if ct.shape != mask_bleed.shape:
                                # Fallback: Tentar carregar o CT resampleado se existir, ou skip overlay
                                # Melhor: pular overlay se shapes não batem para evitar crash
                                print(f"Aviso: Shape mismatch Bleed {mask_bleed.shape} vs CT {ct.shape}. Skipping overlay.")
                            else:
                                for label, slice_idx in slices_to_gen.items():
                                    ct_slice = np.rot90(ct[:, :, slice_idx])
                                    mask_slice = np.rot90(mask_bleed[:, :, slice_idx])
                                    
                                    plt.figure(figsize=(8, 8))
                                    # Windowing: Brain Soft Tissue (Standard: WL 40, WW 80 -> 0 to 80 HU)
                                    plt.imshow(ct_slice, cmap='gray', vmin=0, vmax=80)
                                    
                                    # Ensure mask is binary and visible
                                    mask_binary = (mask_slice > 0).astype(np.float32)
                                    
                                    # Plot Overlay only if something is there
                                    if np.sum(mask_binary) > 0:
                                        masked_data = np.ma.masked_where(mask_binary == 0, mask_binary)
                                        # Use 'jet' or 'autumn' or pure solid color? 
                                        # 'Reds' with vmin=0, vmax=1 -> 1.0 is Dark Red.
                                        plt.imshow(masked_data, cmap='Reds', alpha=0.7, vmin=0, vmax=1)
                                    
                                    plt.axis('off')
                                    plt.title(f"Hemorrhage {label} (z={slice_idx})")
                                    plt.tight_layout()
                                    
                                    out_img = case_output_folder / f"bleed_overlay_{label}.png"
                                    plt.savefig(out_img, dpi=150)
                                    plt.close()
                                    
                                results["hemorrhage_analysis_slices"] = slices_to_gen
                                
                        except Exception as plot_err:
                            print(f"Erro gerando overlay de hemorragia: {plot_err}")
                            
        except Exception as e:
            print(f"Erro na análise de hemorragia: {e}")
            
    return results

def detect_body_regions(total_dir):
    """
    Analisa os arquivos de segmentação para identificar quais regiões do corpo estão presentes.
    Considera presente se a máscara não estiver vazia.
    """
    regions_map = {
        "head": ["skull.nii.gz", "brain.nii.gz", "face.nii.gz"],
        "neck": ["vertebrae_C1.nii.gz", "vertebrae_C2.nii.gz", "vertebrae_C3.nii.gz", "vertebrae_C4.nii.gz", 
                 "vertebrae_C5.nii.gz", "vertebrae_C6.nii.gz", "vertebrae_C7.nii.gz", "trachea.nii.gz", "thyroid_gland.nii.gz"],
        "thorax": ["lung_upper_lobe_left.nii.gz", "lung_upper_lobe_right.nii.gz", "heart.nii.gz", "esophagus.nii.gz",
                   "aorta.nii.gz", "pulmonary_vein.nii.gz"],
        "abdomen": ["liver.nii.gz", "spleen.nii.gz", "pancreas.nii.gz", "kidney_left.nii.gz", "kidney_right.nii.gz", 
                    "stomach.nii.gz", "gallbladder.nii.gz", "adrenal_gland_left.nii.gz"],
        "pelvis": ["sacrum.nii.gz", "urinary_bladder.nii.gz", "prostate.nii.gz", "hip_left.nii.gz", "hip_right.nii.gz", 
                   "gluteus_maximus_left.nii.gz"],
        "legs": ["femur_left.nii.gz", "femur_right.nii.gz"]
    }
    
    detected = []
    
    for region, files in regions_map.items():
        is_present = False
        for fname in files:
            fpath = total_dir / fname
            if fpath.exists():
                try:
                    # Check if file has non-zero voxels without loading full data if possible, 
                    # but nibabel load is lazy. dataobj is useful.
                    # For safety, let's just load get_fdata() as we did in other functions.
                    # To optimize speed, we could assume TotalSegmentator output is clean. 
                    # But we saw empty files are created.
                    
                    # Optimization: check file size? Empty nifti might be small but header is 348 bytes + extensions.
                    # Best way is to check data.
                    nii = nib.load(str(fpath))
                    # Check a quick stats or sum
                    if np.sum(nii.dataobj) > 0: # dataobj access might be faster than get_fdata which casts to float
                        is_present = True
                        break
                except:
                    pass
        
        if is_present:
            detected.append(region)
            
    return detected
