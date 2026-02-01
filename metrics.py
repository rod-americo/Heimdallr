#!/usr/bin/env python3
"""
Heimdallr Metrics Calculation Module (metrics.py)

Calculates clinical metrics from segmentation masks:
- Organ volumes (liver, spleen, kidneys)
- Hounsfield Unit densities (CT only)
- L3 sarcopenia metrics (Skeletal Muscle Area, muscle HU)
- Cerebral hemorrhage quantification
- Overlay image generation
"""

import nibabel as nib
import numpy as np
import json
from pathlib import Path
import sqlite3

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
    """
    Calculate mean and standard deviation of Hounsfield Units within a mask.
    
    Args:
        path: Path to NIfTI mask file
        ct_data: Full CT numpy array
    
    Returns:
        tuple: (mean_HU, std_HU) both rounded to 2 decimal places
    """
    if not path.exists():
        return 0.0, 0.0
    try:
        nii = nib.load(str(path))
        mask = nii.get_fdata()
        
        # Ensure mask and CT have matching dimensions
        if mask.shape != ct_data.shape:
            print(f"Shape mismatch: Mask {mask.shape} vs CT {ct_data.shape}")
            return 0.0, 0.0
        
        # Extract CT values where mask is positive
        voxels = ct_data[mask > 0]
        if voxels.size == 0:
            return 0.0, 0.0
            
        return round(float(np.mean(voxels)), 2), round(float(np.std(voxels)), 2)
        
    except Exception as ex:
        print(f"Error calculating HU for {path.name}: {ex}")
        return 0.0, 0.0

def calculate_all_metrics(case_id, nifti_path, case_output_folder):
    """
    Calculate all clinical metrics for a case.
    
    Performs:
    1. Body region detection
    2. Organ volumetry (liver, spleen, kidneys)
    3. Hounsfield Unit density analysis (CT only)
    4. L3 sarcopenia analysis (SMA, muscle HU)
    5. Cerebral hemorrhage quantification
    
    Args:
        case_id: Patient identifier
        nifti_path: Path to original NIfTI file
        case_output_folder: Output directory with segmentation results
    
    Returns:
        dict: All calculated metrics
    """
    total_dir = case_output_folder / "total"
    
    # ============================================================
    # STEP 1: Detect Body Regions
    # ============================================================
    detected_regions = detect_body_regions(total_dir)
    
    # Determine modality from metadata (default to CT)
    id_json_path = case_output_folder / "id.json"
    modality = "CT"
    if id_json_path.exists():
        try:
            with open(id_json_path, 'r') as f:
                modality = json.load(f).get("Modality", "CT")
        except: 
            pass

    results = {
        "case_id": case_id,
        "body_regions": detected_regions,
        "modality": modality
    }

    # Load original image for density calculation and overlay generation
    ct = nib.load(str(nifti_path)).get_fdata()

    # ============================================================
    # STEP 2: Abdominal Organ Metrics
    # ============================================================
    # Calculate volume for all modalities, density only for CT
    # Organs: liver, spleen, both kidneys
    
    organs_map = [
        ("liver", "liver.nii.gz"),
        ("spleen", "spleen.nii.gz"),
        ("kidney_right", "kidney_right.nii.gz"),
        ("kidney_left", "kidney_left.nii.gz")
    ]
    
    for organ_name, filename in organs_map:
        fpath = total_dir / filename
        
        # Volume is calculated for all modalities (CT and MR)
        vol = get_volume_cm3(fpath)
        results[f"{organ_name}_vol_cm3"] = vol
        
        # Hounsfield Units only for CT (not applicable to MR)
        if modality == "CT":
            hu_mean, hu_std = get_mean_hu(fpath, ct)
            results[f"{organ_name}_hu_mean"] = hu_mean
            results[f"{organ_name}_hu_std"] = hu_std
        else:
            # MR: signal intensity varies by sequence, not standardized like HU
            results[f"{organ_name}_hu_mean"] = None
            results[f"{organ_name}_hu_std"] = None

 # ============================================================
    # STEP 3: L3 Sarcopenia Analysis
    # ============================================================
    # Calculate Skeletal Muscle Area (SMA) and muscle density at L3 vertebra
    # L3 is a standard landmark for body composition assessment
    
    vertebra_L3_file = case_output_folder / "total" / "vertebrae_L3.nii.gz"
    muscle_file = case_output_folder / "tissue_types" / "skeletal_muscle.nii.gz"
    
    if vertebra_L3_file.exists():
        try:
            # Find L3 vertebra slice
            nii_L3 = nib.load(str(vertebra_L3_file))
            mask_L3 = nii_L3.get_fdata()
            
            # Find axial slices containing L3
            slice_L3_indices = np.where(mask_L3.sum(axis=(0, 1)) > 0)[0]
            
            if len(slice_L3_indices) > 0:
                # Use middle slice of L3 vertebra
                slice_idx = int(slice_L3_indices[len(slice_L3_indices)//2])
                results["slice_L3"] = slice_idx
                
                # Generate L3 overlay image (CT only)
                if modality == "CT":
                    try:
                        import matplotlib
                        matplotlib.use('Agg')  # Non-interactive backend
                        import matplotlib.pyplot as plt

                        # Prepare CT slice
                        ct_slice = ct[:, :, slice_idx]
                        ct_slice = np.rot90(ct_slice)
                        
                        # Overlay with muscle mask if available, otherwise L3 mask
                        overlay_mask = None
                        if muscle_file.exists():
                            nii_muscle = nib.load(str(muscle_file))
                            muscle_data = nii_muscle.get_fdata()
                            mask_slice = muscle_data[:, :, slice_idx]
                            overlay_mask = np.rot90(mask_slice)
                        else:
                            overlay_mask = np.rot90(mask_L3[:, :, slice_idx])

                        plt.figure(figsize=(8, 8))
                        
                        # CT windowing for soft tissue (abdomen)
                        plt.imshow(ct_slice, cmap='gray', vmin=-150, vmax=250)
                        
                        # Overlay muscle mask
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
                        print(f"Error generating L3 overlay image: {e}")

                # Calculate muscle metrics if segmentation exists
                if muscle_file.exists():
                    nii_muscle = nib.load(str(muscle_file))
                    muscle_data = nii_muscle.get_fdata()
                    spacing = nii_muscle.header.get_zooms()
                    
                    # Calculate Skeletal Muscle Area (SMA) at L3
                    mask_slice = muscle_data[:, :, slice_idx]
                    area_mm2 = np.sum(mask_slice > 0) * spacing[0] * spacing[1]
                    area_cm2 = area_mm2 / 100.0
                    results["SMA_cm2"] = round(area_cm2, 3)
                    
                    # Calculate muscle Hounsfield Units (CT only)
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
                print("Warning: L3 vertebra found but mask is empty.")
                
        except Exception as e:
            print(f"Error in L3 analysis: {e}")

    # ============================================================
    # STEP 4: Cerebral Hemorrhage Analysis
    # ============================================================
    # Quantify intracranial bleeding if detected
    bleed_file = case_output_folder / "bleed" / "intracerebral_hemorrhage.nii.gz"
    
    if bleed_file.exists():
        try:
            # Calculate hemorrhage volume
            vol_bleed = get_volume_cm3(bleed_file)
            results["hemorrhage_vol_cm3"] = vol_bleed
            
            if vol_bleed > 0:
                nii_bleed = nib.load(str(bleed_file))
                mask_bleed = nii_bleed.get_fdata()
                
                # Find axial slices containing hemorrhage
                z_indices = np.where(mask_bleed.sum(axis=(0, 1)) > 0)[0]
                
                if len(z_indices) > 0:
                    # Select representative slices (inferior 15%, center 50%, superior 85%)
                    n_slices = len(z_indices)
                    
                    idx_15 = int(n_slices * 0.15)
                    idx_50 = int(n_slices * 0.50)
                    idx_85 = int(n_slices * 0.85)
                    
                    # Clamp to valid range
                    idx_15 = max(0, min(idx_15, n_slices - 1))
                    idx_50 = max(0, min(idx_50, n_slices - 1))
                    idx_85 = max(0, min(idx_85, n_slices - 1))
                    
                    slices_to_gen = {
                        "inferior_15": int(z_indices[idx_15]),
                        "center_50":   int(z_indices[idx_50]),
                        "superior_85": int(z_indices[idx_85])
                    }
                    
                    # Generate overlay images (CT only)
                    if modality == "CT":
                        try:
                            import matplotlib
                            matplotlib.use('Agg')
                            import matplotlib.pyplot as plt
                            
                            # Verify CT and hemorrhage mask have matching dimensions
                            if ct.shape != mask_bleed.shape:
                                print(f"Warning: Shape mismatch Bleed {mask_bleed.shape} vs CT {ct.shape}. Skipping overlay.")
                            else:
                                # Generate overlay for each representative slice
                                for label, slice_idx in slices_to_gen.items():
                                    ct_slice = np.rot90(ct[:, :, slice_idx])
                                    mask_slice = np.rot90(mask_bleed[:, :, slice_idx])
                                    
                                    plt.figure(figsize=(8, 8))
                                    
                                    # Brain CT windowing (WL=40, WW=80)
                                    plt.imshow(ct_slice, cmap='gray', vmin=0, vmax=80)
                                    
                                    # Overlay hemorrhage in red
                                    mask_binary = (mask_slice > 0).astype(np.float32)
                                    
                                    if np.sum(mask_binary) > 0:
                                        masked_data = np.ma.masked_where(mask_binary == 0, mask_binary)
                                        plt.imshow(masked_data, cmap='Reds', alpha=0.7, vmin=0, vmax=1)
                                    
                                    plt.axis('off')
                                    plt.title(f"Hemorrhage {label} (z={slice_idx})")
                                    plt.tight_layout()
                                    
                                    out_img = case_output_folder / f"bleed_overlay_{label}.png"
                                    plt.savefig(out_img, dpi=150)
                                    plt.close()
                                    
                                results["hemorrhage_analysis_slices"] = slices_to_gen
                                
                        except Exception as plot_err:
                            print(f"Error generating hemorrhage overlay: {plot_err}")
                            
        except Exception as e:
            print(f"Error in hemorrhage analysis: {e}")
            
    return results

def detect_body_regions(total_dir):
    """
    Analyze segmentation files to identify which body regions are present.
    
    Regions are detected based on the presence of anatomical structures:
    - head: skull, brain, face
    - neck: cervical vertebrae, trachea, thyroid
    - thorax: lungs, heart, aorta
    - abdomen: liver, spleen, pancreas, kidneys
    - pelvis: sacrum, bladder, hips
    - legs: femurs
    
    Args:
        total_dir: Directory containing TotalSegmentator output masks
    
    Returns:
        list: Detected body region names
    """
    # Map regions to their characteristic anatomical structures
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
        
        # Check if any characteristic structure for this region exists and is non-empty
        for fname in files:
            fpath = total_dir / fname
            if fpath.exists():
                try:
                    # Load mask and check if it contains any segmented voxels
                    # Using dataobj for efficiency (lazy loading)
                    nii = nib.load(str(fpath))
                    if np.sum(nii.dataobj) > 0:
                        is_present = True
                        break  # Region confirmed, no need to check other files
                except:
                    pass  # Skip files that can't be loaded
        
        if is_present:
            detected.append(region)
            
    return detected
