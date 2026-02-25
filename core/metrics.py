#!/usr/bin/env python3
# Copyright (c) 2026 Rodrigo Americo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
        return None, None
    try:
        nii = nib.load(str(path))
        mask = nii.get_fdata()
        
        # Ensure mask and CT have matching dimensions
        if mask.shape != ct_data.shape:
            print(f"Shape mismatch: Mask {mask.shape} vs CT {ct_data.shape}")
            return None, None
        
        # Extract CT values where mask is positive
        voxels = ct_data[mask > 0]
        if voxels.size == 0:
            return None, None
            
        return round(float(np.mean(voxels)), 2), round(float(np.std(voxels)), 2)
        
    except Exception as ex:
        print(f"Error calculating HU for {path.name}: {ex}")
        return None, None

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
    
    # Determine modality and KVP from metadata
    id_json_path = case_output_folder / "id.json"
    modality = "CT"
    kvp_value = None
    kvp_raw = "Unknown"
    if id_json_path.exists():
        try:
            with open(id_json_path, 'r') as f:
                id_data = json.load(f)
                modality = id_data.get("Modality", "CT")
                kvp_raw = id_data.get("KVP", "Unknown")
                if kvp_raw is not None and kvp_raw != "Unknown":
                    try:
                        kvp_value = float(kvp_raw)
                    except ValueError:
                        pass
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
            
            # Liver PDFF calculation based on Pickhardt et al. (120 kVp)
            # Only calculate if organ is present (vol > 0.1 and hu_mean is not None)
            if organ_name == "liver" and vol > 0.1 and hu_mean is not None:
                # Calculate for all cases, and add a tag with the KVP
                pdff = -0.58 * hu_mean + 38.2
                pdff = max(0.0, min(100.0, pdff)) # Clamp to 0-100%
                results[f"{organ_name}_pdff_percent"] = round(pdff, 2)
                results[f"{organ_name}_pdff_kvp"] = kvp_raw
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

    # ============================================================
    # STEP 5: Bone Mineral Density (BMD) Analysis — L1 Vertebra
    # ============================================================
    # Opportunistic osteoporosis screening using CT Hounsfield Units
    # Method: 2D ROI on central axial slice of L1 (Pickhardt et al. 2013)
    #   > 160 HU  → Normal
    #   100–160 HU → Osteopenia
    #   < 100 HU  → Osteoporosis
    
    vertebra_L1_file = total_dir / "vertebrae_L1.nii.gz"
    
    if vertebra_L1_file.exists() and modality == "CT":
        try:
            from scipy.ndimage import binary_erosion
            
            nii_L1 = nib.load(str(vertebra_L1_file))
            mask_L1 = nii_L1.get_fdata() > 0
            spacing = nii_L1.header.get_zooms()  # (x_mm, y_mm, z_mm)
            
            if ct.shape != mask_L1.shape:
                print(f"Warning: Shape mismatch L1 {mask_L1.shape} vs CT {ct.shape}. Skipping BMD.")
            else:
                # Find central axial slice of L1
                z_indices = np.where(mask_L1.sum(axis=(0, 1)) > 0)[0]
                
                if len(z_indices) > 0:
                    center_z = int(z_indices[len(z_indices) // 2])
                    
                    # Extract 2D mask and CT at central slice
                    mask_2d = mask_L1[:, :, center_z]
                    ct_2d = ct[:, :, center_z]
                    
                    # 1. 2D erosion to find the vertebral body core (discard posterior elements)
                    in_plane_spacing = min(spacing[0], spacing[1])
                    erosion_iters = max(1, int(5.0 / in_plane_spacing))
                    
                    eroded_2d = binary_erosion(mask_2d, iterations=erosion_iters)
                    
                    # 2. Keep only the largest connected component (vertebral body)
                    from scipy.ndimage import label as ndlabel, center_of_mass
                    labeled, num_features = ndlabel(eroded_2d)
                    if num_features > 1:
                        component_sizes = [np.sum(labeled == i) for i in range(1, num_features + 1)]
                        largest = np.argmax(component_sizes) + 1
                        eroded_2d = (labeled == largest)
                    
                    if np.sum(eroded_2d) > 0:
                        # 3. Calculate centroids to determine anterior direction
                        full_com_x, full_com_y = center_of_mass(mask_2d)
                        body_com_x, body_com_y = center_of_mass(eroded_2d)
                        
                        # 4. Find bounding box of the vertebral body
                        x_indices, y_indices = np.where(eroded_2d)
                        x_min, x_max = x_indices.min(), x_indices.max()
                        y_min, y_max = y_indices.min(), y_indices.max()
                        
                        # Compare X vs Y variances to find the AP axis (usually Y in standard axial)
                        diff_x = abs(full_com_x - body_com_x)
                        diff_y = abs(full_com_y - body_com_y)
                        
                        # 5. Define oval radii (Increased size to match user visual preference)
                        # Diameter X = 70% of vertebral body width
                        # Diameter Y = 40% of vertebral body height
                        rx = (x_max - x_min) * 0.70 / 2.0
                        ry = (y_max - y_min) * 0.40 / 2.0
                        
                        if diff_y > diff_x:
                            # AP axis is Y. Opposite direction of full_com from body_com is Anterior.
                            anterior_is_larger_y = body_com_y > full_com_y
                            center_x = (x_min + x_max) / 2.0
                            
                            if anterior_is_larger_y:
                                # Offset from anterior edge (25% into the body)
                                center_y = y_max - (y_max - y_min) * 0.25
                            else:
                                center_y = y_min + (y_max - y_min) * 0.25
                        else:
                            # AP axis is X.
                            anterior_is_larger_x = body_com_x > full_com_x
                            center_y = (y_min + y_max) / 2.0
                            
                            if anterior_is_larger_x:
                                center_x = x_max - (x_max - x_min) * 0.25
                            else:
                                center_x = x_min + (x_max - x_min) * 0.25
                                
                        # 6. Generate the oval mask mathematically
                        y_grid, x_grid = np.ogrid[:mask_2d.shape[0], :mask_2d.shape[1]]
                        # Note: np.ogrid returns y_grid as (N, 1) and x_grid as (1, M)
                        # np.where(eroded_2d) treats dim 0 as X in our previous extraction, so let's match dimensions
                        # In numpy array indexing, dim 0 is rows (y_grid), dim 1 is cols (x_grid)
                        # Wait, x_indices, y_indices = np.where(eroded_2d) means dim 0 is x_indices, dim 1 is y_indices.
                        # So dim 0 (x) goes with np.ogrid row, dim 1 (y) goes with np.ogrid col.
                        x_grid_arr, y_grid_arr = np.ogrid[:mask_2d.shape[0], :mask_2d.shape[1]]
                        
                        ellipse = ((x_grid_arr - center_x)**2 / rx**2) + ((y_grid_arr - center_y)**2 / ry**2) <= 1
                        
                        # Strict intersection with eroded body to ensure no cortical bone is included
                        roi_mask = ellipse & eroded_2d
                        
                        trabecular_voxel_count = int(np.sum(roi_mask))
                        
                        if trabecular_voxel_count > 0:
                            trabecular_hu = ct_2d[roi_mask]
                            hu_mean = float(np.mean(trabecular_hu))
                            hu_std = float(np.std(trabecular_hu))
                            
                            # Pickhardt classification
                            if hu_mean > 160:
                                classification = "Normal"
                            elif hu_mean >= 100:
                                classification = "Osteopenia"
                            else:
                                classification = "Osteoporose"
                            
                            results["L1_trabecular_HU_mean"] = round(hu_mean, 2)
                            results["L1_trabecular_HU_std"] = round(hu_std, 2)
                            results["L1_trabecular_voxel_count"] = trabecular_voxel_count
                            results["L1_bmd_classification"] = classification
                            
                            # Generate zoomed overlay image
                            try:
                                import matplotlib
                                matplotlib.use('Agg')
                                import matplotlib.pyplot as plt
                                
                                ct_slice = np.rot90(ct_2d)
                                roi_slice = np.rot90(roi_mask)
                                mask_slice = np.rot90(mask_2d)
                                
                                # Crop to L1 bounding box with padding
                                rows = np.where(mask_slice.any(axis=1))[0]
                                cols = np.where(mask_slice.any(axis=0))[0]
                                
                                if len(rows) > 0 and len(cols) > 0:
                                    r_min_crop, r_max_crop = rows[0], rows[-1]
                                    c_min_crop, c_max_crop = cols[0], cols[-1]
                                    
                                    # Pad by ~0.8x vertebra size for anatomic context
                                    h = r_max_crop - r_min_crop
                                    w = c_max_crop - c_min_crop
                                    pad = int(max(h, w) * 0.8)
                                    
                                    r_min_crop = max(0, r_min_crop - pad)
                                    r_max_crop = min(ct_slice.shape[0], r_max_crop + pad)
                                    c_min_crop = max(0, c_min_crop - pad)
                                    c_max_crop = min(ct_slice.shape[1], c_max_crop + pad)
                                    
                                    ct_crop = ct_slice[r_min_crop:r_max_crop, c_min_crop:c_max_crop]
                                    roi_crop = roi_slice[r_min_crop:r_max_crop, c_min_crop:c_max_crop]
                                    hu_crop = np.where(roi_crop, ct_crop, np.nan)
                                    
                                    plt.figure(figsize=(6, 6))
                                    plt.imshow(ct_crop, cmap='gray', vmin=-250, vmax=1250)
                                    
                                    masked_hu = np.ma.masked_where(~roi_crop, hu_crop)
                                    
                                    # Use a clear colormap for the oval ROI
                                    plt.imshow(masked_hu, cmap='cool', alpha=0.8, vmin=0, vmax=300)
                                    
                                    plt.axis('off')
                                    plt.title(f"L1 BMD (Oval ROI) — {classification} ({hu_mean:.0f} HU)")
                                    plt.tight_layout()
                                    
                                    overlay_path = case_output_folder / "L1_BMD_overlay.png"
                                    plt.savefig(overlay_path, dpi=150, bbox_inches='tight')
                                    plt.close()
                                    
                            except Exception as plot_err:
                                print(f"Error generating L1 BMD overlay: {plot_err}")
                        else:
                            print("Warning: L1 oval ROI resulted in empty mask.")
                    else:
                        print("Warning: L1 erosion resulted in empty mask.")
                else:
                    print("Warning: L1 mask has no axial slices.")
                    
        except Exception as e:
            print(f"Error in BMD analysis: {e}")

    # ============================================================
    # STEP 6: Pulmonary Emphysema Quantification (Lobar)
    # ============================================================
    # Quantify emphysema using the "Density Mask" method (< -950 HU)
    # Calculated per lung lobe segmented by TotalSegmentator.
    # Metrics are only reported if all 5 lobes are successfully segmented.
    
    lung_lobes_map = [
        ("lung_upper_lobe_left", "lung_upper_lobe_left.nii.gz"),
        ("lung_lower_lobe_left", "lung_lower_lobe_left.nii.gz"),
        ("lung_upper_lobe_right", "lung_upper_lobe_right.nii.gz"),
        ("lung_middle_lobe_right", "lung_middle_lobe_right.nii.gz"),
        ("lung_lower_lobe_right", "lung_lower_lobe_right.nii.gz")
    ]
    
    # Step 6.1: Validation - Check for completeness
    # Ensure all lobe files exist and are not empty
    # In abdominal CTs, the lung bases are often segmented, which leads to partial lobes being considered "complete".
    # Therefore, we use minimum clinical volume thresholds (cm3) for each lobe to ensure the lung is actually complete.
    lobe_min_volumes_cm3 = {
        "lung_upper_lobe_left": 100.0,
        "lung_lower_lobe_left": 100.0,
        "lung_upper_lobe_right": 100.0,
        "lung_middle_lobe_right": 50.0,
        "lung_lower_lobe_right": 100.0
    }

    lobes_complete = True
    for lobe_key, filename in lung_lobes_map:
        fpath = total_dir / filename
        if not fpath.exists():
            lobes_complete = False
            break
        
        # Check if the volume is above the threshold
        vol_cm3 = get_volume_cm3(fpath)
        min_vol = lobe_min_volumes_cm3.get(lobe_key, 0)
        
        if vol_cm3 < min_vol:
            # print(f"Lobe {lobe_key} volume ({vol_cm3} cm³) is below the minimum threshold ({min_vol} cm³). Marking lungs as incomplete.")
            lobes_complete = False
            break
            
    if lobes_complete and modality == "CT":
        try:
            emphysema_results = {}
            total_lung_voxels = 0
            total_emphysema_voxels = 0
            
            # Get voxel volume in cm3 (1 voxel = pixdim[1]*pixdim[2]*pixdim[3] mm3)
            # 1 cm3 = 1000 mm3
            voxel_vol_mm3 = nii_lobe.header.get_zooms()[0] * nii_lobe.header.get_zooms()[1] * nii_lobe.header.get_zooms()[2]
            voxel_vol_cm3 = voxel_vol_mm3 / 1000.0
            
            for lobe_name, filename in lung_lobes_map:
                fpath = total_dir / filename
                nii_lobe = nib.load(str(fpath))
                mask_lobe = nii_lobe.get_fdata() > 0
                
                if mask_lobe.shape != ct.shape:
                    continue
                
                lobe_voxels = ct[mask_lobe]
                n_total = lobe_voxels.size
                
                if n_total > 0:
                    # -950 HU threshold for emphysema
                    n_emphysema = np.sum(lobe_voxels < -950)
                    perc = round((n_emphysema / n_total) * 100, 2)
                    
                    results[f"{lobe_name}_emphysema_percent"] = perc
                    results[f"{lobe_name}_vol_cm3"] = round(float(n_total * voxel_vol_cm3), 2)
                    results[f"{lobe_name}_emphysema_vol_cm3"] = round(float(n_emphysema * voxel_vol_cm3), 2)
                    results[f"{lobe_name}_total_voxels"] = int(n_total)
                    
                    total_lung_voxels += n_total
                    total_emphysema_voxels += n_emphysema
            
            # Global lung emphysema metric
            if total_lung_voxels > 0:
                global_perc = round((total_emphysema_voxels / total_lung_voxels) * 100, 2)
                results["total_lung_emphysema_percent"] = global_perc
                results["total_lung_vol_cm3"] = round(float(total_lung_voxels * voxel_vol_cm3), 2)
                results["total_lung_emphysema_vol_cm3"] = round(float(total_emphysema_voxels * voxel_vol_cm3), 2)
                results["lung_analysis_status"] = "Complete"
            
        except Exception as e:
            print(f"Error in emphysema analysis: {e}")
            results["lung_analysis_status"] = "Error"
    else:
        results["lung_analysis_status"] = "Incomplete or non-CT"
        if modality == "CT":
             print("Warning: Skipping emphysema analysis due to incomplete lung lobe masks.")

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
