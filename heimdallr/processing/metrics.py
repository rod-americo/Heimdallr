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
from scipy.ndimage import label as ndlabel

from heimdallr.shared.paths import study_id_json

def get_volume_cm3(path):
    """Calcula o volume em cm³ de uma máscara NIfTI."""
    if not path.exists():
        return 0.0
    try:
        nii = nib.load(str(path))
        zooms = nii.header.get_zooms()
        # Volume do voxel em mm³
        voxel_vol_mm3 = zooms[0] * zooms[1] * zooms[2]
        # Soma váriáveis e converte para cm³
        vol_mm3 = np.count_nonzero(np.asanyarray(nii.dataobj)) * voxel_vol_mm3
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
        mask = np.asanyarray(nii.dataobj) > 0
        
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

def find_first_existing_path(base_dir, relative_paths):
    """Return the first existing path from a list of relative candidates."""
    for rel_path in relative_paths:
        candidate = base_dir / rel_path
        if candidate.exists():
            return candidate
    return None

def get_mask_mean_std(mask_data, ct_data):
    """Calculate mean and standard deviation for CT voxels inside a binary mask."""
    if mask_data.shape != ct_data.shape:
        print(f"Shape mismatch: Mask {mask_data.shape} vs CT {ct_data.shape}")
        return None, None

    voxels = ct_data[mask_data]
    if voxels.size == 0:
        return None, None

    return round(float(np.mean(voxels)), 2), round(float(np.std(voxels)), 2)

def load_binary_mask(path):
    """Load a NIfTI file and return a boolean mask plus the loaded image."""
    nii = nib.load(str(path))
    return np.asanyarray(nii.dataobj) > 0, nii

def get_mask_max_euclidean_diameter_mm(mask_data, affine):
    """
    Return a stable major-axis diameter estimate in millimeters.

    The previous implementation used an exact all-pairs distance (`pdist`) over
    every positive voxel, which is O(n^2) in memory/time and can terminate the
    process for large organs. Projecting points onto the principal axis keeps
    the metric clinically useful while remaining linear in the number of voxels.
    """
    mask_bool = np.asarray(mask_data) > 0
    coords_ijk = np.argwhere(mask_bool)
    if coords_ijk.shape[0] < 2:
        return 0.0

    coords_xyz = nib.affines.apply_affine(affine, coords_ijk).astype(np.float32, copy=False)
    centered = coords_xyz - coords_xyz.mean(axis=0, keepdims=True)

    try:
        _, _, vh = np.linalg.svd(centered, full_matrices=False)
    except np.linalg.LinAlgError as ex:
        print(f"Warning: Falling back to bounding-box diameter estimate: {ex}")
        mins = coords_xyz.min(axis=0)
        maxs = coords_xyz.max(axis=0)
        return round(float(np.linalg.norm(maxs - mins)), 2)

    primary_axis = vh[0]
    projected = centered @ primary_axis
    return round(float(projected.max() - projected.min()), 2)

def calculate_stone_mask_metrics(mask_data, spacing_mm, ct_data=None):
    """
    Calculate stone burden metrics from a binary segmentation mask.

    Returns:
        dict: count, total volume, largest axis-aligned diameter, and HU stats.
    """
    metrics = {
        "count": 0,
        "total_volume_mm3": 0.0,
        "total_volume_cm3": 0.0,
        "largest_diameter_mm": 0.0,
        "hu_mean": None,
        "hu_std": None
    }

    mask_bool = np.asarray(mask_data) > 0
    if not np.any(mask_bool):
        return metrics

    voxel_volume_mm3 = float(spacing_mm[0] * spacing_mm[1] * spacing_mm[2])
    total_voxels = int(np.sum(mask_bool))
    total_volume_mm3 = total_voxels * voxel_volume_mm3

    structure = np.ones((3, 3, 3), dtype=np.uint8)
    labeled_mask, num_components = ndlabel(mask_bool, structure=structure)

    largest_diameter_mm = 0.0
    for component_id in range(1, num_components + 1):
        coords = np.argwhere(labeled_mask == component_id)
        if coords.size == 0:
            continue

        spans_vox = coords.max(axis=0) - coords.min(axis=0) + 1
        spans_mm = spans_vox * np.asarray(spacing_mm[:3], dtype=np.float64)
        component_diameter_mm = float(np.max(spans_mm))
        largest_diameter_mm = max(largest_diameter_mm, component_diameter_mm)

    metrics["count"] = int(num_components)
    metrics["total_volume_mm3"] = round(total_volume_mm3, 2)
    metrics["total_volume_cm3"] = round(total_volume_mm3 / 1000.0, 3)
    metrics["largest_diameter_mm"] = round(largest_diameter_mm, 2)

    if ct_data is not None:
        hu_mean, hu_std = get_mask_mean_std(mask_bool, ct_data)
        metrics["hu_mean"] = hu_mean
        metrics["hu_std"] = hu_std

    return metrics

def is_mask_axially_complete(mask_data):
    """
    A structure is considered axially complete only if it does not touch the
    first or last slice of the volume.
    """
    mask_bool = np.asarray(mask_data) > 0
    if mask_bool.ndim != 3 or not np.any(mask_bool):
        return False

    z_indices = np.where(mask_bool.sum(axis=(0, 1)) > 0)[0]
    if len(z_indices) == 0:
        return False

    return int(z_indices[0]) > 0 and int(z_indices[-1]) < (mask_bool.shape[2] - 1)

def get_structure_completeness(path):
    """
    Return completeness metadata for a segmentation file.

    Returns:
        tuple: (status, mask_bool, nii)
        status in {"Not available", "Empty", "Incomplete", "Complete", "Error"}
    """
    if not path.exists():
        return "Not available", None, None

    try:
        mask_data, nii = load_binary_mask(path)
        if not np.any(mask_data):
            return "Empty", mask_data, nii
        if not is_mask_axially_complete(mask_data):
            return "Incomplete", mask_data, nii
        return "Complete", mask_data, nii
    except Exception as ex:
        print(f"Error evaluating completeness for {path.name}: {ex}")
        return "Error", None, None

def calculate_all_metrics(case_id, nifti_path, case_output_folder, generate_overlays=True):
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
    total_dir = case_output_folder / "artifacts" / "total"
    
    # ============================================================
    # STEP 1: Detect Body Regions
    # ============================================================
    detected_regions = detect_body_regions(total_dir)
    
    # Determine modality and KVP from metadata
    id_json_path = study_id_json(case_id)
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

    # Load original image for density calculation and optional overlay generation
    ct = nib.load(str(nifti_path)).get_fdata(dtype=np.float32)

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
        status, organ_mask, organ_nii = get_structure_completeness(fpath)
        results[f"{organ_name}_analysis_status"] = status
        results[f"{organ_name}_complete"] = (status == "Complete")
        if organ_name in {"kidney_right", "kidney_left"}:
            results[f"{organ_name}_max_diameter_mm"] = None

        if status == "Complete":
            zooms = organ_nii.header.get_zooms()
            voxel_vol_mm3 = zooms[0] * zooms[1] * zooms[2]
            vol_mm3 = np.sum(organ_mask) * voxel_vol_mm3
            vol = round(vol_mm3 / 1000.0, 3)
            results[f"{organ_name}_vol_cm3"] = vol
            if organ_name in {"kidney_right", "kidney_left"}:
                results[f"{organ_name}_max_diameter_mm"] = get_mask_max_euclidean_diameter_mm(organ_mask, organ_nii.affine)
        else:
            results[f"{organ_name}_vol_cm3"] = None

        # Liver attenuation and steatosis estimation remain useful even when
        # the liver is axially truncated, so only volume depends on completeness.
        if modality == "CT" and organ_name == "liver" and status in {"Complete", "Incomplete"} and organ_mask is not None:
            hu_mean, hu_std = get_mask_mean_std(organ_mask, ct)
            results[f"{organ_name}_hu_mean"] = hu_mean
            results[f"{organ_name}_hu_std"] = hu_std

            if hu_mean is not None:
                pdff = -0.58 * hu_mean + 38.2
                pdff = max(0.0, min(100.0, pdff))
                results[f"{organ_name}_pdff_percent"] = round(pdff, 2)
                results[f"{organ_name}_pdff_kvp"] = kvp_raw
        elif status == "Complete" and modality == "CT":
            hu_mean, hu_std = get_mask_mean_std(organ_mask, ct)
            results[f"{organ_name}_hu_mean"] = hu_mean
            results[f"{organ_name}_hu_std"] = hu_std
        else:
            results[f"{organ_name}_hu_mean"] = None
            results[f"{organ_name}_hu_std"] = None

    # ============================================================
    # STEP 2.5: Renal Stone Burden
    # ============================================================
    # Literature and current workflow favor volumetric burden as the primary metric.
    # We also expose count, largest diameter, laterality, and HU for operational use.

    left_stone_path = find_first_existing_path(case_output_folder, [
        "total/kidney_stone_left.nii.gz",
        "total/renal_stone_left.nii.gz",
        "total/calculus_left.nii.gz",
        "stones/kidney_stone_left.nii.gz",
        "stones/renal_stone_left.nii.gz",
        "urology/kidney_stone_left.nii.gz",
        "urology/renal_stone_left.nii.gz"
    ])
    right_stone_path = find_first_existing_path(case_output_folder, [
        "total/kidney_stone_right.nii.gz",
        "total/renal_stone_right.nii.gz",
        "total/calculus_right.nii.gz",
        "stones/kidney_stone_right.nii.gz",
        "stones/renal_stone_right.nii.gz",
        "urology/kidney_stone_right.nii.gz",
        "urology/renal_stone_right.nii.gz"
    ])
    total_stone_path = find_first_existing_path(case_output_folder, [
        "total/kidney_stone.nii.gz",
        "total/kidney_stones.nii.gz",
        "total/renal_stone.nii.gz",
        "total/renal_stones.nii.gz",
        "total/calculus.nii.gz",
        "stones/kidney_stone.nii.gz",
        "stones/kidney_stones.nii.gz",
        "stones/renal_stone.nii.gz",
        "stones/renal_stones.nii.gz",
        "urology/kidney_stone.nii.gz",
        "urology/kidney_stones.nii.gz",
        "urology/renal_stone.nii.gz",
        "urology/renal_stones.nii.gz"
    ])

    left_mask_data = None
    right_mask_data = None
    total_mask_data = None
    kidney_left_mask_data = None
    kidney_right_mask_data = None
    stone_spacing_mm = None
    total_mask_is_derived = False

    stone_defaults = {
        "renal_stone_count": None,
        "renal_stone_total_volume_mm3": None,
        "renal_stone_total_volume_cm3": None,
        "renal_stone_largest_diameter_mm": None,
        "renal_stone_hu_mean": None,
        "renal_stone_hu_std": None,
        "renal_stone_left_count": None,
        "renal_stone_left_total_volume_mm3": None,
        "renal_stone_left_total_volume_cm3": None,
        "renal_stone_left_largest_diameter_mm": None,
        "renal_stone_left_hu_mean": None,
        "renal_stone_left_hu_std": None,
        "renal_stone_right_count": None,
        "renal_stone_right_total_volume_mm3": None,
        "renal_stone_right_total_volume_cm3": None,
        "renal_stone_right_largest_diameter_mm": None,
        "renal_stone_right_hu_mean": None,
        "renal_stone_right_hu_std": None,
        "renal_stone_bilateral": None,
        "renal_stone_kidney_left_complete": None,
        "renal_stone_kidney_right_complete": None,
        "renal_stone_kidneys_complete": None,
        "renal_stone_analysis_status": "Not available"
    }
    results.update(stone_defaults)

    try:
        kidney_left_path = total_dir / "kidney_left.nii.gz"
        kidney_right_path = total_dir / "kidney_right.nii.gz"

        left_kidney_status, kidney_left_mask_data, _ = get_structure_completeness(kidney_left_path)
        right_kidney_status, kidney_right_mask_data, _ = get_structure_completeness(kidney_right_path)
        if left_kidney_status != "Not available":
            results["renal_stone_kidney_left_complete"] = (left_kidney_status == "Complete")
        if right_kidney_status != "Not available":
            results["renal_stone_kidney_right_complete"] = (right_kidney_status == "Complete")

        if kidney_left_mask_data is not None and kidney_right_mask_data is not None:
            results["renal_stone_kidneys_complete"] = bool(
                results["renal_stone_kidney_left_complete"] and results["renal_stone_kidney_right_complete"]
            )

        if left_stone_path is not None:
            nii_left_stone = nib.load(str(left_stone_path))
            left_mask_data = np.asanyarray(nii_left_stone.dataobj) > 0
            stone_spacing_mm = nii_left_stone.header.get_zooms()

        if right_stone_path is not None:
            nii_right_stone = nib.load(str(right_stone_path))
            right_mask_data = np.asanyarray(nii_right_stone.dataobj) > 0
            stone_spacing_mm = stone_spacing_mm or nii_right_stone.header.get_zooms()

        if total_stone_path is not None:
            nii_total_stone = nib.load(str(total_stone_path))
            total_mask_data = np.asanyarray(nii_total_stone.dataobj) > 0
            stone_spacing_mm = stone_spacing_mm or nii_total_stone.header.get_zooms()

        left_kidney_complete = results["renal_stone_kidney_left_complete"]
        right_kidney_complete = results["renal_stone_kidney_right_complete"]

        if left_mask_data is not None and left_kidney_complete is False:
            left_mask_data = None

        if right_mask_data is not None and right_kidney_complete is False:
            right_mask_data = None

        if total_mask_data is not None:
            allowed_total_mask = np.zeros_like(total_mask_data, dtype=bool)

            if kidney_left_mask_data is not None and left_kidney_complete is True and kidney_left_mask_data.shape == total_mask_data.shape:
                allowed_total_mask |= kidney_left_mask_data

            if kidney_right_mask_data is not None and right_kidney_complete is True and kidney_right_mask_data.shape == total_mask_data.shape:
                allowed_total_mask |= kidney_right_mask_data

            if np.any(allowed_total_mask):
                total_mask_data = np.logical_and(total_mask_data, allowed_total_mask)
            elif left_kidney_complete is False and right_kidney_complete is False:
                total_mask_data = None

        if left_mask_data is not None and right_mask_data is not None:
            if left_mask_data.shape == right_mask_data.shape:
                total_mask_data = np.logical_or(left_mask_data, right_mask_data)
                total_mask_is_derived = True
            else:
                print(f"Warning: Left/right stone masks shape mismatch: {left_mask_data.shape} vs {right_mask_data.shape}")
        elif total_mask_data is None and left_mask_data is not None:
            total_mask_data = left_mask_data
            total_mask_is_derived = True
        elif total_mask_data is None and right_mask_data is not None:
            total_mask_data = right_mask_data
            total_mask_is_derived = True

        masks_available = any(mask is not None for mask in [left_mask_data, right_mask_data, total_mask_data])
        if masks_available and stone_spacing_mm is not None:
            if total_mask_data is not None:
                total_metrics = calculate_stone_mask_metrics(
                    total_mask_data,
                    stone_spacing_mm,
                    ct if modality == "CT" else None
                )
                results["renal_stone_count"] = total_metrics["count"]
                results["renal_stone_total_volume_mm3"] = total_metrics["total_volume_mm3"]
                results["renal_stone_total_volume_cm3"] = total_metrics["total_volume_cm3"]
                results["renal_stone_largest_diameter_mm"] = total_metrics["largest_diameter_mm"]
                results["renal_stone_hu_mean"] = total_metrics["hu_mean"] if modality == "CT" else None
                results["renal_stone_hu_std"] = total_metrics["hu_std"] if modality == "CT" else None
            for side_name, side_mask in [("left", left_mask_data), ("right", right_mask_data)]:
                if side_mask is not None:
                    side_metrics = calculate_stone_mask_metrics(
                        side_mask,
                        stone_spacing_mm,
                        ct if modality == "CT" else None
                    )
                    results[f"renal_stone_{side_name}_count"] = side_metrics["count"]
                    results[f"renal_stone_{side_name}_total_volume_mm3"] = side_metrics["total_volume_mm3"]
                    results[f"renal_stone_{side_name}_total_volume_cm3"] = side_metrics["total_volume_cm3"]
                    results[f"renal_stone_{side_name}_largest_diameter_mm"] = side_metrics["largest_diameter_mm"]
                    results[f"renal_stone_{side_name}_hu_mean"] = side_metrics["hu_mean"] if modality == "CT" else None
                    results[f"renal_stone_{side_name}_hu_std"] = side_metrics["hu_std"] if modality == "CT" else None
            results["renal_stone_bilateral"] = bool(
                left_mask_data is not None and np.any(left_mask_data) and
                right_mask_data is not None and np.any(right_mask_data)
            )

            if left_mask_data is not None and right_mask_data is not None:
                results["renal_stone_analysis_status"] = "Complete"
            elif left_kidney_complete is False or right_kidney_complete is False:
                results["renal_stone_analysis_status"] = "Incomplete kidneys"
            elif total_stone_path is not None and not total_mask_is_derived:
                results["renal_stone_analysis_status"] = "Total-only"
            else:
                results["renal_stone_analysis_status"] = "Partial"
        elif left_kidney_complete is False or right_kidney_complete is False:
            results["renal_stone_analysis_status"] = "Incomplete kidneys"

    except Exception as e:
        print(f"Error in renal stone burden analysis: {e}")
        results["renal_stone_analysis_status"] = "Error"

 # ============================================================
    # STEP 3: L3 Sarcopenia Analysis
    # ============================================================
    # Calculate Skeletal Muscle Area (SMA) and muscle density at L3 vertebra
    # L3 is a standard landmark for body composition assessment
    
    vertebra_L3_file = total_dir / "vertebrae_L3.nii.gz"
    muscle_file = case_output_folder / "artifacts" / "tissue_types" / "skeletal_muscle.nii.gz"
    results["L3_analysis_status"] = "Not available"

    if vertebra_L3_file.exists():
        try:
            l3_status, mask_L3, _ = get_structure_completeness(vertebra_L3_file)
            results["L3_analysis_status"] = l3_status
            if l3_status != "Complete":
                mask_L3 = None
            
            if mask_L3 is not None:
                # Find axial slices containing L3
                slice_L3_indices = np.where(mask_L3.sum(axis=(0, 1)) > 0)[0]
            
            if mask_L3 is not None and len(slice_L3_indices) > 0:
                # Use middle slice of L3 vertebra
                slice_idx = int(slice_L3_indices[len(slice_L3_indices)//2])
                results["slice_L3"] = slice_idx
                
                # Generate L3 overlay image (CT only)
                if modality == "CT" and generate_overlays:
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
                            muscle_data = nii_muscle.get_fdata(dtype=np.float32)
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
                    muscle_data = nii_muscle.get_fdata(dtype=np.float32)
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
                print(f"Warning: Skipping L3 analysis due to vertebra status: {results['L3_analysis_status']}")
                
        except Exception as e:
            print(f"Error in L3 analysis: {e}")
            results["L3_analysis_status"] = "Error"

    # ============================================================
    # STEP 4: Cerebral Hemorrhage Analysis
    # ============================================================
    bleed_file = case_output_folder / "artifacts" / "bleed" / "intracerebral_hemorrhage.nii.gz"
    brain_file = total_dir / "brain.nii.gz"
    skull_file = total_dir / "skull.nii.gz"
    brain_status, _, _ = get_structure_completeness(brain_file)
    results["brain_analysis_status"] = brain_status
    skull_status, _, _ = get_structure_completeness(skull_file)
    results["skull_analysis_status"] = skull_status
    results["hemorrhage_analysis_status"] = "Not available"
    results["hemorrhage_vol_cm3"] = None

    if bleed_file.exists():
        try:
            if brain_status != "Complete":
                results["hemorrhage_analysis_status"] = "Incomplete brain"
            else:
                bleed_status, mask_bleed, nii_bleed = get_structure_completeness(bleed_file)
                results["hemorrhage_analysis_status"] = bleed_status

                if mask_bleed is not None and nii_bleed is not None:
                    zooms = nii_bleed.header.get_zooms()
                    voxel_vol_mm3 = zooms[0] * zooms[1] * zooms[2]
                    results["hemorrhage_vol_cm3"] = round((np.sum(mask_bleed) * voxel_vol_mm3) / 1000.0, 3)

                if bleed_status == "Complete" and results["hemorrhage_vol_cm3"] and results["hemorrhage_vol_cm3"] > 0:
                    z_indices = np.where(mask_bleed.sum(axis=(0, 1)) > 0)[0]
                    if len(z_indices) > 0:
                        n_slices = len(z_indices)
                        idx_15 = max(0, min(int(n_slices * 0.15), n_slices - 1))
                        idx_50 = max(0, min(int(n_slices * 0.50), n_slices - 1))
                        idx_85 = max(0, min(int(n_slices * 0.85), n_slices - 1))

                        slices_to_gen = {
                            "inferior_15": int(z_indices[idx_15]),
                            "center_50": int(z_indices[idx_50]),
                            "superior_85": int(z_indices[idx_85])
                        }

                        if modality == "CT" and generate_overlays:
                            try:
                                import matplotlib
                                matplotlib.use('Agg')
                                import matplotlib.pyplot as plt

                                if ct.shape != mask_bleed.shape:
                                    print(f"Warning: Shape mismatch Bleed {mask_bleed.shape} vs CT {ct.shape}. Skipping overlay.")
                                else:
                                    for label, slice_idx in slices_to_gen.items():
                                        ct_slice = np.rot90(ct[:, :, slice_idx])
                                        mask_slice = np.rot90(mask_bleed[:, :, slice_idx])

                                        plt.figure(figsize=(8, 8))
                                        plt.imshow(ct_slice, cmap='gray', vmin=0, vmax=80)

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
            results["hemorrhage_analysis_status"] = "Error"

    # ============================================================
    # STEP 5: Bone Mineral Density (BMD) Analysis — L1 Vertebra
    # ============================================================
    vertebra_L1_file = total_dir / "vertebrae_L1.nii.gz"
    results["L1_bmd_analysis_status"] = "Not available"

    if vertebra_L1_file.exists() and modality == "CT":
        try:
            from scipy.ndimage import binary_erosion

            l1_status, mask_L1, nii_L1 = get_structure_completeness(vertebra_L1_file)
            results["L1_bmd_analysis_status"] = l1_status

            if l1_status != "Complete" or mask_L1 is None or nii_L1 is None:
                print(f"Warning: Skipping BMD due to L1 status: {results['L1_bmd_analysis_status']}")
            else:
                spacing = nii_L1.header.get_zooms()

                if ct.shape != mask_L1.shape:
                    print(f"Warning: Shape mismatch L1 {mask_L1.shape} vs CT {ct.shape}. Skipping BMD.")
                    results["L1_bmd_analysis_status"] = "Error"
                else:
                    z_indices = np.where(mask_L1.sum(axis=(0, 1)) > 0)[0]

                    if len(z_indices) > 0:
                        center_z = int(z_indices[len(z_indices) // 2])
                        mask_2d = mask_L1[:, :, center_z]
                        ct_2d = ct[:, :, center_z]

                        in_plane_spacing = min(spacing[0], spacing[1])
                        erosion_iters = max(1, int(5.0 / in_plane_spacing))
                        eroded_2d = binary_erosion(mask_2d, iterations=erosion_iters)

                        from scipy.ndimage import label as ndlabel, center_of_mass
                        labeled, num_features = ndlabel(eroded_2d)
                        if num_features > 1:
                            component_sizes = [np.sum(labeled == i) for i in range(1, num_features + 1)]
                            largest = np.argmax(component_sizes) + 1
                            eroded_2d = (labeled == largest)

                        if np.sum(eroded_2d) > 0:
                            full_com_x, full_com_y = center_of_mass(mask_2d)
                            body_com_x, body_com_y = center_of_mass(eroded_2d)

                            x_indices, y_indices = np.where(eroded_2d)
                            x_min, x_max = x_indices.min(), x_indices.max()
                            y_min, y_max = y_indices.min(), y_indices.max()

                            diff_x = abs(full_com_x - body_com_x)
                            diff_y = abs(full_com_y - body_com_y)

                            rx = (x_max - x_min) * 0.70 / 2.0
                            ry = (y_max - y_min) * 0.40 / 2.0

                            if diff_y > diff_x:
                                anterior_is_larger_y = body_com_y > full_com_y
                                center_x = (x_min + x_max) / 2.0
                                if anterior_is_larger_y:
                                    center_y = y_max - (y_max - y_min) * 0.25
                                else:
                                    center_y = y_min + (y_max - y_min) * 0.25
                            else:
                                anterior_is_larger_x = body_com_x > full_com_x
                                center_y = (y_min + y_max) / 2.0
                                if anterior_is_larger_x:
                                    center_x = x_max - (x_max - x_min) * 0.25
                                else:
                                    center_x = x_min + (x_max - x_min) * 0.25

                            x_grid_arr, y_grid_arr = np.ogrid[:mask_2d.shape[0], :mask_2d.shape[1]]
                            ellipse = ((x_grid_arr - center_x)**2 / rx**2) + ((y_grid_arr - center_y)**2 / ry**2) <= 1
                            roi_mask = ellipse & eroded_2d

                            trabecular_voxel_count = int(np.sum(roi_mask))
                            if trabecular_voxel_count > 0:
                                trabecular_hu = ct_2d[roi_mask]
                                hu_mean = float(np.mean(trabecular_hu))
                                hu_std = float(np.std(trabecular_hu))

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

                                if generate_overlays:
                                    try:
                                        import matplotlib
                                        matplotlib.use('Agg')
                                        import matplotlib.pyplot as plt

                                        ct_slice = np.rot90(ct_2d)
                                        roi_slice = np.rot90(roi_mask)
                                        mask_slice = np.rot90(mask_2d)

                                        rows = np.where(mask_slice.any(axis=1))[0]
                                        cols = np.where(mask_slice.any(axis=0))[0]

                                        if len(rows) > 0 and len(cols) > 0:
                                            r_min_crop, r_max_crop = rows[0], rows[-1]
                                            c_min_crop, c_max_crop = cols[0], cols[-1]

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
            results["L1_bmd_analysis_status"] = "Error"

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
    for lobe_key, _ in lung_lobes_map:
        results[f"{lobe_key}_analysis_status"] = "Not available"
    
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
        status, _, _ = get_structure_completeness(fpath)
        results[f"{lobe_key}_analysis_status"] = status
        if status != "Complete":
            lobes_complete = False
            break

        vol_cm3 = get_volume_cm3(fpath)
        min_vol = lobe_min_volumes_cm3.get(lobe_key, 0)
        
        if vol_cm3 < min_vol:
            results[f"{lobe_key}_analysis_status"] = "Below minimum volume"
            lobes_complete = False
            break
            
    if lobes_complete and modality == "CT":
        try:
            emphysema_results = {}
            total_lung_voxels = 0
            total_emphysema_voxels = 0
            
            # Get voxel volume in cm3 (1 voxel = pixdim[1]*pixdim[2]*pixdim[3] mm3)
            # 1 cm3 = 1000 mm3
            # Load the first lobe to get the header resolution
            nii_ref = nib.load(str(total_dir / lung_lobes_map[0][1]))
            voxel_vol_mm3 = nii_ref.header.get_zooms()[0] * nii_ref.header.get_zooms()[1] * nii_ref.header.get_zooms()[2]
            voxel_vol_cm3 = voxel_vol_mm3 / 1000.0
            
            for lobe_name, filename in lung_lobes_map:
                fpath = total_dir / filename
                nii_lobe = nib.load(str(fpath))
                mask_lobe = np.asanyarray(nii_lobe.dataobj) > 0
                
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
    
    Regions are detected based on the presence of complete anatomical structures:
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
        
        # A region is only considered present when at least one characteristic
        # structure is complete, not merely present in a truncated exam.
        for fname in files:
            fpath = total_dir / fname
            status, _, _ = get_structure_completeness(fpath)
            if status == "Complete":
                is_present = True
                break
        
        if is_present:
            detected.append(region)
            
    return detected
