#!/usr/bin/env python3
"""
Teste preliminar (Prova de Conceito) para o estudo de BMD em L1.
Importa a lógica oficial do Heimdallr de `metrics.metric_l1_bmd` para
garantir 100% de paridade na extração do ROI Oval Axial.
"""

import sys
from pathlib import Path
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import argparse
import nibabel as nib

# Permite importar do Heimdallr
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from metrics.metric_l1_bmd import compute_metric

def get_mean_hu_from_nifti(mask_path: Path, ct_path: Path) -> float | None:
    """Calcula HU volumétrico médio de toda a máscara 3D."""
    if not mask_path.exists() or not ct_path.exists():
        return None
    try:
        nii_mask = nib.load(str(mask_path))
        nii_ct = nib.load(str(ct_path))
        mask_data = np.asanyarray(nii_mask.dataobj) > 0
        ct_data = nii_ct.get_fdata(dtype=np.float32)
        if mask_data.shape != ct_data.shape:
            return None
        voxels = ct_data[mask_data]
        if voxels.size == 0:
            return None
        return float(np.mean(voxels))
    except Exception:
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--images-dir", default="/storage/dataset/prometheus/images", type=str)
    parser.add_argument("--out-dir", default="/home/rodrigo/Heimdallr/output/bmd_comparison", type=str)
    parser.add_argument("--max-cases", default=50, type=int)
    args = parser.parse_args()

    images_dir = Path(args.images_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cases = sorted((p for p in images_dir.iterdir() if p.is_dir()))
    print(f"Buscando até {args.max_cases} casos em {images_dir}...")

    data = []
    count = 0

    for case_dir in cases:
        if count >= args.max_cases:
            break

        ct_niftis = list(case_dir.glob("series_*.nii.gz"))
        if not ct_niftis:
            continue
            
        for ct_path in ct_niftis:
            series_name = ct_path.name.replace(".nii.gz", "")
            
            # Precisamos passar o case_output_folder simulado para a funçao achar o 'total/vertebrae_L1.nii.gz'
            # No novo pipeline, as máscaras estão em case_dir/segmentations/total/series_XXX
            # Contudo, metric_l1_bmd procura em: case_output_folder / "total" / "vertebrae_L1.nii.gz"
            # Então precisamos passar case_output_folder = case_dir / "segmentations" / "total" / series_name
            # mas vamos criar links simbolicos se preciso ou montar o path.
            
            # O Totalsegmentator do Prometheus cria: case_dir/segmentations/total/series_XXX/vertebrae_L1.nii.gz
            # Sendo "series_XXX" a raiz de saída do totalseg. 
            simulated_case_output = case_dir / "segmentations" / "total" / series_name
            l1_mask_path = simulated_case_output / "vertebrae_L1.nii.gz"
            
            if not l1_mask_path.exists():
                # Tenta fallback se estiver direto em segmentations/
                simulated_case_output = case_dir / "segmentations"
                l1_mask_path = simulated_case_output / "total" / "vertebrae_L1.nii.gz"
                if not l1_mask_path.exists():
                    continue

            # 1. Volumétrico 3D (Calculado manualmente pois não está no metric_l1_bmd)
            print(f"  [{case_dir.name}] Loading 3D Volumetric HU (NIfTI load into RAM)...")
            vol_hu = get_mean_hu_from_nifti(l1_mask_path, ct_path)
            if vol_hu is None:
                print(f"  [{case_dir.name}] Failed 3D Volumetric HU")
                continue

            mask_l1_data = np.asanyarray(nib.load(str(l1_mask_path)).dataobj) > 0
            nii_l1 = nib.load(str(l1_mask_path))
            ct_data = nib.load(str(ct_path)).get_fdata(dtype=np.float32)
            
            spacing = nii_l1.header.get_zooms()
            
            # The exact math from metric_l1_bmd.py, but bypassing the wrapper
            from scipy.ndimage import binary_erosion, center_of_mass, label as ndlabel
            
            if ct_data.shape != mask_l1_data.shape:
                continue

            z_indices = np.where(mask_l1_data.sum(axis=(0, 1)) > 0)[0]
            if len(z_indices) == 0:
                continue

            center_z = int(z_indices[len(z_indices) // 2])
            mask_2d = mask_l1_data[:, :, center_z]
            ct_2d = ct_data[:, :, center_z]

            in_plane_spacing = min(spacing[0], spacing[1])
            erosion_iters = max(1, int(5.0 / in_plane_spacing))
            eroded_2d = binary_erosion(mask_2d, iterations=erosion_iters)

            labeled, num_features = ndlabel(eroded_2d)
            if num_features > 1:
                component_sizes = [np.sum(labeled == i) for i in range(1, num_features + 1)]
                largest = int(np.argmax(component_sizes)) + 1
                eroded_2d = labeled == largest

            if np.sum(eroded_2d) <= 0:
                continue

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

            x_grid_arr, y_grid_arr = np.ogrid[: mask_2d.shape[0], : mask_2d.shape[1]]
            ellipse = ((x_grid_arr - center_x) ** 2 / rx**2) + ((y_grid_arr - center_y) ** 2 / ry**2) <= 1
            roi_mask = ellipse & eroded_2d

            trabecular_voxel_count = int(np.sum(roi_mask))
            if trabecular_voxel_count <= 0:
                continue

            trabecular_hu = ct_2d[roi_mask]
            axial_hu = float(np.mean(trabecular_hu))

            count += 1
            data.append({
                "accession_number": case_dir.name,
                "axial_oval_hu": float(axial_hu),
                "volumetric_3d_hu": round(vol_hu, 2)
            })
            
            if count % 10 == 0:
                print(f"Processado: {count}/{args.max_cases} casos")
            break # Pega 1 série válida por caso

    if len(data) < 2:
        print("Poucos dados válidos.")
        return

    df = pd.DataFrame(data)
    csv_path = out_dir / "pre_test_results.csv"
    df.to_csv(csv_path, index=False)

    plt.figure(figsize=(10, 8))
    sns.regplot(data=df, x="axial_oval_hu", y="volumetric_3d_hu", 
                scatter_kws={'alpha':0.6}, line_kws={'color':'red'})
    corr = df["axial_oval_hu"].corr(df["volumetric_3d_hu"])
    plt.title(f"Atenuação em L1: Volumétrico 3D vs. ROI Oval Axial\n(n={len(df)}, r={corr:.3f})")
    plt.xlabel("ROI Oval Axial Anterior (HU) [metric_l1_bmd.py]")
    plt.ylabel("Volumétrico 3D Completo (HU)")
    plt.grid(True, linestyle='--', alpha=0.7)
    
    # 1:1 line
    min_val = min(df["axial_oval_hu"].min(), df["volumetric_3d_hu"].min())
    max_val = max(df["axial_oval_hu"].max(), df["volumetric_3d_hu"].max())
    plt.plot([min_val, max_val], [min_val, max_val], 'k--', alpha=0.5, label='Concordância 1:1')
    plt.legend()
    
    scatter_path = out_dir / "scatter_3d_vs_axial.png"
    plt.savefig(scatter_path, dpi=150, bbox_inches='tight')
    plt.close()

    df["diff"] = df["volumetric_3d_hu"] - df["axial_oval_hu"]
    mean_diff = df["diff"].mean()
    std_diff = df["diff"].std()
    
    print("\n=== ESTATÍSTICAS PRELIMINARES ===")
    print(f"N total: {len(df)}")
    print(f"Correlação de Pearson (r): {corr:.3f}")
    print(f"Média Volumétrico 3D:   {df['volumetric_3d_hu'].mean():.1f} ± {df['volumetric_3d_hu'].std():.1f} HU")
    print(f"Média ROI Oval Axial:   {df['axial_oval_hu'].mean():.1f} ± {df['axial_oval_hu'].std():.1f} HU")
    print(f"Viés Sistemático Médio: {mean_diff:+.1f} HU (3D - Axial)")
    print(f"Desvio Padrão do Viés:  {std_diff:.1f} HU")
    print("=================================")
    print(f"Plots salvos em: {out_dir}")

if __name__ == "__main__":
    main()
