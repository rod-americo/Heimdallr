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
Kidney stone candidate triage for Heimdallr.

Adapted from TotalSegmentator/heimdallr/kidney_stone_triage.py and integrated
as a pipeline-side helper. This module does not modify the source script.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import nibabel as nib
import numpy as np
from scipy import ndimage


DEFAULT_MASKS = ("kidney_left", "kidney_right")


def _load_nifti(path: Path) -> nib.Nifti1Image:
    try:
        return nib.load(str(path))
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"NIfTI file not found: {path}") from exc


def _assert_same_geometry(ct_img: nib.Nifti1Image, mask_img: nib.Nifti1Image, mask_name: str) -> None:
    if ct_img.shape != mask_img.shape:
        raise ValueError(f"Geometry mismatch for {mask_name}: CT shape {ct_img.shape} vs mask shape {mask_img.shape}")
    if not np.allclose(ct_img.affine, mask_img.affine, atol=1e-4):
        raise ValueError(f"Affine mismatch for {mask_name}: CT and mask are not aligned")


def _to_serializable(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def _largest_axis_mm(coords_xyz: np.ndarray) -> float:
    if coords_xyz.shape[0] < 2:
        return 0.0
    centered = coords_xyz - coords_xyz.mean(axis=0, keepdims=True)
    try:
        _, _, vh = np.linalg.svd(centered, full_matrices=False)
    except np.linalg.LinAlgError:
        mins = coords_xyz.min(axis=0)
        maxs = coords_xyz.max(axis=0)
        return float(np.linalg.norm(maxs - mins))

    projected = centered @ vh[0]
    return float(projected.max() - projected.min())


def _principal_axes_mm(coords_xyz: np.ndarray) -> list[float]:
    if coords_xyz.shape[0] < 2:
        return [0.0, 0.0, 0.0]
    centered = coords_xyz - coords_xyz.mean(axis=0)
    _, _, vh = np.linalg.svd(centered, full_matrices=False)
    proj = centered @ vh.T
    extents = proj.max(axis=0) - proj.min(axis=0)
    extents = np.sort(extents)[::-1]
    padded = np.zeros(3, dtype=np.float64)
    padded[: min(3, extents.shape[0])] = extents[:3]
    return [float(v) for v in padded]


def _crop_bounds(mask: np.ndarray, margin_xyz: tuple[int, int, int]) -> tuple[np.ndarray, np.ndarray]:
    coords = np.argwhere(mask)
    mins = np.maximum(coords.min(axis=0) - np.asarray(margin_xyz), 0)
    maxs = np.minimum(coords.max(axis=0) + np.asarray(margin_xyz), np.asarray(mask.shape) - 1)
    return mins, maxs


def _render_component_overlays(
    ct: np.ndarray,
    kidney_mask: np.ndarray,
    component_mask: np.ndarray,
    mins: np.ndarray,
    maxs: np.ndarray,
    title_prefix: str,
    output_prefix: Path,
) -> dict[str, Any]:
    coords = np.argwhere(component_mask)
    centroid_ijk = np.round(coords.mean(axis=0)).astype(int)
    axial_slice = int(centroid_ijk[2])
    coronal_slice = int(centroid_ijk[1])
    x0, y0, z0 = mins
    x1, y1, z1 = maxs

    wl = 40.0
    ww = 400.0
    vmin = wl - ww / 2.0
    vmax = wl + ww / 2.0

    views = [
        (
            "axial",
            ct[:, :, axial_slice].T[y0 : y1 + 1, x0 : x1 + 1],
            kidney_mask[:, :, axial_slice].T[y0 : y1 + 1, x0 : x1 + 1],
            component_mask[:, :, axial_slice].T[y0 : y1 + 1, x0 : x1 + 1],
            f"{title_prefix} axial z={axial_slice}",
        ),
        (
            "coronal",
            ct[:, coronal_slice, :].T[z0 : z1 + 1, x0 : x1 + 1],
            kidney_mask[:, coronal_slice, :].T[z0 : z1 + 1, x0 : x1 + 1],
            component_mask[:, coronal_slice, :].T[z0 : z1 + 1, x0 : x1 + 1],
            f"{title_prefix} coronal y={coronal_slice}",
        ),
    ]

    paths: dict[str, Any] = {
        "axial_slice": axial_slice,
        "coronal_slice": coronal_slice,
    }
    for plane, ct_plane, kidney_plane, component_plane, title in views:
        out_path = output_prefix.parent / f"{output_prefix.name}_{plane}.png"
        fig, ax = plt.subplots(figsize=(7, 7), dpi=180)
        ax.imshow(ct_plane, cmap="gray", vmin=vmin, vmax=vmax, origin="lower")
        ax.contour(kidney_plane.astype(float), levels=[0.5], colors=["deepskyblue"], linewidths=1.0)
        masked = np.ma.masked_where(~component_plane, component_plane)
        ax.imshow(masked, cmap="autumn", alpha=0.8, origin="lower", interpolation="none")
        ax.contour(component_plane.astype(float), levels=[0.5], colors=["yellow"], linewidths=1.2)
        ax.set_title(title)
        ax.axis("off")
        fig.tight_layout(pad=0)
        fig.savefig(out_path, bbox_inches="tight", pad_inches=0)
        plt.close(fig)
        paths[f"{plane}_overlay_png"] = str(out_path)
    return paths


def analyze_kidneys(
    ct_path: Path,
    mask_dir: Path,
    threshold_hu: float = 130.0,
    masks: tuple[str, ...] = DEFAULT_MASKS,
    min_voxels: int = 3,
    min_volume_mm3: float | None = None,
    render_dir: Path | None = None,
) -> dict[str, Any]:
    ct_img = _load_nifti(ct_path)
    ct = ct_img.get_fdata(dtype=np.float32)
    voxel_spacing = tuple(float(v) for v in ct_img.header.get_zooms()[:3])
    voxel_volume_mm3 = float(np.prod(voxel_spacing))
    structure = ndimage.generate_binary_structure(rank=3, connectivity=3)

    if render_dir is None:
        render_dir = mask_dir / "kidney_stone_renders"
    render_dir.mkdir(parents=True, exist_ok=True)

    kidneys: list[dict[str, Any]] = []
    missing_masks: list[str] = []
    total_components = 0
    total_stone_volume_mm3 = 0.0
    max_component_axis_mm = 0.0
    max_component_hu = None

    for mask_name in masks:
        mask_path = mask_dir / f"{mask_name}.nii.gz"
        if not mask_path.exists():
            missing_masks.append(mask_name)
            continue

        mask_img = _load_nifti(mask_path)
        _assert_same_geometry(ct_img, mask_img, mask_name)
        kidney_mask = mask_img.get_fdata() > 0.5
        kidney_values = ct[kidney_mask]
        dense_mask = kidney_mask & (ct > threshold_hu)
        labels, num_components = ndimage.label(dense_mask, structure=structure)
        mins, maxs = _crop_bounds(kidney_mask, margin_xyz=(20, 20, 10))

        components: list[dict[str, Any]] = []
        for label in range(1, num_components + 1):
            component_mask = labels == label
            voxel_count = int(component_mask.sum())
            if voxel_count == 0:
                continue

            volume_mm3 = voxel_count * voxel_volume_mm3
            passes_voxel_filter = voxel_count >= min_voxels
            passes_volume_filter = min_volume_mm3 is not None and volume_mm3 >= min_volume_mm3
            if not (passes_voxel_filter or passes_volume_filter):
                continue

            component_values = ct[component_mask]
            coords_ijk = np.argwhere(component_mask)
            coords_xyz = nib.affines.apply_affine(ct_img.affine, coords_ijk)
            centroid_ijk = coords_ijk.mean(axis=0)
            centroid_xyz = coords_xyz.mean(axis=0)
            component_id = f"{mask_name}_component_{label}"
            overlay_paths = _render_component_overlays(
                ct=ct,
                kidney_mask=kidney_mask,
                component_mask=component_mask,
                mins=mins,
                maxs=maxs,
                title_prefix=component_id,
                output_prefix=render_dir / component_id,
            )
            largest_axis = _largest_axis_mm(coords_xyz)
            hu_max = float(component_values.max())
            max_component_axis_mm = max(max_component_axis_mm, largest_axis)
            max_component_hu = hu_max if max_component_hu is None else max(max_component_hu, hu_max)
            component = {
                "component_id": component_id,
                "label": label,
                "voxel_count": voxel_count,
                "volume_mm3": volume_mm3,
                "volume_ml": volume_mm3 / 1000.0,
                "hu_mean": float(component_values.mean()),
                "hu_max": hu_max,
                "centroid_ijk": [float(v) for v in centroid_ijk],
                "centroid_xyz_mm": [float(v) for v in centroid_xyz],
                "largest_axis_mm": largest_axis,
                "principal_axes_mm": _principal_axes_mm(coords_xyz),
                "passes_min_voxels": passes_voxel_filter,
                "passes_min_volume_mm3": passes_volume_filter,
                **overlay_paths,
            }
            components.append(component)

        components.sort(key=lambda item: item["volume_mm3"], reverse=True)
        total_components += len(components)
        total_stone_volume_mm3 += sum(item["volume_mm3"] for item in components)
        stone_voxel_count = sum(item["voxel_count"] for item in components)
        stone_volume_mm3 = sum(item["volume_mm3"] for item in components)

        kidneys.append(
            {
                "mask_name": mask_name,
                "mask_path": str(mask_path),
                "kidney_voxel_count": int(kidney_mask.sum()),
                "kidney_volume_ml": float(kidney_mask.sum() * voxel_volume_mm3 / 1000.0),
                "kidney_hu_mean": float(kidney_values.mean()) if kidney_values.size else None,
                "kidney_hu_max": float(kidney_values.max()) if kidney_values.size else None,
                "stone_voxel_count": stone_voxel_count,
                "stone_volume_mm3": float(stone_volume_mm3),
                "stone_volume_ml": float(stone_volume_mm3 / 1000.0),
                "component_count": len(components),
                "components": components,
            }
        )

    return {
        "ct_path": str(ct_path),
        "mask_dir": str(mask_dir),
        "threshold_hu": float(threshold_hu),
        "min_voxels": int(min_voxels),
        "min_volume_mm3": None if min_volume_mm3 is None else float(min_volume_mm3),
        "voxel_spacing_mm": voxel_spacing,
        "summary": {
            "kidneys_analyzed": len(kidneys),
            "total_components": total_components,
            "total_stone_volume_mm3": total_stone_volume_mm3,
            "max_component_axis_mm": max_component_axis_mm,
            "max_component_hu": max_component_hu,
        },
        "kidneys": kidneys,
        "missing_masks": missing_masks,
        "disclaimer": [
            "HU-threshold heuristic inside kidney masks; use for triage and review, not diagnostic confirmation.",
            "The default 130 HU threshold follows common non-contrast CT stone detection references.",
            "Artifacts, partial volume, contrast, clips, adjacent vascular calcifications, and imperfect kidney segmentation may produce false positives.",
        ],
    }


def summarize_report(report: dict[str, Any], report_path: Path | None = None, base_dir: Path | None = None) -> dict[str, Any]:
    kidneys = report.get("kidneys", [])
    summary = report.get("summary", {})
    left = next((item for item in kidneys if item.get("mask_name") == "kidney_left"), {})
    right = next((item for item in kidneys if item.get("mask_name") == "kidney_right"), {})

    left_largest = max(left.get("components", []), key=lambda item: float(item.get("volume_mm3", 0.0) or 0.0), default={})
    right_largest = max(right.get("components", []), key=lambda item: float(item.get("volume_mm3", 0.0) or 0.0), default={})
    missing_masks = report.get("missing_masks", [])
    total_components = int(summary.get("total_components", 0) or 0)
    kidneys_analyzed = int(summary.get("kidneys_analyzed", 0) or 0)

    if kidneys_analyzed == 0 and missing_masks:
        status = "Missing kidney masks"
    elif total_components > 0:
        status = "Candidates detected"
    else:
        status = "No candidates"

    report_ref = None
    if report_path is not None:
        report_ref = str(report_path.relative_to(base_dir)) if base_dir is not None else str(report_path)

    return {
        "kidney_stone_triage_status": status,
        "kidney_stone_triage_threshold_hu": float(report.get("threshold_hu", 130.0)),
        "kidney_stone_triage_kidneys_analyzed": kidneys_analyzed,
        "kidney_stone_triage_missing_masks": missing_masks,
        "kidney_stone_triage_total_components": total_components,
        "kidney_stone_triage_total_volume_mm3": round(float(summary.get("total_stone_volume_mm3", 0.0) or 0.0), 2),
        "kidney_stone_triage_max_component_axis_mm": round(float(summary.get("max_component_axis_mm", 0.0) or 0.0), 2),
        "kidney_stone_triage_max_component_hu": None if summary.get("max_component_hu") is None else round(float(summary["max_component_hu"]), 2),
        "kidney_stone_triage_left_components": int(left.get("component_count", 0) or 0),
        "kidney_stone_triage_right_components": int(right.get("component_count", 0) or 0),
        "kidney_stone_triage_left_volume_mm3": round(float(left.get("stone_volume_mm3", 0.0) or 0.0), 2),
        "kidney_stone_triage_right_volume_mm3": round(float(right.get("stone_volume_mm3", 0.0) or 0.0), 2),
        "kidney_stone_triage_left_largest_axis_mm": round(float(left_largest.get("largest_axis_mm", 0.0) or 0.0), 2),
        "kidney_stone_triage_right_largest_axis_mm": round(float(right_largest.get("largest_axis_mm", 0.0) or 0.0), 2),
        "kidney_stone_triage_left_largest_hu_mean": None if left_largest.get("hu_mean") is None else round(float(left_largest["hu_mean"]), 2),
        "kidney_stone_triage_right_largest_hu_mean": None if right_largest.get("hu_mean") is None else round(float(right_largest["hu_mean"]), 2),
        "kidney_stone_triage_left_largest_hu_max": None if left_largest.get("hu_max") is None else round(float(left_largest["hu_max"]), 2),
        "kidney_stone_triage_right_largest_hu_max": None if right_largest.get("hu_max") is None else round(float(right_largest["hu_max"]), 2),
        "kidney_stone_triage_report_path": report_ref,
    }


def write_report(
    ct_path: Path,
    mask_dir: Path,
    output_path: Path,
    threshold_hu: float = 130.0,
    masks: tuple[str, ...] = DEFAULT_MASKS,
    min_voxels: int = 3,
    min_volume_mm3: float | None = None,
    render_dir: Path | None = None,
) -> dict[str, Any]:
    report = analyze_kidneys(
        ct_path=ct_path,
        mask_dir=mask_dir,
        threshold_hu=threshold_hu,
        masks=masks,
        min_voxels=min_voxels,
        min_volume_mm3=min_volume_mm3,
        render_dir=render_dir,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=_to_serializable)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Detect kidney stone candidate components inside TotalSegmentator kidney masks using a HU threshold."
    )
    parser.add_argument("--ct", required=True, type=Path, help="Path to CT NIfTI in HU.")
    parser.add_argument("--mask-dir", required=True, type=Path, help="Directory containing individual TotalSegmentator masks.")
    parser.add_argument("--output", required=True, type=Path, help="Path to output JSON report.")
    parser.add_argument(
        "--masks",
        type=str,
        default=",".join(DEFAULT_MASKS),
        help="Comma-separated kidney masks to analyze. Defaults to kidney_left,kidney_right.",
    )
    parser.add_argument("--threshold-hu", type=float, default=130.0, help="HU threshold for dense stone candidate voxels.")
    parser.add_argument(
        "--min-voxels",
        type=int,
        default=3,
        help="Minimum connected voxels required to keep a component. Default: 3.",
    )
    parser.add_argument(
        "--min-volume-mm3",
        type=float,
        default=None,
        help="Optional minimum component volume in mm3. A component is kept if it passes min-voxels or min-volume-mm3.",
    )
    parser.add_argument(
        "--render-dir",
        type=Path,
        default=None,
        help="Optional directory to save axial/coronal PNG overlays for each component.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    masks = tuple(item.strip() for item in args.masks.split(",") if item.strip())
    write_report(
        ct_path=args.ct,
        mask_dir=args.mask_dir,
        output_path=args.output,
        threshold_hu=args.threshold_hu,
        masks=masks,
        min_voxels=args.min_voxels,
        min_volume_mm3=args.min_volume_mm3,
        render_dir=args.render_dir,
    )


if __name__ == "__main__":
    main()
