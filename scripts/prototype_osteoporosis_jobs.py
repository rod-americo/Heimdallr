#!/usr/bin/env python3
"""Prototype runner for opportunistic osteoporosis jobs.

This script inspects already-segmented Heimdallr studies and exports per-job
JSON and PNG artifacts for:

* bone_health_l1_hu
* bone_health_l1_volumetric
* vertebral_fracture_screen
* opportunistic_osteoporosis_composite

The prototype is intentionally self-contained. It reuses existing study layout
conventions and can fall back to any available vertebra mask when L1 is not
present, so it is usable on partial demo studies.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import nibabel as nib

try:  # pragma: no cover - best effort reuse of existing helpers
    from scipy.ndimage import binary_erosion, label as ndlabel
except Exception as exc:  # pragma: no cover
    raise RuntimeError("scipy is required for the osteoporosis prototype") from exc

try:  # pragma: no cover - optional reuse
    from heimdallr.shared.paths import study_id_json, study_results_json
except Exception:  # pragma: no cover
    study_id_json = None
    study_results_json = None


DEFAULT_OUTPUT_ROOT = Path.home() / "Temp" / "lab-osteoporose"
DEFAULT_TARGET_VERTEBRA = "L1"
VERTEBRA_PRIORITY = [
    "L1",
    "L2",
    "L3",
    "T12",
    "L4",
    "L5",
    "T11",
    "T10",
]


@dataclass
class StudyContext:
    case_id: str
    study_dir: Path
    ct_path: Path
    total_dir: Path
    output_dir: Path
    id_data: dict[str, Any]
    prior_results: dict[str, Any]


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def _as_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _local_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ensure_case_dir(path: Path) -> Path | None:
    if path.is_file() and path.name == "id.json":
        return path.parent.parent
    if (path / "metadata" / "id.json").exists():
        return path
    return None


def discover_study_dirs(inputs: Iterable[Path]) -> list[Path]:
    discovered: list[Path] = []
    seen: set[Path] = set()
    for raw in inputs:
        path = raw.expanduser().resolve()
        if path.is_file() and path.name == "id.json":
            case_dir = path.parent.parent
            if case_dir not in seen:
                discovered.append(case_dir)
                seen.add(case_dir)
            continue

        case_dir = _ensure_case_dir(path)
        if case_dir is not None:
            if case_dir not in seen:
                discovered.append(case_dir)
                seen.add(case_dir)
            continue

        if path.is_dir():
            for candidate in sorted(p for p in path.iterdir() if p.is_dir()):
                case_dir = _ensure_case_dir(candidate)
                if case_dir is not None and case_dir not in seen:
                    discovered.append(case_dir)
                    seen.add(case_dir)
    return discovered


def load_study_context(case_dir: Path, output_root: Path) -> StudyContext:
    metadata_dir = case_dir / "metadata"
    id_data = _load_json(metadata_dir / "id.json")
    prior_results = _load_json(metadata_dir / "resultados.json")
    case_id = str(id_data.get("CaseID") or case_dir.name)
    ct_path = case_dir / "derived" / f"{case_id}.nii.gz"
    if not ct_path.exists():
        derived = sorted(case_dir.glob("derived/*.nii.gz"))
        if len(derived) == 1:
            ct_path = derived[0]
    total_dir = _find_total_dir(case_dir)
    output_dir = output_root / case_id
    return StudyContext(
        case_id=case_id,
        study_dir=case_dir,
        ct_path=ct_path,
        total_dir=total_dir,
        output_dir=output_dir,
        id_data=id_data,
        prior_results=prior_results,
    )


def _find_total_dir(case_dir: Path) -> Path:
    candidates = [
        case_dir / "artifacts" / "total",
        case_dir / "segmentations" / "total",
        case_dir / "total",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return case_dir / "artifacts" / "total"


def _load_ct(ct_path: Path) -> tuple[np.ndarray, nib.Nifti1Image]:
    nii = nib.load(str(ct_path))
    return nii.get_fdata(dtype=np.float32), nii


def _load_mask(mask_path: Path) -> tuple[np.ndarray, nib.Nifti1Image]:
    nii = nib.load(str(mask_path))
    return np.asanyarray(nii.dataobj) > 0, nii


def _existing_vertebrae(total_dir: Path) -> dict[str, Path]:
    found: dict[str, Path] = {}
    for mask in total_dir.rglob("vertebrae_*.nii.gz"):
        stem = mask.name.replace(".nii.gz", "")
        if stem.startswith("vertebrae_"):
            found[stem.replace("vertebrae_", "")] = mask
    return found


def _pick_vertebra_mask(total_dir: Path, target: str = DEFAULT_TARGET_VERTEBRA) -> tuple[str | None, Path | None]:
    available = _existing_vertebrae(total_dir)
    if not available:
        return None, None
    if target in available:
        return target, available[target]
    for vertebra in VERTEBRA_PRIORITY:
        if vertebra in available:
            return vertebra, available[vertebra]
    chosen_name = sorted(available)[0]
    return chosen_name, available[chosen_name]


def _connected_component(mask: np.ndarray) -> np.ndarray:
    labeled, count = ndlabel(mask.astype(bool))
    if count <= 1:
        return mask.astype(bool)
    component_sizes = [(labeled == idx).sum() for idx in range(1, count + 1)]
    largest = int(np.argmax(component_sizes)) + 1
    return labeled == largest


def _mask_mean_std(ct: np.ndarray, mask: np.ndarray) -> tuple[float | None, float | None, int]:
    mask_bool = np.asarray(mask, dtype=bool)
    voxel_count = int(mask_bool.sum())
    if voxel_count == 0 or mask_bool.shape != ct.shape:
        return None, None, 0
    voxels = ct[mask_bool]
    if voxels.size == 0:
        return None, None, 0
    return float(np.mean(voxels)), float(np.std(voxels)), voxel_count


def _classify_bone_hu(hu_mean: float | None) -> str:
    if hu_mean is None:
        return "indeterminate"
    if hu_mean > 160:
        return "normal"
    if hu_mean >= 100:
        return "osteopenia"
    return "osteoporosis"


def _qc_flags(
    *,
    mask_available: bool,
    mask_complete: bool,
    voxel_count: int,
    modality: str | None,
    kvp: Any,
    slice_thickness: Any,
    contrast_phase: Any,
) -> dict[str, Any]:
    flags = {
        "mask_available": mask_available,
        "mask_complete": mask_complete,
        "voxel_count": int(voxel_count),
        "minimum_voxel_count_ok": bool(voxel_count >= 100),
        "modality": modality,
        "kvp": kvp,
        "slice_thickness_mm": slice_thickness,
        "contrast_phase": contrast_phase,
        "needs_manual_review": False,
    }
    if not mask_available or not mask_complete or voxel_count < 100:
        flags["needs_manual_review"] = True
    if modality not in {None, "CT"}:
        flags["needs_manual_review"] = True
    return flags


def _infer_study_metadata(id_data: dict[str, Any]) -> dict[str, Any]:
    return {
        "modality": id_data.get("Modality"),
        "kvp": id_data.get("KVP"),
        "contrast_phase": id_data.get("ContrastPhase") or id_data.get("Phase") or id_data.get("PredictedPhase"),
        "slice_thickness_mm": id_data.get("SliceThickness"),
        "manufacturer": id_data.get("Manufacturer"),
        "model": id_data.get("ManufacturerModelName"),
    }


def _axial_center_slice(mask: np.ndarray) -> int | None:
    z_indices = np.where(mask.sum(axis=(0, 1)) > 0)[0]
    if len(z_indices) == 0:
        return None
    return int(z_indices[len(z_indices) // 2])


def _build_axial_roi(mask_3d: np.ndarray, spacing: tuple[float, float, float]) -> tuple[int | None, np.ndarray | None, np.ndarray | None]:
    center_z = _axial_center_slice(mask_3d)
    if center_z is None:
        return None, None, None

    mask_2d = mask_3d[:, :, center_z]
    in_plane_spacing = max(0.1, float(min(spacing[0], spacing[1])))
    erosion_iters = max(1, int(round(5.0 / in_plane_spacing)))
    eroded_2d = binary_erosion(mask_2d, iterations=erosion_iters)
    eroded_2d = _connected_component(eroded_2d)
    if not np.any(eroded_2d):
        return center_z, mask_2d, None

    xs, ys = np.where(eroded_2d)
    x_min, x_max = int(xs.min()), int(xs.max())
    y_min, y_max = int(ys.min()), int(ys.max())

    full_x, full_y = np.where(mask_2d)
    full_com_x = float(np.mean(full_x)) if len(full_x) else float((x_min + x_max) / 2.0)
    full_com_y = float(np.mean(full_y)) if len(full_y) else float((y_min + y_max) / 2.0)
    core_com_x = float(np.mean(xs))
    core_com_y = float(np.mean(ys))

    diff_x = abs(full_com_x - core_com_x)
    diff_y = abs(full_com_y - core_com_y)

    rx = max(2.0, (x_max - x_min) * 0.70 / 2.0)
    ry = max(2.0, (y_max - y_min) * 0.40 / 2.0)

    if diff_y > diff_x:
        anterior_is_larger_y = core_com_y > full_com_y
        center_x = (x_min + x_max) / 2.0
        if anterior_is_larger_y:
            center_y = y_max - (y_max - y_min) * 0.25
        else:
            center_y = y_min + (y_max - y_min) * 0.25
    else:
        anterior_is_larger_x = core_com_x > full_com_x
        center_y = (y_min + y_max) / 2.0
        if anterior_is_larger_x:
            center_x = x_max - (x_max - x_min) * 0.25
        else:
            center_x = x_min + (x_max - x_min) * 0.25

    x_grid, y_grid = np.ogrid[:mask_2d.shape[0], :mask_2d.shape[1]]
    ellipse = ((x_grid - center_x) ** 2 / rx**2) + ((y_grid - center_y) ** 2 / ry**2) <= 1
    roi_mask = ellipse & eroded_2d
    if not np.any(roi_mask):
        return center_z, mask_2d, None
    return center_z, mask_2d, roi_mask


def _plot_axial_overlay(ct_2d: np.ndarray, mask_2d: np.ndarray, roi_mask: np.ndarray | None, out_path: Path, title: str) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.figure(figsize=(6, 6))
    plt.imshow(np.rot90(ct_2d), cmap="gray", vmin=-250, vmax=1250)
    plt.contour(np.rot90(mask_2d.astype(float)), levels=[0.5], colors="yellow", linewidths=1.2)
    if roi_mask is not None:
        masked = np.ma.masked_where(~np.rot90(roi_mask), np.rot90(ct_2d))
        plt.imshow(masked, cmap="cool", alpha=0.75, vmin=0, vmax=300)
    plt.axis("off")
    plt.title(title)
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close()


def _job_dir(output_root: Path, case_id: str, job_name: str) -> Path:
    return output_root / case_id / "metrics" / job_name


def _bone_health_l1_hu(
    *,
    case_id: str,
    study_dir: Path,
    ct: np.ndarray,
    ct_nii: nib.Nifti1Image,
    vertebra_name: str,
    mask_path: Path | None,
    out_dir: Path,
    study_meta: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "metric_key": "bone_health_l1_hu",
        "case_id": case_id,
        "status": "missing",
        "inputs": {
            "ct_path": None,
            "mask_path": str(mask_path) if mask_path else None,
            "analysis_vertebra": vertebra_name,
        },
        "measurement": {},
        "qc": {},
        "artifacts": {},
    }

    if mask_path is None or not mask_path.exists():
        result["qc"] = _qc_flags(
            mask_available=False,
            mask_complete=False,
            voxel_count=0,
            modality=study_meta.get("modality"),
            kvp=study_meta.get("kvp"),
            slice_thickness=study_meta.get("slice_thickness_mm"),
            contrast_phase=study_meta.get("contrast_phase"),
        )
        return result

    mask, mask_nii = _load_mask(mask_path)
    spacing = mask_nii.header.get_zooms()[:3]
    roi_slice, mask_2d, roi_mask = _build_axial_roi(mask, spacing)
    complete = bool(mask.sum() > 0 and mask.ndim == 3)
    if roi_slice is None or mask_2d is None or roi_mask is None:
        result["status"] = "indeterminate"
        result["qc"] = _qc_flags(
            mask_available=True,
            mask_complete=complete,
            voxel_count=int(mask.sum()),
            modality=study_meta.get("modality"),
            kvp=study_meta.get("kvp"),
            slice_thickness=study_meta.get("slice_thickness_mm"),
            contrast_phase=study_meta.get("contrast_phase"),
        )
        return result

    hu_mean, hu_std, voxel_count = _mask_mean_std(ct[:, :, roi_slice], roi_mask)
    classification = _classify_bone_hu(hu_mean)
    result["status"] = "done"
    result["inputs"]["ct_path"] = str(study_dir / "derived" / f"{case_id}.nii.gz")
    result["measurement"] = {
        "slice_index": int(roi_slice),
        "roi_voxel_count": int(voxel_count),
        "hu_mean": round(hu_mean, 2) if hu_mean is not None else None,
        "hu_std": round(hu_std, 2) if hu_std is not None else None,
        "classification": classification,
    }
    result["qc"] = _qc_flags(
        mask_available=True,
        mask_complete=complete,
        voxel_count=int(mask.sum()),
        modality=study_meta.get("modality"),
        kvp=study_meta.get("kvp"),
        slice_thickness=study_meta.get("slice_thickness_mm"),
        contrast_phase=study_meta.get("contrast_phase"),
    )

    overlay_path = out_dir / "overlay.png"
    _plot_axial_overlay(
        ct[:, :, roi_slice],
        mask_2d,
        roi_mask,
        overlay_path,
        f"{case_id} - bone_health_l1_hu ({vertebra_name})",
    )
    result["artifacts"] = {"result_json": "result.json", "overlay_png": "overlay.png"}
    result["status"] = "done"
    return result


def _bone_health_l1_volumetric(
    *,
    case_id: str,
    study_dir: Path,
    ct: np.ndarray,
    ct_nii: nib.Nifti1Image,
    vertebra_name: str,
    mask_path: Path | None,
    out_dir: Path,
    study_meta: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "metric_key": "bone_health_l1_volumetric",
        "case_id": case_id,
        "status": "missing",
        "inputs": {
            "ct_path": None,
            "mask_path": str(mask_path) if mask_path else None,
            "analysis_vertebra": vertebra_name,
        },
        "measurement": {},
        "qc": {},
        "artifacts": {},
    }

    if mask_path is None or not mask_path.exists():
        result["qc"] = _qc_flags(
            mask_available=False,
            mask_complete=False,
            voxel_count=0,
            modality=study_meta.get("modality"),
            kvp=study_meta.get("kvp"),
            slice_thickness=study_meta.get("slice_thickness_mm"),
            contrast_phase=study_meta.get("contrast_phase"),
        )
        return result

    mask, mask_nii = _load_mask(mask_path)
    spacing = mask_nii.header.get_zooms()[:3]
    voxel_count = int(mask.sum())
    if voxel_count == 0:
        result["qc"] = _qc_flags(
            mask_available=True,
            mask_complete=False,
            voxel_count=0,
            modality=study_meta.get("modality"),
            kvp=study_meta.get("kvp"),
            slice_thickness=study_meta.get("slice_thickness_mm"),
            contrast_phase=study_meta.get("contrast_phase"),
        )
        return result

    erode_iters = max(1, int(round(3.0 / max(0.1, float(min(spacing))))))
    core = binary_erosion(mask, iterations=erode_iters)
    core = _connected_component(core)

    z_indices = np.where(core.sum(axis=(0, 1)) > 0)[0]
    if len(z_indices) < 3:
        core = mask
        z_indices = np.where(core.sum(axis=(0, 1)) > 0)[0]

    if len(z_indices) == 0:
        result["qc"] = _qc_flags(
            mask_available=True,
            mask_complete=False,
            voxel_count=voxel_count,
            modality=study_meta.get("modality"),
            kvp=study_meta.get("kvp"),
            slice_thickness=study_meta.get("slice_thickness_mm"),
            contrast_phase=study_meta.get("contrast_phase"),
        )
        return result

    z_lo = int(z_indices[max(0, int(len(z_indices) * 0.20))])
    z_hi = int(z_indices[min(len(z_indices) - 1, int(len(z_indices) * 0.80))])
    if z_hi <= z_lo:
        z_lo, z_hi = int(z_indices[0]), int(z_indices[-1])

    window = np.zeros_like(core, dtype=bool)
    window[:, :, z_lo : z_hi + 1] = True
    core_window = core & window
    if not np.any(core_window):
        core_window = core

    hu_mean, hu_std, core_voxels = _mask_mean_std(ct, core_window)
    classification = _classify_bone_hu(hu_mean)
    result["status"] = "done"
    result["inputs"]["ct_path"] = str(study_dir / "derived" / f"{case_id}.nii.gz")
    result["measurement"] = {
        "core_voxel_count": int(core_voxels),
        "window_slice_range": [int(z_lo), int(z_hi)],
        "hu_mean": round(hu_mean, 2) if hu_mean is not None else None,
        "hu_std": round(hu_std, 2) if hu_std is not None else None,
        "classification": classification,
        "erosion_iterations": int(erode_iters),
        "window_fraction": round(float(core_window.sum()) / float(voxel_count), 4),
    }
    result["qc"] = _qc_flags(
        mask_available=True,
        mask_complete=True,
        voxel_count=voxel_count,
        modality=study_meta.get("modality"),
        kvp=study_meta.get("kvp"),
        slice_thickness=study_meta.get("slice_thickness_mm"),
        contrast_phase=study_meta.get("contrast_phase"),
    )

    overlay_path = out_dir / "overlay.png"
    projection = np.max(core_window, axis=2).astype(float)
    ct_projection = np.max(ct[:, :, z_lo : z_hi + 1], axis=2)
    _plot_axial_overlay(
        ct_projection,
        projection > 0,
        None,
        overlay_path,
        f"{case_id} - bone_health_l1_volumetric ({vertebra_name})",
    )
    result["artifacts"] = {"result_json": "result.json", "overlay_png": "overlay.png"}
    return result


def _vertebral_fracture_screen(
    *,
    case_id: str,
    study_dir: Path,
    ct: np.ndarray,
    ct_nii: nib.Nifti1Image,
    vertebra_name: str,
    mask_path: Path | None,
    out_dir: Path,
    study_meta: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "metric_key": "vertebral_fracture_screen",
        "case_id": case_id,
        "status": "missing",
        "inputs": {
            "ct_path": None,
            "mask_path": str(mask_path) if mask_path else None,
            "analysis_vertebra": vertebra_name,
        },
        "measurement": {},
        "qc": {},
        "artifacts": {},
    }

    if mask_path is None or not mask_path.exists():
        result["qc"] = _qc_flags(
            mask_available=False,
            mask_complete=False,
            voxel_count=0,
            modality=study_meta.get("modality"),
            kvp=study_meta.get("kvp"),
            slice_thickness=study_meta.get("slice_thickness_mm"),
            contrast_phase=study_meta.get("contrast_phase"),
        )
        return result

    mask, mask_nii = _load_mask(mask_path)
    voxel_count = int(mask.sum())
    if voxel_count < 80:
        result["status"] = "indeterminate"
        result["qc"] = _qc_flags(
            mask_available=True,
            mask_complete=False,
            voxel_count=voxel_count,
            modality=study_meta.get("modality"),
            kvp=study_meta.get("kvp"),
            slice_thickness=study_meta.get("slice_thickness_mm"),
            contrast_phase=study_meta.get("contrast_phase"),
        )
        return result

    affine = mask_nii.affine
    coords = np.argwhere(mask)
    coords_mm = nib.affines.apply_affine(affine, coords).astype(np.float32, copy=False)
    centered = coords_mm - coords_mm.mean(axis=0, keepdims=True)
    try:
        _, _, vh = np.linalg.svd(centered, full_matrices=False)
    except np.linalg.LinAlgError:
        result["status"] = "indeterminate"
        result["qc"] = _qc_flags(
            mask_available=True,
            mask_complete=True,
            voxel_count=voxel_count,
            modality=study_meta.get("modality"),
            kvp=study_meta.get("kvp"),
            slice_thickness=study_meta.get("slice_thickness_mm"),
            contrast_phase=study_meta.get("contrast_phase"),
        )
        return result

    projected = centered @ vh.T
    height_axis = projected[:, 0]
    sagittal_axis = projected[:, 1]

    tertiles = np.quantile(sagittal_axis, [1 / 3, 2 / 3])
    bins = [
        sagittal_axis <= tertiles[0],
        (sagittal_axis > tertiles[0]) & (sagittal_axis <= tertiles[1]),
        sagittal_axis > tertiles[1],
    ]
    bin_heights: list[float | None] = []
    for bin_mask in bins:
        subset = height_axis[bin_mask]
        if subset.size < 5:
            bin_heights.append(None)
        else:
            low = float(np.percentile(subset, 5))
            high = float(np.percentile(subset, 95))
            bin_heights.append(round(high - low, 2))

    valid = [h for h in bin_heights if h is not None]
    full_height = round(float(np.percentile(height_axis, 95) - np.percentile(height_axis, 5)), 2)
    width = round(float(np.percentile(sagittal_axis, 95) - np.percentile(sagittal_axis, 5)), 2)
    depth = round(float(np.percentile(projected[:, 2], 95) - np.percentile(projected[:, 2], 5)), 2)
    confidence = "high" if voxel_count >= 400 and len(valid) >= 3 else "low"

    suspected = False
    pattern = "indeterminate"
    if len(valid) == 3:
        h0, h1, h2 = valid
        edge_max = max(h0, h2)
        edge_min = min(h0, h2)
        if h1 < 0.75 * edge_max and edge_min > 0.80 * edge_max:
            suspected = True
            pattern = "biconcave"
        elif edge_min < 0.75 * edge_max:
            suspected = True
            pattern = "wedge"
        elif full_height < 0.55 * max(width, depth):
            suspected = True
            pattern = "compression_like"

    status = "suspected" if suspected else "negative"
    if confidence == "low":
        status = "indeterminate"

    result["status"] = status
    result["inputs"]["ct_path"] = str(study_dir / "derived" / f"{case_id}.nii.gz")
    result["measurement"] = {
        "full_height_mm": full_height,
        "cross_section_width_mm": width,
        "cross_section_depth_mm": depth,
        "bin_heights_mm": bin_heights,
        "height_ratio_min_max": round(float(min(valid) / max(valid)), 3) if valid and max(valid) > 0 else None,
        "height_ratio_mid_edge_mean": round(float(valid[1] / ((valid[0] + valid[2]) / 2.0)), 3) if len(valid) == 3 and (valid[0] + valid[2]) > 0 else None,
        "pattern": pattern,
        "suspected": suspected,
        "confidence": confidence,
    }
    result["qc"] = _qc_flags(
        mask_available=True,
        mask_complete=True,
        voxel_count=voxel_count,
        modality=study_meta.get("modality"),
        kvp=study_meta.get("kvp"),
        slice_thickness=study_meta.get("slice_thickness_mm"),
        contrast_phase=study_meta.get("contrast_phase"),
    )

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(7, 4))
    labels = ["bin_1", "bin_2", "bin_3"]
    heights = [0 if h is None else h for h in bin_heights]
    colors = ["#2d6cdf", "#6c5ce7", "#00b894"]
    ax.bar(labels, heights, color=colors)
    ax.set_ylabel("Height extent (mm)")
    ax.set_title(f"{case_id} - vertebral_fracture_screen ({vertebra_name})")
    ax.text(0.02, 0.95, f"status: {status}\npattern: {pattern}\nconfidence: {confidence}", transform=ax.transAxes, va="top")
    fig.tight_layout()
    overlay_path = out_dir / "profile.png"
    overlay_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(overlay_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    result["artifacts"] = {"result_json": "result.json", "profile_png": "profile.png"}
    return result


def _opportunistic_osteoporosis_composite(
    *,
    case_id: str,
    bone_hu: dict[str, Any],
    volumetric: dict[str, Any],
    fracture: dict[str, Any],
    out_dir: Path,
    study_meta: dict[str, Any],
) -> dict[str, Any]:
    score = 0
    rationale: list[str] = []

    for source in (bone_hu, volumetric):
        cls = source.get("measurement", {}).get("classification")
        if cls == "osteoporosis":
            score += 2
            rationale.append(f"{source['metric_key']}: osteoporosis")
        elif cls == "osteopenia":
            score += 1
            rationale.append(f"{source['metric_key']}: osteopenia")

    if fracture.get("measurement", {}).get("suspected"):
        score += 2
        rationale.append(f"{fracture['metric_key']}: suspected fracture")

    if any(job.get("status") == "indeterminate" for job in (bone_hu, volumetric, fracture)):
        score = max(0, score - 1)
        rationale.append("qc penalty: indeterminate signal")

    if score >= 4:
        band = "high"
    elif score >= 2:
        band = "moderate"
    else:
        band = "low"

    result = {
        "metric_key": "opportunistic_osteoporosis_composite",
        "case_id": case_id,
        "status": "done",
        "inputs": {
            "bone_health_l1_hu": bone_hu.get("metric_key"),
            "bone_health_l1_volumetric": volumetric.get("metric_key"),
            "vertebral_fracture_screen": fracture.get("metric_key"),
        },
        "measurement": {
            "score": int(score),
            "risk_band": band,
            "rationale": rationale,
        },
        "qc": {
            "modality": study_meta.get("modality"),
            "needs_manual_review": band == "high" or any(job.get("qc", {}).get("needs_manual_review") for job in (bone_hu, volumetric, fracture)),
        },
        "artifacts": {"result_json": "result.json"},
    }
    return result


def _write_job(case_id: str, job_name: str, out_root: Path, payload: dict[str, Any] | None = None) -> Path:
    job_dir = _job_dir(out_root, case_id, job_name)
    job_dir.mkdir(parents=True, exist_ok=True)
    if payload is not None:
        _save_json(job_dir / "result.json", payload)
    return job_dir


def _write_case_summary(case_id: str, study_dir: Path, outputs: dict[str, dict[str, Any]], out_root: Path, study_meta: dict[str, Any]) -> Path:
    summary = {
        "case_id": case_id,
        "created_at": _local_timestamp(),
        "source_study_dir": str(study_dir),
        "study_meta": study_meta,
        "jobs": outputs,
    }
    summary_dir = out_root / case_id
    summary_dir.mkdir(parents=True, exist_ok=True)
    _save_json(summary_dir / "prototype_summary.json", summary)

    md_lines = [
        f"# Osteoporosis Prototype Summary - {case_id}",
        "",
        f"- Created at: {_local_timestamp()}",
        f"- Source study: `{study_dir}`",
        "",
    ]
    for name, payload in outputs.items():
        md_lines.append(f"## {name}")
        md_lines.append(f"- status: `{payload.get('status')}`")
        md_lines.append(f"- metric_key: `{payload.get('metric_key')}`")
        md_lines.append(f"- result: `metrics/{name}/result.json`")
        artifacts = payload.get("artifacts", {})
        for artifact_name, rel_path in artifacts.items():
            md_lines.append(f"- {artifact_name}: `metrics/{name}/{rel_path}`")
        md_lines.append("")
    (summary_dir / "prototype_summary.md").write_text("\n".join(md_lines), encoding="utf-8")
    return summary_dir


def run_case(case_dir: Path, output_root: Path, target_vertebra: str = DEFAULT_TARGET_VERTEBRA) -> dict[str, Any]:
    ctx = load_study_context(case_dir, output_root)
    ctx.output_dir.mkdir(parents=True, exist_ok=True)
    ct, ct_nii = _load_ct(ctx.ct_path)
    vertebra_name, vertebra_mask_path = _pick_vertebra_mask(ctx.total_dir, target_vertebra)
    if vertebra_mask_path is None:
        vertebra_name = target_vertebra

    study_meta = _infer_study_metadata(ctx.id_data)
    outputs: dict[str, dict[str, Any]] = {}

    bone_dir = _write_job(ctx.case_id, "bone_health_l1_hu", output_root)
    bone_hu = _bone_health_l1_hu(
        case_id=ctx.case_id,
        study_dir=ctx.study_dir,
        ct=ct,
        ct_nii=ct_nii,
        vertebra_name=vertebra_name or target_vertebra,
        mask_path=vertebra_mask_path,
        out_dir=bone_dir,
        study_meta=study_meta,
    )
    _save_json(bone_dir / "result.json", bone_hu)
    outputs["bone_health_l1_hu"] = bone_hu

    vol_dir = _write_job(ctx.case_id, "bone_health_l1_volumetric", output_root)
    volumetric = _bone_health_l1_volumetric(
        case_id=ctx.case_id,
        study_dir=ctx.study_dir,
        ct=ct,
        ct_nii=ct_nii,
        vertebra_name=vertebra_name or target_vertebra,
        mask_path=vertebra_mask_path,
        out_dir=vol_dir,
        study_meta=study_meta,
    )
    _save_json(vol_dir / "result.json", volumetric)
    outputs["bone_health_l1_volumetric"] = volumetric

    fracture_dir = _write_job(ctx.case_id, "vertebral_fracture_screen", output_root)
    fracture = _vertebral_fracture_screen(
        case_id=ctx.case_id,
        study_dir=ctx.study_dir,
        ct=ct,
        ct_nii=ct_nii,
        vertebra_name=vertebra_name or target_vertebra,
        mask_path=vertebra_mask_path,
        out_dir=fracture_dir,
        study_meta=study_meta,
    )
    _save_json(fracture_dir / "result.json", fracture)
    outputs["vertebral_fracture_screen"] = fracture

    composite_dir = _write_job(ctx.case_id, "opportunistic_osteoporosis_composite", output_root)
    composite = _opportunistic_osteoporosis_composite(
        case_id=ctx.case_id,
        bone_hu=bone_hu,
        volumetric=volumetric,
        fracture=fracture,
        out_dir=composite_dir,
        study_meta=study_meta,
    )
    _save_json(composite_dir / "result.json", composite)
    outputs["opportunistic_osteoporosis_composite"] = composite

    _write_case_summary(ctx.case_id, ctx.study_dir, outputs, output_root, study_meta)
    return outputs


def _write_placeholder(case_dir: Path, job_name: str, output_root: Path) -> Path:
    job_dir = output_root / case_dir.name / "metrics" / job_name
    job_dir.mkdir(parents=True, exist_ok=True)
    return job_dir


def _generate_synthetic_case() -> tuple[np.ndarray, nib.Nifti1Image, np.ndarray, nib.Nifti1Image]:
    ct = np.full((64, 64, 24), 80.0, dtype=np.float32)
    mask = np.zeros((64, 64, 24), dtype=bool)
    for z in range(8, 16):
        yy, xx = np.ogrid[:64, :64]
        ellipse = ((xx - 32) ** 2 / 12.0**2) + ((yy - 32) ** 2 / 8.0**2) <= 1
        mask[:, :, z] = ellipse
        ct[:, :, z][ellipse] = 145.0
    ct_nii = nib.Nifti1Image(ct, affine=np.eye(4))
    mask_nii = nib.Nifti1Image(mask.astype(np.uint8), affine=np.eye(4))
    return ct, ct_nii, mask.astype(bool), mask_nii


def run_self_test() -> None:
    ct, ct_nii, mask, mask_nii = _generate_synthetic_case()
    tmp = Path.cwd() / "_osteoporosis_self_test"
    study_dir = tmp / "study"
    study_dir.mkdir(parents=True, exist_ok=True)
    study_meta = {"modality": "CT", "kvp": 120, "slice_thickness_mm": 1.0, "contrast_phase": "native"}
    bone_hu = _bone_health_l1_hu(
        case_id="synthetic",
        study_dir=study_dir,
        ct=ct,
        ct_nii=ct_nii,
        vertebra_name="L1",
        mask_path=tmp / "vertebrae_L1.nii.gz",
        out_dir=tmp / "bone_hu",
        study_meta=study_meta,
    )
    # Use the in-memory mask directly for smoke testing of the lower-level helpers.
    center = _axial_center_slice(mask)
    assert center is not None
    _, _, roi = _build_axial_roi(mask, mask_nii.header.get_zooms()[:3])
    assert roi is not None and roi.sum() > 0
    vol_mean, _, _ = _mask_mean_std(ct, mask)
    assert vol_mean is not None and 100.0 < vol_mean < 170.0
    fracture = _vertebral_fracture_screen(
        case_id="synthetic",
        study_dir=study_dir,
        ct=ct,
        ct_nii=ct_nii,
        vertebra_name="L1",
        mask_path=tmp / "vertebrae_L1.nii.gz",
        out_dir=tmp / "fracture",
        study_meta=study_meta,
    )
    assert isinstance(fracture, dict)
    assert bone_hu["metric_key"] == "bone_health_l1_hu"
    print("self-test: ok")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prototype osteoporosis jobs from segmented Heimdallr studies.")
    parser.add_argument("paths", nargs="*", type=Path, help="Study directories, id.json files, or a directory containing studies.")
    parser.add_argument("--input-root", type=Path, default=Path("runtime/studies"), help="Default root to scan when no explicit paths are passed.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT, help="Where prototype artifacts are written.")
    parser.add_argument("--target-vertebra", default=DEFAULT_TARGET_VERTEBRA, help="Preferred vertebra for the opportunistic jobs.")
    parser.add_argument("--limit", type=int, default=0, help="Limit the number of studies processed.")
    parser.add_argument("--self-test", action="store_true", help="Run a lightweight synthetic smoke test and exit.")
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.self_test:
        run_self_test()
        return 0

    inputs = list(args.paths) if args.paths else [args.input_root]
    study_dirs = discover_study_dirs(inputs)
    if not study_dirs:
        print("No studies found.")
        return 1

    if args.limit and args.limit > 0:
        study_dirs = study_dirs[: args.limit]

    args.output_root.mkdir(parents=True, exist_ok=True)
    index: list[dict[str, Any]] = []

    for case_dir in study_dirs:
        try:
            outputs = run_case(case_dir, args.output_root, target_vertebra=args.target_vertebra)
            index.append(
                {
                    "case_id": case_dir.name,
                    "source_study_dir": str(case_dir),
                    "status": "done",
                    "jobs": {name: payload.get("status") for name, payload in outputs.items()},
                }
            )
            print(f"[ok] {case_dir.name}")
        except Exception as exc:
            index.append(
                {
                    "case_id": case_dir.name,
                    "source_study_dir": str(case_dir),
                    "status": "error",
                    "error": str(exc),
                }
            )
            print(f"[error] {case_dir.name}: {exc}")

    _save_json(args.output_root / "index.json", {"created_at": _local_timestamp(), "cases": index})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
