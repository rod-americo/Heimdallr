#!/usr/bin/env python3
"""Prototype runner for abdominal VAT/SAT-style fat jobs.

This script is intentionally self-contained and follows the same artifact
layout used by the osteoporosis prototype. It uses TotalSegmentator outputs:

* artifacts/tissue_types/subcutaneous_fat.nii.gz
* artifacts/tissue_types/torso_fat.nii.gz
* artifacts/total/vertebrae_{T12,L1,L2,L3,L4,L5}.nii.gz

The jobs use vertebral-mask axial extents as reproducible slabs for
longitudinal follow-up. The `torso_fat` compartment is preserved by name and
also exposed as a "visceral proxy" within the abdominal slabs.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import nibabel as nib
import numpy as np


DEFAULT_OUTPUT_ROOT = Path.home() / "Temp" / "lab-gordura"
TARGET_LEVELS = ["T12", "L1", "L2", "L3", "L4", "L5"]


@dataclass
class StudyContext:
    case_id: str
    study_dir: Path
    ct_path: Path
    total_dir: Path
    tissue_dir: Path
    output_dir: Path
    id_data: dict[str, Any]
    prior_results: dict[str, Any]


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


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


def _find_dir(case_dir: Path, name: str) -> Path:
    candidates = [
        case_dir / "artifacts" / name,
        case_dir / "segmentations" / name,
        case_dir / name,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


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
    return StudyContext(
        case_id=case_id,
        study_dir=case_dir,
        ct_path=ct_path,
        total_dir=_find_dir(case_dir, "total"),
        tissue_dir=_find_dir(case_dir, "tissue_types"),
        output_dir=output_root / case_id,
        id_data=id_data,
        prior_results=prior_results,
    )


def _load_ct(ct_path: Path) -> tuple[np.ndarray, nib.Nifti1Image]:
    nii = nib.load(str(ct_path))
    return nii.get_fdata(dtype=np.float32), nii


def _load_mask(mask_path: Path) -> tuple[np.ndarray, nib.Nifti1Image]:
    nii = nib.load(str(mask_path))
    return np.asanyarray(nii.dataobj) > 0, nii


def _mask_axial_extent(mask: np.ndarray) -> tuple[int, int] | None:
    z_indices = np.where(mask.sum(axis=(0, 1)) > 0)[0]
    if len(z_indices) == 0:
        return None
    return int(z_indices[0]), int(z_indices[-1])


def _mask_complete(mask: np.ndarray) -> bool:
    extent = _mask_axial_extent(mask)
    if extent is None:
        return False
    z_min, z_max = extent
    return z_min > 0 and z_max < (mask.shape[2] - 1)


def _volume_cm3(mask: np.ndarray, spacing: tuple[float, float, float], z_range: tuple[int, int] | None = None) -> float:
    mask_bool = np.asarray(mask, dtype=bool)
    if z_range is not None:
        z_min, z_max = z_range
        slab = np.zeros_like(mask_bool, dtype=bool)
        slab[:, :, z_min : z_max + 1] = True
        mask_bool = mask_bool & slab
    voxel_volume_mm3 = float(spacing[0] * spacing[1] * spacing[2])
    return round(float(mask_bool.sum()) * voxel_volume_mm3 / 1000.0, 3)


def _slice_area_cm2(mask_2d: np.ndarray, spacing_xy: tuple[float, float]) -> float:
    pixel_area_mm2 = float(spacing_xy[0] * spacing_xy[1])
    return round(float(np.count_nonzero(mask_2d)) * pixel_area_mm2 / 100.0, 3)


def _level_mask_paths(total_dir: Path) -> dict[str, Path]:
    found: dict[str, Path] = {}
    for level in TARGET_LEVELS:
        mask_path = total_dir / f"vertebrae_{level}.nii.gz"
        if mask_path.exists():
            found[level] = mask_path
    return found


def _infer_study_metadata(id_data: dict[str, Any]) -> dict[str, Any]:
    return {
        "modality": id_data.get("Modality"),
        "kvp": id_data.get("KVP"),
        "contrast_phase": id_data.get("ContrastPhase") or id_data.get("Phase") or id_data.get("PredictedPhase"),
        "slice_thickness_mm": id_data.get("SliceThickness"),
        "study_date": id_data.get("StudyDate"),
    }


def _fat_qc_flags(
    *,
    study_meta: dict[str, Any],
    subcutaneous_available: bool,
    torso_available: bool,
    level_status: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    missing_levels = sorted(level for level in TARGET_LEVELS if level not in level_status)
    incomplete_levels = sorted(
        level for level, payload in level_status.items() if not payload.get("vertebra_complete", False)
    )
    coverage_complete = (
        subcutaneous_available
        and torso_available
        and not missing_levels
        and not incomplete_levels
    )
    return {
        "modality": study_meta.get("modality"),
        "contrast_phase": study_meta.get("contrast_phase"),
        "slice_thickness_mm": study_meta.get("slice_thickness_mm"),
        "subcutaneous_mask_available": subcutaneous_available,
        "torso_mask_available": torso_available,
        "target_levels": TARGET_LEVELS,
        "levels_available": sorted(level_status),
        "missing_levels": missing_levels,
        "incomplete_levels": incomplete_levels,
        "coverage_complete": coverage_complete,
        "needs_manual_review": not coverage_complete,
    }


def _plot_l3_overlay(
    ct_slice: np.ndarray,
    sat_slice: np.ndarray,
    torso_slice: np.ndarray,
    out_path: Path,
    title: str,
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.figure(figsize=(7, 7))
    plt.imshow(np.rot90(ct_slice), cmap="gray", vmin=-180, vmax=220)

    sat_rot = np.rot90(sat_slice.astype(float))
    torso_rot = np.rot90(torso_slice.astype(float))
    sat_masked = np.ma.masked_where(sat_rot == 0, sat_rot)
    torso_masked = np.ma.masked_where(torso_rot == 0, torso_rot)
    plt.imshow(sat_masked, cmap="Blues", alpha=0.35, vmin=0, vmax=1)
    plt.imshow(torso_masked, cmap="autumn", alpha=0.45, vmin=0, vmax=1)

    plt.axis("off")
    plt.title(title)
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close()


def _plot_level_bars(level_measurements: dict[str, dict[str, Any]], out_path: Path, title: str) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    levels = [level for level in TARGET_LEVELS if level in level_measurements]
    sat = [level_measurements[level]["subcutaneous_fat_volume_cm3"] for level in levels]
    torso = [level_measurements[level]["torso_fat_volume_cm3"] for level in levels]

    x = np.arange(len(levels))
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.bar(x - 0.18, sat, width=0.36, color="#3c78d8", label="subcutaneous_fat")
    ax.bar(x + 0.18, torso, width=0.36, color="#e69138", label="torso_fat")
    ax.set_xticks(x, levels)
    ax.set_ylabel("Volume (cm3)")
    ax.set_title(title)
    ax.legend(loc="upper right")
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def _write_job(case_id: str, job_name: str, out_root: Path, payload: dict[str, Any] | None = None) -> Path:
    job_dir = out_root / case_id / "metrics" / job_name
    job_dir.mkdir(parents=True, exist_ok=True)
    if payload is not None:
        _save_json(job_dir / "result.json", payload)
    return job_dir


def _abdominal_fat_l3_reference(
    *,
    case_id: str,
    study_dir: Path,
    ct: np.ndarray,
    subcutaneous_mask: np.ndarray | None,
    torso_mask: np.ndarray | None,
    vertebra_path: Path | None,
    out_dir: Path,
    study_meta: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "metric_key": "abdominal_fat_l3_reference",
        "case_id": case_id,
        "status": "missing",
        "inputs": {
            "ct_path": str(study_dir / "derived"),
            "subcutaneous_mask_path": "artifacts/tissue_types/subcutaneous_fat.nii.gz",
            "torso_mask_path": "artifacts/tissue_types/torso_fat.nii.gz",
            "vertebra_mask_path": str(vertebra_path) if vertebra_path else None,
            "reference_level": "L3",
        },
        "measurement": {},
        "qc": {},
        "artifacts": {},
    }

    if subcutaneous_mask is None or torso_mask is None or vertebra_path is None or not vertebra_path.exists():
        result["qc"] = {
            "subcutaneous_mask_available": subcutaneous_mask is not None,
            "torso_mask_available": torso_mask is not None,
            "vertebra_l3_available": bool(vertebra_path and vertebra_path.exists()),
            "needs_manual_review": True,
        }
        return result

    vertebra_mask, vertebra_nii = _load_mask(vertebra_path)
    if vertebra_mask.shape != ct.shape or subcutaneous_mask.shape != ct.shape or torso_mask.shape != ct.shape:
        result["status"] = "error"
        result["qc"] = {"shape_mismatch": True, "needs_manual_review": True}
        return result

    extent = _mask_axial_extent(vertebra_mask)
    if extent is None:
        result["status"] = "indeterminate"
        result["qc"] = {"vertebra_l3_empty": True, "needs_manual_review": True}
        return result

    z_min, z_max = extent
    slice_idx = int((z_min + z_max) // 2)
    spacing = vertebra_nii.header.get_zooms()[:3]
    sat_slice = subcutaneous_mask[:, :, slice_idx]
    torso_slice = torso_mask[:, :, slice_idx]
    sat_area_cm2 = _slice_area_cm2(sat_slice, spacing[:2])
    torso_area_cm2 = _slice_area_cm2(torso_slice, spacing[:2])
    ratio = round(torso_area_cm2 / sat_area_cm2, 4) if sat_area_cm2 > 0 else None

    overlay_path = out_dir / "overlay.png"
    _plot_l3_overlay(
        ct[:, :, slice_idx],
        sat_slice,
        torso_slice,
        overlay_path,
        f"{case_id} - abdominal_fat_l3_reference",
    )

    result["status"] = "done"
    result["measurement"] = {
        "slice_index": slice_idx,
        "l3_slice_range": [z_min, z_max],
        "subcutaneous_fat_area_cm2": sat_area_cm2,
        "torso_fat_area_cm2": torso_area_cm2,
        "visceral_proxy_area_cm2": torso_area_cm2,
        "torso_to_subcutaneous_area_ratio": ratio,
    }
    result["qc"] = {
        "vertebra_complete": _mask_complete(vertebra_mask),
        "needs_manual_review": not _mask_complete(vertebra_mask),
    }
    result["artifacts"] = {"result_json": "result.json", "overlay_png": "overlay.png"}
    return result


def _abdominal_fat_t12_l5_volumetry(
    *,
    case_id: str,
    study_dir: Path,
    subcutaneous_mask: np.ndarray | None,
    torso_mask: np.ndarray | None,
    tissue_nii: nib.Nifti1Image | None,
    level_paths: dict[str, Path],
    out_dir: Path,
    study_meta: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "metric_key": "abdominal_fat_t12_l5_volumetry",
        "case_id": case_id,
        "status": "missing",
        "inputs": {
            "subcutaneous_mask_path": "artifacts/tissue_types/subcutaneous_fat.nii.gz",
            "torso_mask_path": "artifacts/tissue_types/torso_fat.nii.gz",
            "slab_definition": "vertebral_mask_axial_extent",
            "levels": TARGET_LEVELS,
        },
        "measurement": {},
        "qc": {},
        "artifacts": {},
    }

    if subcutaneous_mask is None or torso_mask is None or tissue_nii is None:
        result["qc"] = _fat_qc_flags(
            study_meta=study_meta,
            subcutaneous_available=subcutaneous_mask is not None,
            torso_available=torso_mask is not None,
            level_status={},
        )
        return result

    spacing = tissue_nii.header.get_zooms()[:3]
    if subcutaneous_mask.shape != torso_mask.shape:
        result["status"] = "error"
        result["qc"] = {"shape_mismatch": True, "needs_manual_review": True}
        return result

    per_level: dict[str, dict[str, Any]] = {}
    level_status: dict[str, dict[str, Any]] = {}

    for level in TARGET_LEVELS:
        path = level_paths.get(level)
        if path is None:
            continue
        vertebra_mask, vertebra_nii = _load_mask(path)
        if vertebra_mask.shape != subcutaneous_mask.shape:
            continue
        extent = _mask_axial_extent(vertebra_mask)
        if extent is None:
            continue

        z_min, z_max = extent
        sat_cm3 = _volume_cm3(subcutaneous_mask, spacing, (z_min, z_max))
        torso_cm3 = _volume_cm3(torso_mask, spacing, (z_min, z_max))
        ratio = round(torso_cm3 / sat_cm3, 4) if sat_cm3 > 0 else None
        world_min = float(nib.affines.apply_affine(vertebra_nii.affine, [0, 0, z_min])[2])
        world_max = float(nib.affines.apply_affine(vertebra_nii.affine, [0, 0, z_max])[2])

        per_level[level] = {
            "slice_range": [z_min, z_max],
            "world_z_mm": [round(world_min, 2), round(world_max, 2)],
            "subcutaneous_fat_volume_cm3": sat_cm3,
            "torso_fat_volume_cm3": torso_cm3,
            "visceral_proxy_volume_cm3": torso_cm3,
            "torso_to_subcutaneous_volume_ratio": ratio,
        }
        level_status[level] = {
            "vertebra_complete": _mask_complete(vertebra_mask),
        }

    qc = _fat_qc_flags(
        study_meta=study_meta,
        subcutaneous_available=True,
        torso_available=True,
        level_status=level_status,
    )
    result["qc"] = qc
    if not per_level:
        return result

    total_sat = round(sum(level["subcutaneous_fat_volume_cm3"] for level in per_level.values()), 3)
    total_torso = round(sum(level["torso_fat_volume_cm3"] for level in per_level.values()), 3)
    aggregate_ratio = round(total_torso / total_sat, 4) if total_sat > 0 else None

    profile_path = out_dir / "profile.png"
    _plot_level_bars(per_level, profile_path, f"{case_id} - abdominal_fat_t12_l5_volumetry")

    result["status"] = "done"
    result["measurement"] = {
        "levels": per_level,
        "aggregate": {
            "subcutaneous_fat_volume_cm3": total_sat,
            "torso_fat_volume_cm3": total_torso,
            "visceral_proxy_volume_cm3": total_torso,
            "torso_to_subcutaneous_volume_ratio": aggregate_ratio,
            "levels_included": [level for level in TARGET_LEVELS if level in per_level],
        },
    }
    result["artifacts"] = {"result_json": "result.json", "profile_png": "profile.png"}
    return result


def _abdominal_fat_summary(
    *,
    case_id: str,
    volumetry: dict[str, Any],
    l3_reference: dict[str, Any],
    out_dir: Path,
    study_meta: dict[str, Any],
) -> dict[str, Any]:
    aggregate = volumetry.get("measurement", {}).get("aggregate", {})
    l3 = l3_reference.get("measurement", {})
    total_sat = aggregate.get("subcutaneous_fat_volume_cm3")
    total_torso = aggregate.get("torso_fat_volume_cm3")
    total_ratio = aggregate.get("torso_to_subcutaneous_volume_ratio")
    l3_ratio = l3.get("torso_to_subcutaneous_area_ratio")

    result = {
        "metric_key": "abdominal_fat_summary",
        "case_id": case_id,
        "status": "done" if volumetry.get("status") == "done" else "missing",
        "inputs": {
            "abdominal_fat_t12_l5_volumetry": volumetry.get("metric_key"),
            "abdominal_fat_l3_reference": l3_reference.get("metric_key"),
        },
        "measurement": {
            "subcutaneous_fat_volume_cm3": total_sat,
            "torso_fat_volume_cm3": total_torso,
            "visceral_proxy_volume_cm3": total_torso,
            "torso_to_subcutaneous_volume_ratio": total_ratio,
            "l3_torso_to_subcutaneous_area_ratio": l3_ratio,
            "slab_definition": "T12-L5 vertebral-mask axial extents",
            "semantic_note": "torso_fat is preserved by source name and used as a visceral proxy inside the abdominal slabs",
        },
        "qc": {
            "coverage_complete": bool(volumetry.get("qc", {}).get("coverage_complete")),
            "needs_manual_review": bool(volumetry.get("qc", {}).get("needs_manual_review")),
            "modality": study_meta.get("modality"),
        },
        "artifacts": {"result_json": "result.json"},
    }

    return result


def _write_case_summary(
    case_id: str,
    study_dir: Path,
    outputs: dict[str, dict[str, Any]],
    out_root: Path,
    study_meta: dict[str, Any],
) -> Path:
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

    lines = [
        f"# Abdominal Fat Prototype Summary - {case_id}",
        "",
        f"- Created at: {_local_timestamp()}",
        f"- Source study: `{study_dir}`",
        "",
    ]
    for job_name, payload in outputs.items():
        lines.append(f"## {job_name}")
        lines.append(f"- status: `{payload.get('status')}`")
        lines.append(f"- metric_key: `{payload.get('metric_key')}`")
        artifacts = payload.get("artifacts", {})
        for artifact_name, rel_path in artifacts.items():
            lines.append(f"- {artifact_name}: `metrics/{job_name}/{rel_path}`")
        lines.append("")
    (summary_dir / "prototype_summary.md").write_text("\n".join(lines), encoding="utf-8")
    return summary_dir


def run_case(case_dir: Path, output_root: Path) -> dict[str, Any]:
    ctx = load_study_context(case_dir, output_root)
    ctx.output_dir.mkdir(parents=True, exist_ok=True)
    ct, _ = _load_ct(ctx.ct_path)

    subcutaneous_path = ctx.tissue_dir / "subcutaneous_fat.nii.gz"
    torso_path = ctx.tissue_dir / "torso_fat.nii.gz"
    subcutaneous_mask = torso_mask = None
    tissue_nii = None

    if subcutaneous_path.exists():
        subcutaneous_mask, tissue_nii = _load_mask(subcutaneous_path)
    if torso_path.exists():
        torso_mask, torso_nii = _load_mask(torso_path)
        tissue_nii = tissue_nii or torso_nii

    study_meta = _infer_study_metadata(ctx.id_data)
    level_paths = _level_mask_paths(ctx.total_dir)

    outputs: dict[str, dict[str, Any]] = {}

    l3_dir = _write_job(ctx.case_id, "abdominal_fat_l3_reference", output_root)
    l3_reference = _abdominal_fat_l3_reference(
        case_id=ctx.case_id,
        study_dir=ctx.study_dir,
        ct=ct,
        subcutaneous_mask=subcutaneous_mask,
        torso_mask=torso_mask,
        vertebra_path=level_paths.get("L3"),
        out_dir=l3_dir,
        study_meta=study_meta,
    )
    _save_json(l3_dir / "result.json", l3_reference)
    outputs["abdominal_fat_l3_reference"] = l3_reference

    volumetry_dir = _write_job(ctx.case_id, "abdominal_fat_t12_l5_volumetry", output_root)
    volumetry = _abdominal_fat_t12_l5_volumetry(
        case_id=ctx.case_id,
        study_dir=ctx.study_dir,
        subcutaneous_mask=subcutaneous_mask,
        torso_mask=torso_mask,
        tissue_nii=tissue_nii,
        level_paths=level_paths,
        out_dir=volumetry_dir,
        study_meta=study_meta,
    )
    _save_json(volumetry_dir / "result.json", volumetry)
    outputs["abdominal_fat_t12_l5_volumetry"] = volumetry

    summary_dir = _write_job(ctx.case_id, "abdominal_fat_summary", output_root)
    summary = _abdominal_fat_summary(
        case_id=ctx.case_id,
        volumetry=volumetry,
        l3_reference=l3_reference,
        out_dir=summary_dir,
        study_meta=study_meta,
    )
    _save_json(summary_dir / "result.json", summary)
    outputs["abdominal_fat_summary"] = summary

    _write_case_summary(ctx.case_id, ctx.study_dir, outputs, output_root, study_meta)
    return outputs


def _generate_synthetic_case() -> tuple[np.ndarray, nib.Nifti1Image, np.ndarray, np.ndarray, dict[str, np.ndarray]]:
    shape = (72, 72, 24)
    ct = np.full(shape, -60.0, dtype=np.float32)
    subcutaneous = np.zeros(shape, dtype=bool)
    torso = np.zeros(shape, dtype=bool)
    vertebrae: dict[str, np.ndarray] = {}

    yy, xx = np.ogrid[: shape[0], : shape[1]]
    sat_ring = (((xx - 36) ** 2 + (yy - 36) ** 2) <= 28**2) & (((xx - 36) ** 2 + (yy - 36) ** 2) >= 22**2)
    torso_core = ((xx - 36) ** 2 / 16.0**2) + ((yy - 36) ** 2 / 12.0**2) <= 1
    level_slices = {
        "T12": (2, 4),
        "L1": (5, 7),
        "L2": (8, 10),
        "L3": (11, 13),
        "L4": (14, 16),
        "L5": (17, 19),
    }

    for level, (z_min, z_max) in level_slices.items():
        vertebra = np.zeros(shape, dtype=bool)
        for z in range(z_min, z_max + 1):
            subcutaneous[:, :, z] = sat_ring
            torso[:, :, z] = torso_core
            vertebra[30:42, 31:41, z] = True
            ct[:, :, z][sat_ring] = -100.0
            ct[:, :, z][torso_core] = -95.0
        vertebrae[level] = vertebra

    nii = nib.Nifti1Image(ct, affine=np.eye(4))
    return ct, nii, subcutaneous, torso, vertebrae


def run_self_test() -> None:
    ct, ct_nii, subcutaneous, torso, vertebrae = _generate_synthetic_case()
    tmp = Path.cwd() / "_abdominal_fat_self_test"
    study_dir = tmp / "study"
    tissue_dir = study_dir / "artifacts" / "tissue_types"
    total_dir = study_dir / "artifacts" / "total"
    derived_dir = study_dir / "derived"
    (study_dir / "metadata").mkdir(parents=True, exist_ok=True)
    tissue_dir.mkdir(parents=True, exist_ok=True)
    total_dir.mkdir(parents=True, exist_ok=True)
    derived_dir.mkdir(parents=True, exist_ok=True)

    nib.save(ct_nii, derived_dir / "synthetic.nii.gz")
    nib.save(nib.Nifti1Image(subcutaneous.astype(np.uint8), affine=np.eye(4)), tissue_dir / "subcutaneous_fat.nii.gz")
    nib.save(nib.Nifti1Image(torso.astype(np.uint8), affine=np.eye(4)), tissue_dir / "torso_fat.nii.gz")
    for level, mask in vertebrae.items():
        nib.save(nib.Nifti1Image(mask.astype(np.uint8), affine=np.eye(4)), total_dir / f"vertebrae_{level}.nii.gz")
    (study_dir / "metadata" / "id.json").write_text(json.dumps({"CaseID": "synthetic", "Modality": "CT"}), encoding="utf-8")

    outputs = run_case(study_dir, tmp / "out")
    aggregate = outputs["abdominal_fat_t12_l5_volumetry"]["measurement"]["aggregate"]
    assert aggregate["subcutaneous_fat_volume_cm3"] > 0
    assert aggregate["torso_fat_volume_cm3"] > 0
    assert outputs["abdominal_fat_l3_reference"]["measurement"]["subcutaneous_fat_area_cm2"] > 0
    print("self-test: ok")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prototype abdominal fat jobs from segmented Heimdallr studies.")
    parser.add_argument("paths", nargs="*", type=Path, help="Study directories, id.json files, or a directory containing studies.")
    parser.add_argument("--input-root", type=Path, default=Path("runtime/studies"), help="Default root to scan when no explicit paths are passed.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT, help="Where prototype artifacts are written.")
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
            outputs = run_case(case_dir, args.output_root)
            index.append(
                {
                    "case_id": case_dir.name,
                    "study_dir": str(case_dir),
                    "jobs": {name: payload.get("status") for name, payload in outputs.items()},
                }
            )
            print(f"[ok] {case_dir.name}")
        except Exception as exc:
            index.append({"case_id": case_dir.name, "study_dir": str(case_dir), "error": str(exc)})
            print(f"[error] {case_dir.name}: {exc}")

    _save_json(args.output_root / "index.json", {"created_at": _local_timestamp(), "cases": index})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
