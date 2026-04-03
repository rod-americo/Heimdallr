#!/usr/bin/env python3
"""Shared helpers for abdominal body-fat metric jobs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np

from heimdallr.metrics.jobs._bone_job_common import (
    center_slice_index,
    load_ct_volume,
    load_job_config,
    load_nifti_mask,
    mask_complete,
    metric_output_dir,
    parse_args,
    read_json,
    resolve_canonical_nifti,
    write_payload,
)
from heimdallr.shared.paths import study_artifacts_dir, study_id_json, study_metadata_json, study_results_json


TARGET_LEVELS = ("T12", "L1", "L2", "L3", "L4", "L5")


def load_case_json_bundle(case_id: str) -> dict[str, Any]:
    return {
        "id_json": read_json(study_id_json(case_id)),
        "metadata_json": read_json(study_metadata_json(case_id)),
        "results_json": read_json(study_results_json(case_id)),
    }


def mask_axial_extent(mask: np.ndarray) -> tuple[int, int] | None:
    z_indices = np.where(np.asarray(mask, dtype=bool).sum(axis=(0, 1)) > 0)[0]
    if len(z_indices) == 0:
        return None
    return int(z_indices[0]), int(z_indices[-1])


def slice_area_cm2(mask_2d: np.ndarray, spacing_xy: tuple[float, float]) -> float:
    pixel_area_mm2 = float(spacing_xy[0] * spacing_xy[1])
    return round(float(np.count_nonzero(np.asarray(mask_2d, dtype=bool))) * pixel_area_mm2 / 100.0, 3)


def slab_volume_cm3(mask: np.ndarray, spacing_xyz: tuple[float, float, float], z_range: tuple[int, int]) -> float:
    z_min, z_max = z_range
    voxel_volume_mm3 = float(spacing_xyz[0] * spacing_xyz[1] * spacing_xyz[2])
    slab = np.asarray(mask[:, :, z_min : z_max + 1], dtype=bool)
    return round(float(np.count_nonzero(slab)) * voxel_volume_mm3 / 1000.0, 3)


def longest_contiguous_block(levels: list[str] | tuple[str, ...], ordered_levels: tuple[str, ...] = TARGET_LEVELS) -> list[str]:
    level_set = set(levels)
    best: list[str] = []
    current: list[str] = []
    for level in ordered_levels:
        if level in level_set:
            current.append(level)
        else:
            if len(current) > len(best):
                best = current[:]
            current = []
    if len(current) > len(best):
        best = current[:]
    return best


def resolve_body_fat_inputs(case_id: str) -> dict[str, Path]:
    artifacts_dir = study_artifacts_dir(case_id)
    return {
        "artifacts_dir": artifacts_dir,
        "ct_path": resolve_canonical_nifti(case_id),
        "subcutaneous_fat_path": artifacts_dir / "tissue_types" / "subcutaneous_fat.nii.gz",
        "torso_fat_path": artifacts_dir / "tissue_types" / "torso_fat.nii.gz",
        "l3_path": artifacts_dir / "total" / "vertebrae_L3.nii.gz",
        "level_paths": {
            level: artifacts_dir / "total" / f"vertebrae_{level}.nii.gz"
            for level in TARGET_LEVELS
        },
    }


def compute_level_measurements(
    level_masks: dict[str, np.ndarray],
    sat_mask: np.ndarray,
    torso_mask: np.ndarray,
    spacing_xyz: tuple[float, float, float],
) -> tuple[dict[str, dict[str, Any]], list[str], list[str]]:
    level_measurements: dict[str, dict[str, Any]] = {}
    complete_levels: list[str] = []
    measurable_levels: list[str] = []

    for level in TARGET_LEVELS:
        vertebra_mask = level_masks.get(level)
        if vertebra_mask is None:
            continue

        extent = mask_axial_extent(vertebra_mask)
        completeness = mask_complete(vertebra_mask)
        payload: dict[str, Any] = {
            "vertebra_complete": completeness,
            "slice_range": None,
            "subcutaneous_fat_volume_cm3": None,
            "torso_fat_volume_cm3": None,
            "visceral_proxy_volume_cm3": None,
            "torso_to_subcutaneous_volume_ratio": None,
        }

        if extent is not None:
            measurable_levels.append(level)
            sat_volume = slab_volume_cm3(sat_mask, spacing_xyz, extent)
            torso_volume = slab_volume_cm3(torso_mask, spacing_xyz, extent)
            ratio = round(torso_volume / sat_volume, 4) if sat_volume > 0 else None
            payload.update(
                {
                    "slice_range": [int(extent[0]), int(extent[1])],
                    "subcutaneous_fat_volume_cm3": sat_volume,
                    "torso_fat_volume_cm3": torso_volume,
                    "visceral_proxy_volume_cm3": torso_volume,
                    "torso_to_subcutaneous_volume_ratio": ratio,
                }
            )
        if completeness:
            complete_levels.append(level)
        level_measurements[level] = payload

    return level_measurements, complete_levels, measurable_levels


def build_abdominal_aggregate(
    level_measurements: dict[str, dict[str, Any]],
    complete_levels: list[str],
    measurable_levels: list[str],
    sat_mask: np.ndarray,
    torso_mask: np.ndarray,
    spacing_xyz: tuple[float, float, float],
) -> dict[str, Any]:
    preferred_levels = longest_contiguous_block(complete_levels)
    used_incomplete_levels = False
    if not preferred_levels:
        preferred_levels = longest_contiguous_block(measurable_levels)
        used_incomplete_levels = True
    if not preferred_levels:
        return {
            "job_status": "no_measurable_levels",
            "coverage_complete": False,
        }

    starts = [int(level_measurements[level]["slice_range"][0]) for level in preferred_levels]
    ends = [int(level_measurements[level]["slice_range"][1]) for level in preferred_levels]
    z_range = (min(starts), max(ends))
    sat_volume = slab_volume_cm3(sat_mask, spacing_xyz, z_range)
    torso_volume = slab_volume_cm3(torso_mask, spacing_xyz, z_range)
    ratio = round(torso_volume / sat_volume, 4) if sat_volume > 0 else None
    coverage_complete = preferred_levels == list(TARGET_LEVELS) and not used_incomplete_levels

    return {
        "job_status": "complete" if coverage_complete else "partial",
        "coverage_complete": coverage_complete,
        "measured_region": f"{preferred_levels[0]}-{preferred_levels[-1]}",
        "levels_used": preferred_levels,
        "slice_range": [int(z_range[0]), int(z_range[1])],
        "slice_count": int(z_range[1] - z_range[0] + 1),
        "subcutaneous_fat_volume_cm3": sat_volume,
        "torso_fat_volume_cm3": torso_volume,
        "visceral_proxy_volume_cm3": torso_volume,
        "torso_to_subcutaneous_volume_ratio": ratio,
        "used_incomplete_levels": used_incomplete_levels,
    }


def save_l3_overlay(
    ct_slice: np.ndarray,
    sat_slice: np.ndarray,
    torso_slice: np.ndarray,
    l3_slice: np.ndarray,
    output_path: Path,
    summary_lines: list[str],
) -> None:
    rotated_ct = np.rot90(np.asarray(ct_slice, dtype=np.float32))
    rotated_sat = np.rot90(np.asarray(sat_slice, dtype=bool))
    rotated_torso = np.rot90(np.asarray(torso_slice, dtype=bool))
    rotated_l3 = np.rot90(np.asarray(l3_slice, dtype=bool))

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.imshow(np.clip(rotated_ct, -180.0, 240.0), cmap="gray", interpolation="nearest")

    if rotated_sat.any():
        sat_masked = np.ma.masked_where(~rotated_sat, rotated_sat.astype(np.uint8))
        ax.imshow(sat_masked, cmap="Blues", alpha=0.30, interpolation="nearest")
        ax.contour(rotated_sat, levels=[0.5], colors=["#6ea8fe"], linewidths=1.0)

    if rotated_torso.any():
        torso_masked = np.ma.masked_where(~rotated_torso, rotated_torso.astype(np.uint8))
        ax.imshow(torso_masked, cmap="autumn", alpha=0.42, interpolation="nearest")
        ax.contour(rotated_torso, levels=[0.5], colors=["#ffb000"], linewidths=1.1)

    if rotated_l3.any():
        ax.contour(rotated_l3, levels=[0.5], colors=["#00d5ff"], linewidths=0.9)

    ax.set_title("L3 Body Fat Reference", fontsize=14)
    ax.text(
        0.03,
        0.97,
        "\n".join(summary_lines),
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=10,
        color="white",
        bbox={"boxstyle": "round,pad=0.4", "facecolor": "black", "alpha": 0.55, "edgecolor": "none"},
    )
    ax.axis("off")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)


def save_volumetry_profile(
    level_measurements: dict[str, dict[str, Any]],
    aggregate: dict[str, Any],
    output_path: Path,
) -> None:
    labels = [level for level in TARGET_LEVELS if level in level_measurements]
    sat_values = [level_measurements[level]["subcutaneous_fat_volume_cm3"] or 0.0 for level in labels]
    torso_values = [level_measurements[level]["torso_fat_volume_cm3"] or 0.0 for level in labels]
    colors = [
        "#2563eb" if level_measurements[level].get("vertebra_complete") else "#9ca3af"
        for level in labels
    ]

    x = np.arange(len(labels))
    width = 0.38
    fig, (ax0, ax1) = plt.subplots(2, 1, figsize=(10, 8), gridspec_kw={"height_ratios": [1, 2]})

    summary_lines = [
        f"Status: {aggregate.get('job_status', '-')}",
        f"Region: {aggregate.get('measured_region', '-')}",
        f"Coverage complete: {aggregate.get('coverage_complete')}",
        f"Torso fat: {aggregate.get('torso_fat_volume_cm3', '-')}",
        f"Subcutaneous fat: {aggregate.get('subcutaneous_fat_volume_cm3', '-')}",
        f"Ratio: {aggregate.get('torso_to_subcutaneous_volume_ratio', '-')}",
    ]
    ax0.axis("off")
    ax0.text(0.01, 0.95, "\n".join(str(line) for line in summary_lines), va="top", ha="left", family="monospace", fontsize=11)

    ax1.bar(x - width / 2, torso_values, width=width, label="Torso fat / visceral proxy", color="#ef4444")
    ax1.bar(x + width / 2, sat_values, width=width, label="Subcutaneous fat", color=colors)
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels)
    ax1.set_ylabel("Volume (cm3)")
    ax1.set_title("Abdominal body fat by vertebral slab")
    ax1.legend()
    ax1.grid(axis="y", alpha=0.2)

    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)
