#!/usr/bin/env python3
"""Prototype runner for abdominal fat compartment metrics.

This script is intentionally self-contained and does not modify the production
metrics pipeline. It reads an already-segmented Heimdallr study folder and
exports:

* a JSON summary with slab-based volume measurements for `torso_fat` and
  `subcutaneous_fat`
* a PNG overview for quick manual inspection

The default slab plan uses vertebral mid-slices from T12 to L5 as anatomical
delimiters. If landmarks are missing, the script falls back to a single
full-volume slab so it still produces a usable prototype artifact.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import nibabel as nib
import numpy as np


DEFAULT_OUTPUT_ROOT = Path.home() / "Temp" / "lab-gordura"
DEFAULT_LANDMARKS = ("T12", "L1", "L2", "L3", "L4", "L5")
FAT_COMPARTMENTS = {
    "torso_fat": "torso_fat",
    "subcutaneous_fat": "subcutaneous_fat",
}


@dataclass(frozen=True)
class SlabSpec:
    label: str
    start_slice: int
    end_slice: int
    start_mm: float
    end_mm: float


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def resolve_case_dir(case_folder: Path) -> Path:
    case_folder = Path(case_folder).expanduser().resolve()
    if (case_folder / "metadata" / "id.json").exists():
        return case_folder
    if (case_folder / "id.json").exists():
        return case_folder.parent
    raise FileNotFoundError(f"Could not find metadata/id.json in {case_folder}")


def load_case_context(case_dir: Path) -> dict[str, Any]:
    metadata_dir = case_dir / "metadata"
    id_data = load_json(metadata_dir / "id.json")
    case_id = str(id_data.get("CaseID") or case_dir.name)

    derived_dir = case_dir / "derived"
    ct_path = derived_dir / f"{case_id}.nii.gz"
    if not ct_path.exists():
        candidates = sorted(derived_dir.glob("*.nii.gz"))
        if len(candidates) == 1:
            ct_path = candidates[0]
        else:
            raise FileNotFoundError(f"Could not resolve CT volume in {derived_dir}")

    total_dir = case_dir / "artifacts" / "total"
    tissue_dir = case_dir / "artifacts" / "tissue_types"
    if not total_dir.exists():
        raise FileNotFoundError(f"Missing total masks directory: {total_dir}")
    if not tissue_dir.exists():
        raise FileNotFoundError(f"Missing tissue_types directory: {tissue_dir}")

    return {
        "case_id": case_id,
        "case_dir": case_dir,
        "id_data": id_data,
        "ct_path": ct_path,
        "total_dir": total_dir,
        "tissue_dir": tissue_dir,
    }


def load_bool_nifti(path: Path) -> tuple[np.ndarray, nib.Nifti1Image]:
    nii = nib.load(str(path))
    return np.asanyarray(nii.dataobj) > 0, nii


def find_first_existing_path(base_dir: Path, candidates: list[str]) -> Path | None:
    for rel_path in candidates:
        candidate = base_dir / rel_path
        if candidate.exists():
            return candidate
    return None


def find_landmark_mid_slice(total_dir: Path, landmark: str) -> int | None:
    path = find_first_existing_path(
        total_dir,
        [
            f"vertebrae_{landmark}.nii.gz",
            f"total/vertebrae_{landmark}.nii.gz",
        ],
    )
    if path is None:
        return None

    mask, _ = load_bool_nifti(path)
    z_indices = np.where(mask.sum(axis=(0, 1)) > 0)[0]
    if z_indices.size == 0:
        return None
    return int(z_indices[z_indices.size // 2])


def build_slab_plan(
    landmark_slices: dict[str, int],
    z_size: int,
    spacing_z_mm: float,
    landmark_order: tuple[str, ...] = DEFAULT_LANDMARKS,
) -> list[SlabSpec]:
    ordered = [(label, landmark_slices[label]) for label in landmark_order if label in landmark_slices]
    ordered = sorted(ordered, key=lambda item: item[1])
    if len(ordered) < 2:
        return [
            SlabSpec(
                label="full_volume",
                start_slice=0,
                end_slice=z_size,
                start_mm=0.0,
                end_mm=float(z_size * spacing_z_mm),
            )
        ]

    slabs: list[SlabSpec] = []
    for (left_label, left_slice), (right_label, right_slice) in zip(ordered, ordered[1:]):
        start_slice = int(max(0, min(left_slice, z_size)))
        end_slice = int(max(start_slice + 1, min(right_slice, z_size)))
        slabs.append(
            SlabSpec(
                label=f"{left_label}_to_{right_label}",
                start_slice=start_slice,
                end_slice=end_slice,
                start_mm=float(start_slice * spacing_z_mm),
                end_mm=float(end_slice * spacing_z_mm),
            )
        )
    return slabs


def calculate_compartment_volume(mask: np.ndarray, spacing_mm: tuple[float, float, float]) -> dict[str, Any]:
    mask_bool = np.asarray(mask, dtype=bool)
    voxel_vol_mm3 = float(spacing_mm[0] * spacing_mm[1] * spacing_mm[2])
    voxel_count = int(mask_bool.sum())
    volume_mm3 = float(voxel_count * voxel_vol_mm3)
    return {
        "voxel_count": voxel_count,
        "volume_mm3": round(volume_mm3, 3),
        "volume_cm3": round(volume_mm3 / 1000.0, 3),
    }


def calculate_slab_metrics(
    mask: np.ndarray,
    spacing_mm: tuple[float, float, float],
    slabs: list[SlabSpec],
) -> dict[str, Any]:
    mask_bool = np.asarray(mask, dtype=bool)
    voxel_vol_mm3 = float(spacing_mm[0] * spacing_mm[1] * spacing_mm[2])
    slab_rows: list[dict[str, Any]] = []
    total_voxels = 0

    for slab in slabs:
        slab_mask = mask_bool[:, :, slab.start_slice:slab.end_slice]
        voxel_count = int(slab_mask.sum())
        volume_mm3 = float(voxel_count * voxel_vol_mm3)
        slab_rows.append(
            {
                "label": slab.label,
                "start_slice": slab.start_slice,
                "end_slice": slab.end_slice,
                "start_mm": round(slab.start_mm, 3),
                "end_mm": round(slab.end_mm, 3),
                "voxel_count": voxel_count,
                "volume_mm3": round(volume_mm3, 3),
                "volume_cm3": round(volume_mm3 / 1000.0, 3),
            }
        )
        total_voxels += voxel_count

    total_volume_mm3 = float(total_voxels * voxel_vol_mm3)
    return {
        "voxel_count": total_voxels,
        "volume_mm3": round(total_volume_mm3, 3),
        "volume_cm3": round(total_volume_mm3 / 1000.0, 3),
        "slabs": slab_rows,
    }


def build_overview_png(
    ct: np.ndarray,
    ct_spacing: tuple[float, float, float],
    anchor_slice: int,
    slabs: list[SlabSpec],
    compartment_metrics: dict[str, dict[str, Any]],
    output_path: Path,
) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        try:
            from PIL import Image, ImageDraw

            image = Image.new("RGB", (1200, 700), "white")
            draw = ImageDraw.Draw(image)
            lines = [
                "Fat metrics prototype",
                f"Anchor slice: {anchor_slice}",
                f"Z spacing: {ct_spacing[2]:.2f} mm",
                f"Landmarks: {len(slabs)} slabs",
                f"torso_fat total: {compartment_metrics['torso_fat']['volume_cm3']:.3f} cm3",
                f"subcutaneous_fat total: {compartment_metrics['subcutaneous_fat']['volume_cm3']:.3f} cm3",
            ]
            y = 60
            for line in lines:
                draw.text((60, y), line, fill="black")
                y += 44
            output_path.parent.mkdir(parents=True, exist_ok=True)
            image.save(output_path)
        except Exception:
            return
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig = plt.figure(figsize=(14, 8))
    gs = fig.add_gridspec(2, 2, height_ratios=[1, 1.1])

    ax_slice = fig.add_subplot(gs[:, 0])
    slice_img = np.rot90(ct[:, :, anchor_slice])
    ax_slice.imshow(slice_img, cmap="gray", vmin=np.percentile(ct, 5), vmax=np.percentile(ct, 95))
    ax_slice.set_title(f"Anchor slice z={anchor_slice}")
    ax_slice.axis("off")
    slab_lines = [f"{slab.label}: {slab.start_slice}-{slab.end_slice}" for slab in slabs]
    ax_slice.text(
        0.02,
        0.02,
        "\n".join(slab_lines),
        transform=ax_slice.transAxes,
        fontsize=9,
        color="cyan",
        va="bottom",
        ha="left",
        bbox={"facecolor": "black", "alpha": 0.5, "pad": 6, "edgecolor": "none"},
    )

    ax_summary = fig.add_subplot(gs[0, 1])
    labels = [slab.label for slab in slabs]
    x = np.arange(len(labels))
    vat = [item["volume_cm3"] for item in compartment_metrics["torso_fat"]["slabs"]]
    sat = [item["volume_cm3"] for item in compartment_metrics["subcutaneous_fat"]["slabs"]]
    ax_summary.bar(x - 0.2, vat, width=0.4, label="torso_fat")
    ax_summary.bar(x + 0.2, sat, width=0.4, label="subcutaneous_fat")
    ax_summary.set_xticks(x)
    ax_summary.set_xticklabels(labels, rotation=20, ha="right")
    ax_summary.set_ylabel("Volume (cm3)")
    ax_summary.set_title("Slab volumes")
    ax_summary.legend()

    ax_totals = fig.add_subplot(gs[1, 1])
    totals = [
        ("torso_fat", compartment_metrics["torso_fat"]["volume_cm3"]),
        ("subcutaneous_fat", compartment_metrics["subcutaneous_fat"]["volume_cm3"]),
    ]
    ax_totals.bar([t[0] for t in totals], [t[1] for t in totals], color=["#d95f02", "#1b9e77"])
    ax_totals.set_ylabel("Volume (cm3)")
    ax_totals.set_title("Total block volume")
    for idx, (_, value) in enumerate(totals):
        ax_totals.text(idx, value, f"{value:.1f}", ha="center", va="bottom")

    fig.suptitle(f"Fat compartment prototype | spacing_z={ct_spacing[2]:.2f} mm", fontsize=14)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def run_case(case_folder: Path, output_root: Path) -> dict[str, Any]:
    case_dir = resolve_case_dir(case_folder)
    context = load_case_context(case_dir)
    ct_nii = nib.load(str(context["ct_path"]))
    ct = ct_nii.get_fdata(dtype=np.float32)
    spacing_mm = ct_nii.header.get_zooms()[:3]
    z_size = ct.shape[2]

    landmark_slices = {
        label: slice_idx
        for label in DEFAULT_LANDMARKS
        if (slice_idx := find_landmark_mid_slice(context["total_dir"], label)) is not None
    }

    slabs = build_slab_plan(landmark_slices, z_size=z_size, spacing_z_mm=float(spacing_mm[2]))
    fat_results: dict[str, dict[str, Any]] = {}
    for key, compartment_name in FAT_COMPARTMENTS.items():
        mask_path = context["tissue_dir"] / f"{compartment_name}.nii.gz"
        if not mask_path.exists():
            fat_results[key] = {
                "available": False,
                "total_voxels": 0,
                "volume_mm3": 0.0,
                "volume_cm3": 0.0,
                "slabs": [],
            }
            continue
        mask, _ = load_bool_nifti(mask_path)
        if mask.shape != ct.shape:
            raise ValueError(f"Shape mismatch for {mask_path.name}: {mask.shape} vs {ct.shape}")
        total_metrics = calculate_compartment_volume(mask, spacing_mm)
        slab_metrics = calculate_slab_metrics(mask, spacing_mm, slabs)
        fat_results[key] = {
            "available": True,
            **total_metrics,
            "slabs": slab_metrics["slabs"],
        }

    summary = {
        "case_id": context["case_id"],
        "case_dir": str(case_dir),
        "ct_path": str(context["ct_path"]),
        "landmarks": landmark_slices,
        "slab_plan": [asdict(slab) for slab in slabs],
        "fat_compartments": fat_results,
        "qc": {
            "anchor_strategy": "vertebral_mid_slices",
            "landmark_count": len(landmark_slices),
            "fallback_used": len(slabs) == 1 and slabs[0].label == "full_volume",
            "ct_spacing_mm": [float(x) for x in spacing_mm[:3]],
        },
    }

    case_output = output_root.expanduser().resolve() / context["case_id"] / "fat_metrics"
    case_output.mkdir(parents=True, exist_ok=True)
    save_json(case_output / "summary.json", summary)
    anchor_slice = slabs[len(slabs) // 2].start_slice if slabs and slabs[0].label != "full_volume" else z_size // 2
    build_overview_png(
        ct=ct,
        ct_spacing=spacing_mm[:3],
        anchor_slice=anchor_slice,
        slabs=slabs,
        compartment_metrics=fat_results,
        output_path=case_output / "overview.png",
    )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prototype abdominal fat metrics runner.")
    parser.add_argument("case_folder", type=Path, help="Heimdallr case folder")
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help=f"Output root directory (default: {DEFAULT_OUTPUT_ROOT})",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    summary = run_case(args.case_folder, args.output_dir)
    print(json.dumps(summary, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
