#!/usr/bin/env python3
"""Prototype runner for opportunistic abdominal body-fat jobs."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import nibabel as nib
import numpy as np

from heimdallr.processing.body_fat import (
    ABDOMINAL_VERTEBRA_LEVELS,
    build_abdominal_slabs,
    compute_l3_slice_fat_areas,
    calculate_body_fat_distribution,
)


DEFAULT_OUTPUT_ROOT = Path.home() / "Temp" / "lab-gordura"


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
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def _save_markdown(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _timestamp() -> str:
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
            candidate = path.parent.parent
            if candidate not in seen:
                discovered.append(candidate)
                seen.add(candidate)
            continue

        case_dir = _ensure_case_dir(path)
        if case_dir is not None:
            if case_dir not in seen:
                discovered.append(case_dir)
                seen.add(case_dir)
            continue

        if path.is_dir():
            for child in sorted(p for p in path.iterdir() if p.is_dir()):
                case_dir = _ensure_case_dir(child)
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
        derived_files = sorted(case_dir.glob("derived/*.nii.gz"))
        if len(derived_files) == 1:
            ct_path = derived_files[0]

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


def _load_mask(path: Path) -> tuple[np.ndarray, nib.Nifti1Image]:
    nii = nib.load(str(path))
    return np.asanyarray(nii.dataobj) > 0, nii


def _load_ct(path: Path) -> tuple[np.ndarray, nib.Nifti1Image]:
    nii = nib.load(str(path))
    return nii.get_fdata(dtype=np.float32), nii


def _relative_to_case(case_dir: Path, path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return str(path.relative_to(case_dir))
    except ValueError:
        return str(path)


def _render_sagittal_profile(
    ct: np.ndarray,
    slabs: dict[str, Any],
    output_path: Path,
) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    sagittal_idx = ct.shape[0] // 2
    sagittal = np.rot90(ct[sagittal_idx, :, :])

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.imshow(sagittal, cmap="gray", vmin=-150, vmax=250)

    if slabs:
        z_size = ct.shape[2]
        for level in ABDOMINAL_VERTEBRA_LEVELS:
            slab = slabs.get(level)
            if not slab:
                continue
            start = int(slab["start_slice"])
            end = int(slab["end_slice"])
            center = float(slab["center_slice"])
            row_start = z_size - 1 - end
            row_end = z_size - 1 - start
            ax.axhspan(row_start, row_end, alpha=0.08, color="#00897b")
            ax.axhline(z_size - 1 - center, color="#ef6c00", linewidth=0.8)
            ax.text(
                3,
                z_size - 1 - center,
                level,
                color="white",
                fontsize=8,
                va="center",
                bbox={"facecolor": "#263238", "alpha": 0.7, "pad": 2},
            )

    ax.set_title("Abdominal fat slab profile (vertebra-anchored)")
    ax.axis("off")
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def _render_l3_overlay(
    ct: np.ndarray,
    sat_mask: np.ndarray,
    torso_mask: np.ndarray,
    slice_index: int,
    output_path: Path,
) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ct_slice = np.rot90(ct[:, :, slice_index])
    sat_slice = np.rot90(sat_mask[:, :, slice_index])
    torso_slice = np.rot90(torso_mask[:, :, slice_index])

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.imshow(ct_slice, cmap="gray", vmin=-150, vmax=250)

    sat_overlay = np.ma.masked_where(~sat_slice.astype(bool), sat_slice)
    torso_overlay = np.ma.masked_where(~torso_slice.astype(bool), torso_slice)

    ax.imshow(sat_overlay, cmap="Greens", alpha=0.35, vmin=0, vmax=1)
    ax.imshow(torso_overlay, cmap="autumn", alpha=0.35, vmin=0, vmax=1)
    ax.set_title(f"L3 fat overlay (z={slice_index})")
    ax.axis("off")
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def run_body_fat_abdominal_volumes(context: StudyContext) -> dict[str, Any]:
    job_dir = context.output_dir / "metrics" / "body_fat_abdominal_volumes"
    result_json = job_dir / "result.json"
    sagittal_png = job_dir / "sagittal_profile.png"

    sat_path = context.tissue_dir / "subcutaneous_fat.nii.gz"
    torso_path = context.tissue_dir / "torso_fat.nii.gz"
    required = {"subcutaneous_fat": sat_path, "torso_fat": torso_path}
    missing_inputs = [name for name, path in required.items() if not path.exists()]

    result: dict[str, Any] = {
        "metric_key": "body_fat_abdominal_volumes",
        "status": "missing" if missing_inputs else "done",
        "case_id": context.case_id,
        "generated_at": _timestamp(),
        "inputs": {
            "canonical_nifti": _relative_to_case(context.study_dir, context.ct_path),
            "subcutaneous_fat_mask": _relative_to_case(context.study_dir, sat_path if sat_path.exists() else None),
            "torso_fat_mask": _relative_to_case(context.study_dir, torso_path if torso_path.exists() else None),
            "vertebrae_dir": _relative_to_case(context.study_dir, context.total_dir),
        },
        "measurement": {},
        "qc": {
            "missing_inputs": missing_inputs,
            "needs_manual_review": bool(missing_inputs),
        },
        "artifacts": {
            "result_json": "result.json",
            "sagittal_profile_png": "sagittal_profile.png",
        },
    }
    if missing_inputs:
        _save_json(result_json, result)
        return result

    ct, _ = _load_ct(context.ct_path)
    sat_mask, sat_nii = _load_mask(sat_path)
    torso_mask, _ = _load_mask(torso_path)

    if sat_mask.shape != torso_mask.shape or sat_mask.shape != ct.shape:
        result["status"] = "error"
        result["error"] = f"Shape mismatch: CT {ct.shape}, SAT {sat_mask.shape}, torso {torso_mask.shape}"
        result["qc"]["needs_manual_review"] = True
        _save_json(result_json, result)
        return result

    vertebra_masks: dict[str, np.ndarray] = {}
    for level in ABDOMINAL_VERTEBRA_LEVELS:
        vertebra_path = context.total_dir / f"vertebrae_{level}.nii.gz"
        if vertebra_path.exists():
            vertebra_masks[level], _ = _load_mask(vertebra_path)

    slab_definition = build_abdominal_slabs(vertebra_masks, z_size=ct.shape[2])
    distribution = calculate_body_fat_distribution(
        subcutaneous_fat_mask=sat_mask,
        torso_fat_mask=torso_mask,
        spacing_mm=sat_nii.header.get_zooms(),
        slab_definition=slab_definition,
    )

    result["measurement"] = distribution
    result["qc"].update(
        {
            "available_levels": slab_definition["available_levels"],
            "missing_levels": slab_definition["missing_levels"],
            "coverage_complete": slab_definition["coverage_complete"],
            "slab_strategy": slab_definition["strategy"],
            "needs_manual_review": bool(
                missing_inputs or distribution["aggregate"]["needs_manual_review"]
            ),
        }
    )

    if slab_definition["slabs"]:
        _render_sagittal_profile(ct, slab_definition["slabs"], sagittal_png)

    _save_json(result_json, result)
    return result


def run_body_fat_l3_slice(context: StudyContext) -> dict[str, Any]:
    job_dir = context.output_dir / "metrics" / "body_fat_l3_slice"
    result_json = job_dir / "result.json"
    overlay_png = job_dir / "overlay.png"

    sat_path = context.tissue_dir / "subcutaneous_fat.nii.gz"
    torso_path = context.tissue_dir / "torso_fat.nii.gz"
    l3_path = context.total_dir / "vertebrae_L3.nii.gz"
    required = {
        "subcutaneous_fat": sat_path,
        "torso_fat": torso_path,
        "vertebrae_L3": l3_path,
    }
    missing_inputs = [name for name, path in required.items() if not path.exists()]

    result: dict[str, Any] = {
        "metric_key": "body_fat_l3_slice",
        "status": "missing" if missing_inputs else "done",
        "case_id": context.case_id,
        "generated_at": _timestamp(),
        "inputs": {
            "canonical_nifti": _relative_to_case(context.study_dir, context.ct_path),
            "subcutaneous_fat_mask": _relative_to_case(context.study_dir, sat_path if sat_path.exists() else None),
            "torso_fat_mask": _relative_to_case(context.study_dir, torso_path if torso_path.exists() else None),
            "vertebra_l3_mask": _relative_to_case(context.study_dir, l3_path if l3_path.exists() else None),
        },
        "measurement": {},
        "qc": {
            "missing_inputs": missing_inputs,
            "needs_manual_review": bool(missing_inputs),
        },
        "artifacts": {
            "result_json": "result.json",
            "overlay_png": "overlay.png",
        },
    }
    if missing_inputs:
        _save_json(result_json, result)
        return result

    ct, _ = _load_ct(context.ct_path)
    sat_mask, sat_nii = _load_mask(sat_path)
    torso_mask, _ = _load_mask(torso_path)
    l3_mask, _ = _load_mask(l3_path)

    measurement = compute_l3_slice_fat_areas(
        vertebra_l3_mask=l3_mask,
        subcutaneous_fat_mask=sat_mask,
        torso_fat_mask=torso_mask,
        spacing_mm=sat_nii.header.get_zooms()[:2],
    )
    result["measurement"] = measurement
    result["status"] = measurement.get("status", result["status"])
    result["qc"]["needs_manual_review"] = result["status"] != "done"

    if result["status"] == "done" and measurement.get("slice_index") is not None:
        _render_l3_overlay(ct, sat_mask, torso_mask, int(measurement["slice_index"]), overlay_png)

    _save_json(result_json, result)
    return result


def render_case_summary(case_id: str, job_results: list[dict[str, Any]]) -> str:
    lines = [
        f"# Prototype body-fat summary: {case_id}",
        "",
        f"Generated at: {_timestamp()}",
        "",
    ]
    for result in job_results:
        key = result.get("metric_key", "unknown")
        lines.append(f"## {key}")
        lines.append("")
        lines.append(f"- status: {result.get('status')}")
        if key == "body_fat_abdominal_volumes":
            aggregate = (result.get("measurement") or {}).get("aggregate") or {}
            lines.append(f"- abdominal levels: {', '.join(aggregate.get('abdominal_levels') or []) or '-'}")
            lines.append(f"- coverage complete: {aggregate.get('coverage_complete')}")
            lines.append(f"- torso fat: {aggregate.get('torso_fat_cm3')}")
            lines.append(f"- subcutaneous fat: {aggregate.get('subcutaneous_fat_cm3')}")
            lines.append(f"- torso/subcutaneous ratio: {aggregate.get('torso_to_subcutaneous_ratio')}")
        elif key == "body_fat_l3_slice":
            measurement = result.get("measurement") or {}
            lines.append(f"- slice index: {measurement.get('slice_index')}")
            lines.append(f"- torso fat area: {measurement.get('torso_fat_area_cm2')}")
            lines.append(f"- subcutaneous fat area: {measurement.get('subcutaneous_fat_area_cm2')}")
            lines.append(f"- torso/subcutaneous ratio: {measurement.get('torso_to_subcutaneous_ratio')}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def process_case(context: StudyContext) -> dict[str, Any]:
    context.output_dir.mkdir(parents=True, exist_ok=True)
    job_results = [
        run_body_fat_abdominal_volumes(context),
        run_body_fat_l3_slice(context),
    ]
    summary = {
        "case_id": context.case_id,
        "source_study_dir": str(context.study_dir),
        "generated_at": _timestamp(),
        "jobs": {result["metric_key"]: result for result in job_results},
    }
    _save_json(context.output_dir / "prototype_summary.json", summary)
    _save_markdown(context.output_dir / "prototype_summary.md", render_case_summary(context.case_id, job_results))
    return summary


def run_self_test(output_root: Path) -> int:
    spacing = (1.0, 1.0, 2.0)
    sat_mask = np.zeros((32, 32, 24), dtype=bool)
    torso_mask = np.zeros_like(sat_mask)
    vertebrae: dict[str, np.ndarray] = {}

    sat_mask[2:30, 2:30, 4:20] = True
    torso_mask[8:24, 8:24, 4:20] = True

    center_map = {
        "T12": 4,
        "L1": 7,
        "L2": 10,
        "L3": 13,
        "L4": 16,
        "L5": 19,
    }
    for level, center in center_map.items():
        mask = np.zeros_like(sat_mask)
        mask[12:20, 12:20, center - 1 : center + 2] = True
        vertebrae[level] = mask

    slabs = build_abdominal_slabs(vertebrae, z_size=sat_mask.shape[2])
    distribution = calculate_body_fat_distribution(
        subcutaneous_fat_mask=sat_mask,
        torso_fat_mask=torso_mask,
        spacing_mm=spacing,
        slab_definition=slabs,
    )
    l3 = compute_l3_slice_fat_areas(
        vertebra_l3_mask=vertebrae["L3"],
        subcutaneous_fat_mask=sat_mask,
        torso_fat_mask=torso_mask,
        spacing_mm=spacing[:2],
    )
    payload = {
        "generated_at": _timestamp(),
        "slab_definition": slabs,
        "distribution": distribution,
        "l3_slice": l3,
    }
    _save_json(output_root / "self_test.json", payload)
    print(f"[self-test] wrote {output_root / 'self_test.json'}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="*", type=Path, help="Study dir, id.json, or directory containing studies.")
    parser.add_argument("--input-root", type=Path, help="Directory containing Heimdallr studies.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT, help="Prototype output root.")
    parser.add_argument("--self-test", action="store_true", help="Run a synthetic smoke test instead of reading studies.")
    args = parser.parse_args()

    output_root = args.output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    if args.self_test:
        return run_self_test(output_root)

    raw_inputs = list(args.inputs)
    if args.input_root:
        raw_inputs.append(args.input_root)
    if not raw_inputs:
        parser.error("Provide at least one study path or --input-root.")

    study_dirs = discover_study_dirs(raw_inputs)
    if not study_dirs:
        parser.error("No Heimdallr studies discovered from the provided inputs.")

    index: dict[str, Any] = {
        "generated_at": _timestamp(),
        "output_root": str(output_root),
        "cases": {},
    }
    for study_dir in study_dirs:
        context = load_study_context(study_dir, output_root)
        summary = process_case(context)
        index["cases"][context.case_id] = {
            "source_study_dir": str(study_dir),
            "prototype_summary_json": str((context.output_dir / "prototype_summary.json").relative_to(output_root)),
            "prototype_summary_md": str((context.output_dir / "prototype_summary.md").relative_to(output_root)),
            "jobs": {key: value.get("status") for key, value in summary["jobs"].items()},
        }
        print(f"[case] {context.case_id}: done")

    _save_json(output_root / "index.json", index)
    print(f"[index] wrote {output_root / 'index.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
