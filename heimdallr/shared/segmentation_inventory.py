"""Segmentation inventory helpers used by automatic CT planning."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import nibabel as nib
import numpy as np

from heimdallr.metrics.head import collect_mask_statuses, compute_mask_status
from heimdallr.shared.segmentation_coverage import mask_complete as mask_complete_along_z


INVENTORY_FILENAME = "segmentation_inventory.json"
PARENCHYMAL_ORGAN_MASKS = (
    "liver",
    "spleen",
    "pancreas",
    "kidney_left",
    "kidney_right",
)
HEAD_MASKS = ("skull", "brain")
VERTEBRA_MASKS = ("vertebrae_L1", "vertebrae_L3")


def _reference_geometry(reference_image_path: Path) -> tuple[tuple[int, int, int], tuple[float, float, float]]:
    reference_image = nib.load(str(reference_image_path))
    reference_shape = tuple(int(value) for value in reference_image.shape[:3])
    spacing_xyz = tuple(float(value) for value in reference_image.header.get_zooms()[:3])
    return reference_shape, spacing_xyz


def mask_inventory_status(mask_path: Path, reference_image_path: Path) -> dict[str, Any]:
    """Return present/complete status for one TotalSegmentator mask."""
    try:
        reference_shape, spacing_xyz = _reference_geometry(reference_image_path)
    except Exception as exc:
        return {
            "present": False,
            "complete": False,
            "reason": "reference_read_error",
            "error": str(exc),
        }

    if not mask_path.exists():
        return {
            "present": False,
            "complete": False,
            "reason": "missing_mask",
            "mask": str(mask_path),
        }

    try:
        mask_image = nib.load(str(mask_path))
        mask = np.asarray(mask_image.get_fdata(), dtype=np.float32) > 0
    except Exception as exc:
        return {
            "present": False,
            "complete": False,
            "reason": "mask_read_error",
            "mask": str(mask_path),
            "error": str(exc),
        }

    if tuple(mask.shape[:3]) != reference_shape:
        return {
            "present": False,
            "complete": False,
            "reason": "geometry_mismatch",
            "mask": str(mask_path),
            "shape": [int(value) for value in mask.shape[:3]],
            "reference_shape": [int(value) for value in reference_shape],
        }

    status = compute_mask_status(mask, spacing_xyz)
    present = bool(status.get("present"))
    complete = bool(present and mask_complete_along_z(mask))
    return {
        "present": present,
        "complete": complete,
        "reason": "complete" if complete else ("present_but_incomplete" if present else "empty_mask"),
        "mask": str(mask_path),
        "status": status,
    }


def build_segmentation_inventory(total_dir: Path, reference_image_path: Path) -> dict[str, Any]:
    """Build a deterministic inventory from the `total` segmentation output."""
    inventory: dict[str, Any] = {
        "schema_version": 1,
        "source_task": "total",
        "total_dir": str(total_dir),
        "masks": {},
    }

    masks = inventory["masks"]
    for mask_name in HEAD_MASKS + VERTEBRA_MASKS + PARENCHYMAL_ORGAN_MASKS:
        masks[mask_name] = mask_inventory_status(total_dir / f"{mask_name}.nii.gz", reference_image_path)

    brain = masks.get("brain", {})
    skull = masks.get("skull", {})
    l1 = masks.get("vertebrae_L1", {})
    l3 = masks.get("vertebrae_L3", {})
    organ_statuses = {name: masks.get(name, {}) for name in PARENCHYMAL_ORGAN_MASKS}
    present_organs = [name for name, status in organ_statuses.items() if status.get("present")]
    complete_organs = [name for name, status in organ_statuses.items() if status.get("complete")]

    inventory["brain"] = {
        "present": bool(brain.get("present")),
        "complete": bool(brain.get("complete")),
    }
    inventory["skull"] = {
        "present": bool(skull.get("present")),
        "complete": bool(skull.get("complete")),
    }
    inventory["head"] = {
        "present": bool(brain.get("present") or skull.get("present")),
        "complete": bool(brain.get("complete")),
        "required_mask": "brain",
        "skull_required": False,
    }
    inventory["vertebrae_L1"] = {
        "present": bool(l1.get("present")),
        "complete": bool(l1.get("complete")),
    }
    inventory["vertebrae_L3"] = {
        "present": bool(l3.get("present")),
        "complete": bool(l3.get("complete")),
    }
    inventory["parenchymal_organs"] = {
        "mask_names": list(PARENCHYMAL_ORGAN_MASKS),
        "present": present_organs,
        "complete": complete_organs,
        "any_present": bool(present_organs),
        "any_complete": bool(complete_organs),
    }

    try:
        reference_shape, spacing_xyz = _reference_geometry(reference_image_path)
        inventory["reference"] = {
            "path": str(reference_image_path),
            "shape": [int(value) for value in reference_shape],
            "spacing_xyz_mm": [float(value) for value in spacing_xyz],
        }
        inventory["head_components"] = collect_mask_statuses(
            total_dir,
            list(HEAD_MASKS),
            spacing_xyz,
            reference_shape=reference_shape,
        )
    except Exception as exc:
        inventory["reference"] = {"path": str(reference_image_path), "error": str(exc)}

    return inventory


def write_segmentation_inventory(artifacts_dir: Path, inventory: dict[str, Any]) -> Path:
    path = artifacts_dir / INVENTORY_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    return path


def load_segmentation_inventory(case_root: Path) -> dict[str, Any] | None:
    path = case_root / "artifacts" / INVENTORY_FILENAME
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def inventory_value(inventory: dict[str, Any] | None, requirement: str) -> Any:
    current: Any = inventory if isinstance(inventory, dict) else {}
    for part in str(requirement or "").split("."):
        if not part:
            return None
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def inventory_satisfies(inventory: dict[str, Any] | None, requirement: str) -> bool:
    return bool(inventory_value(inventory, requirement))


def normalize_inventory_requirements(job: dict[str, Any]) -> list[str]:
    raw = job.get("requires_inventory", [])
    if raw in (None, ""):
        return []
    if not isinstance(raw, list):
        raise RuntimeError(
            f"Metrics job '{job.get('name', '<unknown>')}' requires_inventory must be a list"
        )
    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw:
        requirement = str(item or "").strip()
        if not requirement or requirement in seen:
            continue
        normalized.append(requirement)
        seen.add(requirement)
    return normalized
