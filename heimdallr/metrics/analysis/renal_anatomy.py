"""Component-aware renal anatomy QC for parenchymal organ volumetry."""

from __future__ import annotations

from typing import Any

import numpy as np
from scipy.ndimage import label as ndlabel


MINIMUM_SIGNIFICANT_RENAL_COMPONENT_VOLUME_CM3 = 5.0
MINIMUM_CONTRALATERAL_SUPERIOR_OVERLAP_FRACTION = 0.25
RENAL_COMPONENT_CONNECTIVITY = np.ones((3, 3, 3), dtype=np.uint8)


def _world_position(affine: np.ndarray, index_xyz: np.ndarray) -> np.ndarray:
    homogeneous = np.append(np.asarray(index_xyz, dtype=float), 1.0)
    return np.asarray(affine, dtype=float).dot(homogeneous)[:3]


def _mask_complete(mask: np.ndarray) -> bool:
    mask_bool = np.asarray(mask, dtype=bool)
    if not mask_bool.any():
        return False
    return not any(
        (
            np.any(mask_bool[0, :, :]),
            np.any(mask_bool[-1, :, :]),
            np.any(mask_bool[:, 0, :]),
            np.any(mask_bool[:, -1, :]),
            np.any(mask_bool[:, :, 0]),
            np.any(mask_bool[:, :, -1]),
        )
    )


def _reference_superior_coordinate_mm(
    mask: np.ndarray | None,
    affine: np.ndarray,
) -> float | None:
    if mask is None:
        return None
    mask_bool = np.asarray(mask, dtype=bool)
    if not _mask_complete(mask_bool):
        return None
    coords = np.argwhere(mask_bool)
    if not coords.size:
        return None
    return float(_world_position(affine, coords.mean(axis=0))[2])


def _topographic_reference(
    reference_masks: dict[str, np.ndarray | None],
    affine: np.ndarray,
) -> dict[str, Any]:
    l3_superior_mm = _reference_superior_coordinate_mm(
        reference_masks.get("vertebra_l3"),
        affine,
    )
    l4_superior_mm = _reference_superior_coordinate_mm(
        reference_masks.get("vertebra_l4"),
        affine,
    )
    available = bool(
        l3_superior_mm is not None
        and l4_superior_mm is not None
        and l3_superior_mm > l4_superior_mm
    )
    return {
        "status": "available" if available else "unavailable",
        "coordinate_system": "NIfTI_world_RAS_mm",
        "superior_axis": "S",
        "native_region_rule": "component_centroid_at_or_superior_to_L3_centroid",
        "pelvic_region_rule": "component_centroid_at_or_inferior_to_L4_centroid",
        "vertebra_l3_centroid_superior_mm": l3_superior_mm,
        "vertebra_l4_centroid_superior_mm": l4_superior_mm,
    }


def _component_region(
    centroid_superior_mm: float,
    reference: dict[str, Any],
) -> str:
    if reference.get("status") != "available":
        return "unresolved"
    l3_superior_mm = float(reference["vertebra_l3_centroid_superior_mm"])
    l4_superior_mm = float(reference["vertebra_l4_centroid_superior_mm"])
    if centroid_superior_mm >= l3_superior_mm:
        return "native_renal_region"
    if centroid_superior_mm <= l4_superior_mm:
        return "pelvic_region"
    return "indeterminate_between_L3_and_L4"


def _superior_overlap_fraction(
    component: dict[str, Any],
    contralateral_component: dict[str, Any],
) -> float:
    component_extent = component["superior_extent_world_ras_mm"]
    contralateral_extent = contralateral_component["superior_extent_world_ras_mm"]
    inferior = max(
        float(component_extent["inferior"]),
        float(contralateral_extent["inferior"]),
    )
    superior = min(
        float(component_extent["superior"]),
        float(contralateral_extent["superior"]),
    )
    overlap_mm = max(0.0, superior - inferior)
    component_length_mm = max(
        0.0,
        float(component_extent["superior"]) - float(component_extent["inferior"]),
    )
    if component_length_mm <= 0.0:
        return 0.0
    return overlap_mm / component_length_mm


def _extract_components(
    source_mask: str,
    mask: np.ndarray,
    ct_data: np.ndarray,
    affine: np.ndarray,
    spacing_xyz_mm: tuple[float, float, float],
    reference: dict[str, Any],
    *,
    suppress_density: bool,
) -> tuple[np.ndarray, list[dict[str, Any]]]:
    labeled, component_count = ndlabel(
        np.asarray(mask, dtype=bool),
        structure=RENAL_COMPONENT_CONNECTIVITY,
    )
    voxel_volume_cm3 = float(np.prod(spacing_xyz_mm)) / 1000.0
    extracted: list[dict[str, Any]] = []
    for source_label in range(1, int(component_count) + 1):
        component_mask = labeled == source_label
        coords = np.argwhere(component_mask)
        if not coords.size:
            continue
        voxel_count = int(coords.shape[0])
        volume_cm3 = float(voxel_count * voxel_volume_cm3)
        centroid_index = coords.mean(axis=0)
        centroid_world = _world_position(affine, centroid_index)
        affine_array = np.asarray(affine, dtype=float)
        world_coords = coords.dot(affine_array[:3, :3].T) + affine_array[:3, 3]
        occupied_slices = np.flatnonzero(np.any(component_mask, axis=(0, 1)))
        if suppress_density:
            hu_mean = None
            hu_std = None
        else:
            hu_values = np.asarray(ct_data)[component_mask]
            hu_mean = round(float(np.mean(hu_values)), 2) if hu_values.size else None
            hu_std = round(float(np.std(hu_values)), 2) if hu_values.size else None
        complete = _mask_complete(component_mask)
        extracted.append(
            {
                "_source_label": source_label,
                "source_mask": source_mask,
                "voxel_count": voxel_count,
                "attenuation_sample_volume_cm3": round(volume_cm3, 3),
                "observed_volume_cm3": round(volume_cm3, 3) if complete else None,
                "volume_cm3": round(volume_cm3, 3) if complete else None,
                "hu_mean": hu_mean,
                "hu_std": hu_std,
                "complete": complete,
                "truncated_at_scan_bounds": not complete,
                "centroid_index_xyz": [round(float(value), 3) for value in centroid_index],
                "centroid_world_ras_mm": [round(float(value), 3) for value in centroid_world],
                "superior_extent_world_ras_mm": {
                    "inferior": round(float(np.min(world_coords[:, 2])), 3),
                    "superior": round(float(np.max(world_coords[:, 2])), 3),
                },
                "axial_slice_extent": {
                    "start": int(occupied_slices[0]),
                    "end": int(occupied_slices[-1]),
                },
                "attenuation_sample_slice_count": int(occupied_slices.size),
                "attenuation_sample_axial_extent_mm": round(
                    int(occupied_slices[-1] - occupied_slices[0] + 1)
                    * float(spacing_xyz_mm[2]),
                    3,
                ),
                "topographic_region": _component_region(float(centroid_world[2]), reference),
                "significant": volume_cm3 >= MINIMUM_SIGNIFICANT_RENAL_COMPONENT_VOLUME_CM3,
            }
        )
    extracted.sort(
        key=lambda component: (
            -float(component["centroid_world_ras_mm"][2]),
            -int(component["voxel_count"]),
        )
    )
    for component_id, component in enumerate(extracted, start=1):
        component["component_id"] = component_id
    return labeled, extracted


def analyze_renal_anatomy(
    kidney_masks: dict[str, np.ndarray | None],
    ct_data: np.ndarray,
    affine: np.ndarray,
    spacing_xyz_mm: tuple[float, float, float],
    reference_masks: dict[str, np.ndarray | None],
    *,
    suppress_density: bool = False,
) -> tuple[dict[str, Any], dict[str, dict[str, Any] | None], list[dict[str, Any]]]:
    """Classify renal components without treating topography as transplant proof."""
    reference = _topographic_reference(reference_masks, affine)
    selected_measurement_components: dict[str, dict[str, Any] | None] = {}
    overlay_components: list[dict[str, Any]] = []
    kidney_audit: dict[str, Any] = {}
    suspected_allografts: list[dict[str, Any]] = []

    extracted_by_source: dict[str, tuple[np.ndarray | None, list[dict[str, Any]]]] = {}
    for source_mask in ("kidney_right", "kidney_left"):
        mask = kidney_masks.get(source_mask)
        if mask is None:
            extracted_by_source[source_mask] = (None, [])
            continue
        extracted_by_source[source_mask] = _extract_components(
            source_mask,
            mask,
            ct_data,
            affine,
            spacing_xyz_mm,
            reference,
            suppress_density=suppress_density,
        )

    for source_mask in ("kidney_right", "kidney_left"):
        side = "right" if source_mask.endswith("right") else "left"
        mask = kidney_masks.get(source_mask)
        if mask is None:
            selected_measurement_components[source_mask] = None
            kidney_audit[source_mask] = {
                "classification_status": "missing",
                "raw_component_count": 0,
                "significant_component_count": 0,
                "native_component_id": None,
                "measurement_component_id": None,
                "components": [],
            }
            continue

        labeled, components = extracted_by_source[source_mask]
        assert labeled is not None
        significant = [component for component in components if component["significant"]]
        native_candidates = [
            component
            for component in significant
            if component["topographic_region"] == "native_renal_region"
        ]
        pelvic_candidates = [
            component
            for component in significant
            if component["topographic_region"] == "pelvic_region"
        ]

        native_component: dict[str, Any] | None = None
        contralateral_overlap_fraction: float | None = None
        native_selection_reason: str | None = None
        if reference.get("status") == "available":
            if len(native_candidates) == 1:
                native_component = native_candidates[0]
                native_selection_reason = "single_native_region_component"
            elif (
                len(significant) == 1
                and significant[0]["topographic_region"] == "indeterminate_between_L3_and_L4"
            ):
                contralateral_source = (
                    "kidney_left" if source_mask == "kidney_right" else "kidney_right"
                )
                contralateral_significant = [
                    component
                    for component in extracted_by_source[contralateral_source][1]
                    if component["significant"]
                ]
                if (
                    len(contralateral_significant) == 1
                    and contralateral_significant[0]["topographic_region"]
                    != "pelvic_region"
                ):
                    contralateral_overlap_fraction = _superior_overlap_fraction(
                        significant[0],
                        contralateral_significant[0],
                    )
                    if (
                        contralateral_overlap_fraction
                        >= MINIMUM_CONTRALATERAL_SUPERIOR_OVERLAP_FRACTION
                    ):
                        native_component = significant[0]
                        native_selection_reason = "contralateral_superior_extent_overlap"
        elif len(significant) == 1:
            native_component = significant[0]
            native_selection_reason = "single_significant_component_legacy_fallback"

        measurement_component = native_component
        if (
            measurement_component is None
            and len(significant) == 1
            and significant[0]["topographic_region"]
            in {"indeterminate_between_L3_and_L4", "pelvic_region"}
        ):
            measurement_component = significant[0]

        for component in components:
            component_id = int(component["component_id"])
            is_native = native_component is component
            if not component["significant"]:
                anatomic_role = "segmentation_fragment"
                reason = "below_minimum_significant_component_volume"
            elif is_native:
                anatomic_role = f"native_kidney_{side}"
                reason = str(native_selection_reason)
            elif (
                component["topographic_region"] == "pelvic_region"
                and native_component is not None
            ):
                anatomic_role = f"suspected_renal_allograft_{side}"
                reason = "pelvic_component_separate_from_native_component"
            elif component["topographic_region"] == "pelvic_region":
                anatomic_role = f"indeterminate_pelvic_renal_component_{side}"
                reason = "pelvic_position_without_identified_native_component"
            elif component["topographic_region"] == "native_renal_region":
                anatomic_role = "unclassified_native_region_component"
                reason = "multiple_native_region_components"
            else:
                anatomic_role = "unclassified_renal_component"
                reason = "topographic_classification_indeterminate"
            component["anatomic_role"] = anatomic_role
            component["classification_reason"] = reason
            component["included_in_native_measurement"] = is_native
            component["included_in_renal_measurement"] = measurement_component is component
            if significant and component is significant[0]:
                component["contralateral_superior_overlap_fraction"] = (
                    round(float(contralateral_overlap_fraction), 3)
                    if contralateral_overlap_fraction is not None
                    else None
                )

            source_label = int(component.pop("_source_label"))
            component_mask = labeled == source_label
            if anatomic_role.startswith("suspected_renal_allograft_"):
                overlay_components.append(
                    {
                        "source_mask": source_mask,
                        "component_id": component_id,
                        "anatomic_role": anatomic_role,
                        "mask": component_mask,
                    }
                )
                suspected_allografts.append(
                    {
                        key: value
                        for key, value in component.items()
                        if key
                        in {
                            "source_mask",
                            "component_id",
                            "voxel_count",
                            "attenuation_sample_volume_cm3",
                            "observed_volume_cm3",
                            "volume_cm3",
                            "hu_mean",
                            "hu_std",
                            "complete",
                            "truncated_at_scan_bounds",
                            "centroid_index_xyz",
                            "centroid_world_ras_mm",
                            "superior_extent_world_ras_mm",
                            "axial_slice_extent",
                            "attenuation_sample_slice_count",
                            "attenuation_sample_axial_extent_mm",
                            "topographic_region",
                            "anatomic_role",
                            "classification_reason",
                        }
                    }
                )

        if measurement_component is not None:
            selected_measurement_components[source_mask] = measurement_component
        else:
            selected_measurement_components[source_mask] = None

        if not significant:
            classification_status = "no_significant_component"
        elif reference.get("status") != "available" and len(significant) == 1:
            classification_status = "single_component_legacy_fallback"
        elif native_component is not None and pelvic_candidates:
            classification_status = "native_and_suspected_allograft"
        elif native_component is not None and len(significant) == 1:
            classification_status = "native_only"
        elif native_component is not None:
            classification_status = "native_with_unclassified_components"
        elif (
            measurement_component is not None
            and measurement_component["topographic_region"] == "pelvic_region"
        ):
            classification_status = "single_pelvic_component_anatomy_indeterminate"
        elif measurement_component is not None:
            classification_status = "single_component_anatomy_indeterminate"
        elif len(significant) > 1:
            classification_status = "ambiguous_multiple_components"
        else:
            classification_status = "anatomy_unresolved"

        kidney_audit[source_mask] = {
            "classification_status": classification_status,
            "raw_component_count": len(components),
            "significant_component_count": len(significant),
            "native_component_id": (
                int(native_component["component_id"])
                if native_component is not None
                else None
            ),
            "measurement_component_id": (
                int(measurement_component["component_id"])
                if measurement_component is not None
                else None
            ),
            "components": components,
        }

    audit = {
        "method": "connected_components_with_L3_L4_topographic_qc",
        "connectivity": 26,
        "minimum_significant_component_volume_cm3": (
            MINIMUM_SIGNIFICANT_RENAL_COMPONENT_VOLUME_CM3
        ),
        "minimum_contralateral_superior_overlap_fraction": (
            MINIMUM_CONTRALATERAL_SUPERIOR_OVERLAP_FRACTION
        ),
        "classification_scope": "topographic_suspicion_not_transplant_diagnosis",
        "topographic_reference": reference,
        "multiple_significant_components": any(
            int(item["significant_component_count"]) > 1
            for item in kidney_audit.values()
        ),
        "suspected_allograft": bool(suspected_allografts),
        "kidneys": kidney_audit,
        "suspected_renal_allografts": suspected_allografts,
    }
    return audit, selected_measurement_components, overlay_components
