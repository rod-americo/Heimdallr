"""Deterministic study evidence helpers for opt-in multi-acquisition QC."""

from __future__ import annotations

from datetime import datetime
from hashlib import sha256
from importlib import metadata as importlib_metadata
import json
import math
from pathlib import Path
from typing import Any
import uuid

import nibabel as nib
import numpy as np


SCHEMA_VERSION = 1
ANATOMY_STATES = {
    "anatomy_not_detected",
    "anatomy_present",
    "anatomy_complete",
    "anatomy_truncated",
    "unknown",
}


def canonical_signature(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return sha256(encoded).hexdigest()


def new_analysis_id() -> str:
    return str(uuid.uuid4())


def heimdallr_version() -> str:
    try:
        return importlib_metadata.version("heimdallr")
    except importlib_metadata.PackageNotFoundError:
        return "source"


def totalsegmentator_version() -> str:
    for package_name in ("TotalSegmentator", "totalsegmentator"):
        try:
            return importlib_metadata.version(package_name)
        except importlib_metadata.PackageNotFoundError:
            continue
    return "unknown"


def study_content_fingerprint(series_map: dict[Any, dict[str, Any]]) -> str:
    """Hash the immutable DICOM instance inventory and bytes, independent of paths."""
    entries: list[tuple[str, str, str]] = []
    for raw_uid, series in series_map.items():
        series_uid = str(raw_uid)
        sop_uids = [str(item or "") for item in series.get("SOPInstanceUIDs", [])]
        files = list(series.get("files", []))
        for index, path_value in enumerate(files):
            path = Path(path_value)
            digest = sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            sop_uid = sop_uids[index] if index < len(sop_uids) else ""
            entries.append((series_uid, sop_uid, digest.hexdigest()))
    return canonical_signature(sorted(entries))


def _float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _sequence(value: Any, length: int) -> list[float]:
    if isinstance(value, str):
        raw = value.replace("\\", ",").split(",")
    elif isinstance(value, (list, tuple)):
        raw = value
    else:
        return []
    parsed = [_float(item) for item in raw]
    if len(parsed) != length or any(item is None for item in parsed):
        return []
    return [float(item) for item in parsed if item is not None]


def _normal(orientation: Any) -> np.ndarray | None:
    values = _sequence(orientation, 6)
    if not values:
        return None
    normal = np.cross(np.asarray(values[:3]), np.asarray(values[3:]))
    norm = float(np.linalg.norm(normal))
    return normal / norm if norm > 0 else None


def _parse_dicom_datetime(value: Any) -> datetime | None:
    text = "".join(ch for ch in str(value or "") if ch.isdigit() or ch == ".")
    if not text:
        return None
    main = text.split(".", 1)[0]
    for fmt, length in (("%Y%m%d%H%M%S", 14), ("%Y%m%d%H%M", 12), ("%H%M%S", 6)):
        if len(main) >= length:
            try:
                return datetime.strptime(main[:length], fmt)
            except ValueError:
                continue
    return None


def _overlap_ratio(left: dict[str, Any], right: dict[str, Any]) -> float | None:
    left_min = _float(left.get("geometry", {}).get("min_position_mm"))
    left_max = _float(left.get("geometry", {}).get("max_position_mm"))
    right_min = _float(right.get("geometry", {}).get("min_position_mm"))
    right_max = _float(right.get("geometry", {}).get("max_position_mm"))
    if None in {left_min, left_max, right_min, right_max}:
        return None
    left_low, left_high = sorted((float(left_min), float(left_max)))
    right_low, right_high = sorted((float(right_min), float(right_max)))
    overlap = max(0.0, min(left_high, right_high) - max(left_low, right_low))
    denominator = min(left_high - left_low, right_high - right_low)
    return overlap / denominator if denominator > 0 else None


def _series_equivalent(left: dict[str, Any], right: dict[str, Any], policy: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if left["modality"] != right["modality"]:
        return False, ["modality_conflict"]
    left_frame = str(left.get("frame_of_reference_uid") or "")
    right_frame = str(right.get("frame_of_reference_uid") or "")
    if left_frame and right_frame and left_frame != right_frame:
        return False, ["frame_of_reference_conflict"]
    if left_frame and right_frame:
        reasons.append("same_frame_of_reference")

    left_phase = str(left.get("contrast_phase") or "unknown")
    right_phase = str(right.get("contrast_phase") or "unknown")
    if left_phase != "unknown" and right_phase != "unknown" and left_phase != right_phase:
        return False, ["contrast_phase_conflict"]
    if left_phase == right_phase:
        reasons.append("same_contrast_phase")

    left_acq = str(left.get("acquisition_number") or "")
    right_acq = str(right.get("acquisition_number") or "")
    if left_acq and right_acq:
        if left_acq != right_acq:
            return False, ["acquisition_number_conflict"]
        reasons.append("same_acquisition_number")

    left_time = _parse_dicom_datetime(left.get("acquisition_datetime"))
    right_time = _parse_dicom_datetime(right.get("acquisition_datetime"))
    if left_time and right_time:
        delta = abs((left_time - right_time).total_seconds())
        tolerance = float(policy.get("acquisition_time_tolerance_seconds", 30))
        if delta > tolerance:
            return False, ["acquisition_time_conflict"]
        reasons.append("acquisition_time_proximity")

    overlap = _overlap_ratio(left, right)
    if overlap is not None:
        if overlap < float(policy.get("minimum_spatial_overlap_ratio", 0.8)):
            return False, ["insufficient_spatial_overlap"]
        reasons.append("spatial_overlap")

    left_normal = _normal(left.get("image_orientation_patient"))
    right_normal = _normal(right.get("image_orientation_patient"))
    if left_normal is not None and right_normal is not None and not (left["derived"] or right["derived"]):
        cosine = min(1.0, max(-1.0, abs(float(np.dot(left_normal, right_normal)))))
        degrees = math.degrees(math.acos(cosine))
        if degrees > float(policy.get("orientation_tolerance_degrees", 5)):
            return False, ["orientation_conflict"]
        reasons.append("parallel_orientation")

    reliable = "same_acquisition_number" in reasons and "same_frame_of_reference" in reasons
    fallback = "acquisition_time_proximity" in reasons and "spatial_overlap" in reasons
    if not reliable and not fallback:
        return False, ["insufficient_equivalence_evidence"]
    return True, reasons


def _classification(raw: dict[str, Any], converted: dict[str, Any] | None) -> dict[str, Any]:
    modality = str(raw.get("Modality") or "OT").upper()
    description = str(raw.get("SeriesDescriptionOriginal") or raw.get("SeriesDescription") or "")
    raw_image_type = raw.get("ImageType", [])
    if isinstance(raw_image_type, str):
        raw_image_type = raw_image_type.replace("\\", ",").split(",")
    image_type = [str(item).upper() for item in raw_image_type]
    text = " ".join([description.upper(), *image_type])
    derived = "DERIVED" in image_type or any(token in text for token in (" MIP", "MPR", "REFORMAT"))
    localizer = any(token in text for token in ("LOCALIZER", "SCOUT", "TOPOGRAM", "SURVIEW"))
    geometry = {
        "slice_thickness_mm": raw.get("SliceThicknessMm"),
        "spacing_between_slices_mm": raw.get("SpacingBetweenSlicesMm"),
        "z_spacing_mm": raw.get("ZSpacingMm"),
        "coverage_mm": raw.get("CoverageMm") or raw.get("EstimatedCoverageMm"),
        "min_position_mm": raw.get("GeometryMinPositionMm"),
        "max_position_mm": raw.get("GeometryMaxPositionMm"),
        "patient_bounds_mm": raw.get("PatientBoundsMm"),
        "confidence": raw.get("GeometryConfidence", "none"),
        "warnings": list(raw.get("GeometryWarnings", [])),
    }
    incomplete_geometry = geometry["confidence"] != "position"
    slice_count = int(raw.get("SliceCount") or len(raw.get("files", [])))
    volumetric = slice_count >= 2 and not localizer and geometry["confidence"] != "none"
    useful = volumetric and not localizer
    segmentable = modality == "CT" and useful and not derived and converted is not None
    reasons: list[str] = []
    if localizer:
        reasons.append("localizer")
    if derived:
        reasons.append("derived_series")
    if incomplete_geometry:
        reasons.append("incomplete_geometry")
    if modality == "MR":
        reasons.append("no_configured_mr_segmenter")
    elif modality != "CT":
        reasons.append("unsupported_modality")
    if converted is None:
        reasons.append("no_converted_nifti")
    if segmentable:
        reasons.append("ct_total_segmentator_compatible")
    return {
        "series_uid": str(raw.get("SeriesInstanceUID") or ""),
        "series_number": str(raw.get("SeriesNumber") or ""),
        "modality": modality,
        "description": description,
        "slice_count": slice_count,
        "frame_of_reference_uid": raw.get("FrameOfReferenceUID"),
        "acquisition_number": raw.get("AcquisitionNumber"),
        "temporal_position_identifier": raw.get("TemporalPositionIdentifier"),
        "acquisition_datetime": raw.get("AcquisitionDateTime"),
        "image_orientation_patient": raw.get("ImageOrientationPatient"),
        "image_type": image_type,
        "kernel": raw.get("ConvolutionKernel"),
        "reconstruction_algorithm": raw.get("ReconstructionAlgorithm"),
        "protocol_name": raw.get("ProtocolName"),
        "sequence_name": raw.get("SequenceName"),
        "contrast_phase": (converted or {}).get("DetectedPhase", "unknown"),
        "contrast_phase_evidence": (converted or {}).get("PhaseData", {}),
        "geometry": geometry,
        "derived_nifti_path": (converted or {}).get("DerivedNiftiPath"),
        "volumetric": volumetric,
        "useful_for_qc": useful,
        "segmentable": segmentable,
        "selected_for_metrics": False,
        "derived": derived,
        "localizer": localizer,
        "duplicate_reconstruction": False,
        "incomplete_geometry": incomplete_geometry,
        "classification_reasons": reasons,
        "segmentation_status": "not_segmentable" if not segmentable else "segmentation_pending",
    }


def _reconstruction_preference(item: dict[str, Any]) -> int:
    text = " ".join(
        [
            str(item.get("description") or ""),
            str(item.get("kernel") or ""),
            str(item.get("reconstruction_algorithm") or ""),
        ]
    ).lower()
    score = sum(token in text for token in ("soft", "standard", "body", "mediast"))
    score -= sum(token in text for token in ("lung", "pulmao", "bone", "mip", "sharp"))
    return int(score)


def build_inventory(
    raw_series: list[dict[str, Any]],
    converted_series: list[dict[str, Any]],
    *,
    policy: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    converted_by_uid = {str(item.get("SeriesInstanceUID") or ""): item for item in converted_series}
    series = [_classification(raw, converted_by_uid.get(str(raw.get("SeriesInstanceUID") or ""))) for raw in raw_series]
    groups: list[list[dict[str, Any]]] = []
    group_reasons: list[dict[tuple[str, str], list[str]]] = []
    for item in sorted(series, key=lambda value: value["series_uid"]):
        placed = False
        for index, group in enumerate(groups):
            equivalent, reasons = _series_equivalent(group[0], item, policy)
            if equivalent:
                group_reasons[index][(group[0]["series_uid"], item["series_uid"])] = reasons
                group.append(item)
                placed = True
                break
        if not placed:
            groups.append([item])
            group_reasons.append({})

    acquisitions: list[dict[str, Any]] = []
    for group, reasons in zip(groups, group_reasons):
        candidates = [item for item in group if item["segmentable"]]
        max_coverage = max(
            (float(item["geometry"].get("coverage_mm") or 0) for item in candidates),
            default=0.0,
        )
        coverage_floor = max(max_coverage * 0.92, max_coverage - 50.0)
        candidates.sort(
            key=lambda item: (
                bool(item["incomplete_geometry"]),
                bool(item["derived"]),
                float(item["geometry"].get("coverage_mm") or 0) < coverage_floor,
                float(item["geometry"].get("z_spacing_mm") or item["geometry"].get("slice_thickness_mm") or 9999),
                -_reconstruction_preference(item),
                -float(item["geometry"].get("coverage_mm") or 0),
                -int(item["slice_count"]),
                item["series_uid"],
            )
        )
        representative = candidates[0] if candidates else None
        acquisition_id = "acq_" + canonical_signature([item["series_uid"] for item in group])[:16]
        for item in group:
            item["acquisition_id"] = acquisition_id
            item["duplicate_reconstruction"] = len(group) > 1 and item is not representative
            if representative is not None and item is representative:
                item["segmentation_status"] = "segmentation_pending"
            elif item["segmentable"]:
                item["segmentation_status"] = "not_segmented"
                item["classification_reasons"].append("equivalent_non_representative")
        acquisitions.append(
            {
                "acquisition_id": acquisition_id,
                "modality": group[0]["modality"],
                "contrast_phase": group[0]["contrast_phase"],
                "series_uids": [item["series_uid"] for item in group],
                "representative_series_uid": representative["series_uid"] if representative else None,
                "frame_of_reference_uid": (representative or group[0]).get("frame_of_reference_uid"),
                "image_orientation_patient": (representative or group[0]).get("image_orientation_patient"),
                "geometry": (representative or group[0]).get("geometry", {}),
                "equivalence_evidence": [
                    {"left": left, "right": right, "reasons": value}
                    for (left, right), value in sorted(reasons.items())
                ],
                "segmentation_status": "segmentation_pending" if representative else "not_segmentable",
            }
        )
    return series, acquisitions


def inventory_total_masks(total_dir: Path, reference_path: Path) -> list[dict[str, Any]]:
    reference = nib.load(str(reference_path))
    reference_shape = tuple(int(value) for value in reference.shape[:3])
    spacing = tuple(float(value) for value in reference.header.get_zooms()[:3])
    voxel_volume_ml = float(np.prod(spacing)) / 1000.0
    evidence: list[dict[str, Any]] = []
    for mask_path in sorted(total_dir.glob("*.nii.gz")):
        anatomy_key = mask_path.name[:-7]
        payload: dict[str, Any] = {
            "anatomy_key": anatomy_key,
            "mask_path": str(mask_path),
            "confidence": None,
        }
        try:
            mask_image = nib.load(str(mask_path))
            mask = np.asarray(mask_image.get_fdata(dtype=np.float32)) > 0
        except Exception as exc:
            payload.update({"state": "unknown", "reason": "mask_read_error", "error": str(exc)})
            evidence.append(payload)
            continue
        if tuple(mask.shape[:3]) != reference_shape:
            payload.update(
                {
                    "state": "unknown",
                    "reason": "geometry_mismatch",
                    "shape": list(mask.shape[:3]),
                    "reference_shape": list(reference_shape),
                }
            )
            evidence.append(payload)
            continue
        if not np.allclose(mask_image.affine, reference.affine, rtol=1e-5, atol=1e-4):
            payload.update({"state": "unknown", "reason": "affine_mismatch"})
            evidence.append(payload)
            continue
        coordinates = np.argwhere(mask)
        voxel_count = int(coordinates.shape[0])
        if voxel_count == 0:
            payload.update(
                {
                    "state": "anatomy_not_detected",
                    "voxel_count": 0,
                    "volume_ml": 0.0,
                    "bounds_voxel": None,
                    "bounds_world_mm": None,
                    "boundary_contacts": [],
                }
            )
            evidence.append(payload)
            continue
        minimum = coordinates.min(axis=0)
        maximum = coordinates.max(axis=0)
        contacts = []
        labels = ((0, "x_min", "x_max"), (1, "y_min", "y_max"), (2, "z_min", "z_max"))
        for axis, low_label, high_label in labels:
            if int(minimum[axis]) == 0:
                contacts.append(low_label)
            if int(maximum[axis]) == reference_shape[axis] - 1:
                contacts.append(high_label)
        voxel_corners = np.asarray(
            [
                [x, y, z, 1]
                for x in (minimum[0], maximum[0])
                for y in (minimum[1], maximum[1])
                for z in (minimum[2], maximum[2])
            ],
            dtype=float,
        )
        world = (reference.affine @ voxel_corners.T).T[:, :3]
        payload.update(
            {
                "state": "anatomy_truncated" if contacts else "anatomy_complete",
                "voxel_count": voxel_count,
                "volume_ml": round(voxel_count * voxel_volume_ml, 6),
                "bounds_voxel": {"min": minimum.tolist(), "max": maximum.tolist()},
                "bounds_world_mm": {
                    "min": world.min(axis=0).tolist(),
                    "max": world.max(axis=0).tolist(),
                },
                "boundary_contacts": contacts,
            }
        )
        evidence.append(payload)
    return evidence


def consolidate_coverage(
    acquisitions: list[dict[str, Any]],
    anatomy_evidence: list[dict[str, Any]],
) -> dict[str, Any]:
    by_anatomy: dict[str, list[dict[str, Any]]] = {}
    for item in anatomy_evidence:
        by_anatomy.setdefault(str(item["anatomy_key"]), []).append(item)
    applicable = [item for item in acquisitions if item.get("representative_series_uid")]
    acquisition_statuses = {item["acquisition_id"]: item.get("segmentation_status") for item in applicable}
    anatomy: dict[str, Any] = {}
    conflicts: list[dict[str, Any]] = []
    for key, evidence in sorted(by_anatomy.items()):
        states = {str(item.get("state") or "unknown") for item in evidence}
        evidenced_acquisitions = {str(item.get("acquisition_id") or "") for item in evidence}
        outstanding = any(status != "done" for status in acquisition_statuses.values())
        missing = any(acquisition_id not in evidenced_acquisitions for acquisition_id in acquisition_statuses)
        if "anatomy_complete" in states:
            state = "anatomy_complete"
        elif "anatomy_present" in states:
            state = "anatomy_present"
        elif states == {"anatomy_truncated"} and not outstanding and not missing:
            state = "anatomy_truncated"
        elif states == {"anatomy_not_detected"} and not outstanding and not missing:
            state = "anatomy_not_detected"
        else:
            state = "unknown"
        anatomy[key] = {
            "state": state,
            "contributors": [
                {
                    "acquisition_id": item.get("acquisition_id"),
                    "series_uid": item.get("series_uid"),
                    "state": item.get("state"),
                }
                for item in evidence
            ],
        }
        positive = states & {"anatomy_complete", "anatomy_present", "anatomy_truncated"}
        if positive and "anatomy_not_detected" in states:
            conflicts.append(
                {
                    "anatomy_key": key,
                    "states": sorted(states),
                    "acquisition_ids": sorted(evidenced_acquisitions),
                }
            )
    phases = sorted(
        {
            str(item.get("contrast_phase"))
            for item in acquisitions
            if item.get("contrast_phase") not in (None, "", "unknown")
        }
    )
    gaps: list[dict[str, Any]] = []
    comparable = []
    for item in applicable:
        geometry = item.get("geometry", {})
        low = _float(geometry.get("min_position_mm"))
        high = _float(geometry.get("max_position_mm"))
        normal = _normal(item.get("image_orientation_patient"))
        if low is None or high is None or normal is None:
            continue
        comparable.append(
            {
                "acquisition_id": item["acquisition_id"],
                "frame": item.get("frame_of_reference_uid"),
                "normal": normal,
                "low": min(low, high),
                "high": max(low, high),
                "spacing": _float(geometry.get("z_spacing_mm")) or 0.0,
            }
        )
    for index, left in enumerate(comparable):
        for right in comparable[index + 1 :]:
            if left["frame"] and right["frame"] and left["frame"] != right["frame"]:
                continue
            cosine = abs(float(np.dot(left["normal"], right["normal"])))
            if cosine < math.cos(math.radians(5)):
                continue
            gap = max(0.0, max(left["low"], right["low"]) - min(left["high"], right["high"]))
            tolerance = max(float(left["spacing"]), float(right["spacing"]), 1.0) * 1.5
            if gap > tolerance:
                gaps.append(
                    {
                        "acquisition_ids": [left["acquisition_id"], right["acquisition_id"]],
                        "distance_mm": round(gap, 4),
                    }
                )
    extents = []
    for frame in sorted({str(item["frame"] or "unknown") for item in comparable}):
        members = [item for item in comparable if str(item["frame"] or "unknown") == frame]
        extents.append(
            {
                "frame_of_reference_uid": None if frame == "unknown" else frame,
                "inferior_position_mm": min(item["low"] for item in members),
                "superior_position_mm": max(item["high"] for item in members),
                "acquisition_ids": [item["acquisition_id"] for item in members],
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "anatomies": anatomy,
        "available_phases": phases,
        "spatial_extents": extents,
        "gaps": gaps,
        "conflicts": conflicts,
    }
