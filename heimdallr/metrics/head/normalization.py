"""Deterministic head CT mask status and geometry normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import nibabel as nib
import numpy as np
from nibabel.orientations import aff2axcodes
from nibabel.processing import resample_from_to, resample_to_output


HEAD_COMPONENT_MASKS = ("skull", "brain")

BRAIN_STRUCTURE_MASKS = (
    "brainstem",
    "subarachnoid_space",
    "venous_sinuses",
    "septum_pellucidum",
    "cerebellum",
    "caudate_nucleus",
    "lentiform_nucleus",
    "insular_cortex",
    "internal_capsule",
    "ventricle",
    "central_sulcus",
    "frontal_lobe",
    "parietal_lobe",
    "occipital_lobe",
    "temporal_lobe",
    "thalamus",
)

HEAD_ANATOMIC_LANDMARKS = (
    "left_orbitale",
    "right_orbitale",
    "left_porion",
    "right_porion",
)
HEAD_LANDMARK_ALIASES = {
    "left_external_auditory_meatus": "left_porion",
    "right_external_auditory_meatus": "right_porion",
    "left_eam": "left_porion",
    "right_eam": "right_porion",
}

@dataclass(frozen=True)
class HeadNormalizationSpec:
    """Target geometry for head CT downstream deterministic jobs."""

    target_plane: str = "axial"
    target_in_plane_spacing_mm: tuple[float, float] = (1.0, 1.0)
    target_slice_thickness_mm: float = 5.0
    write_normalized_nifti: bool = True

    @property
    def target_spacing_mm(self) -> tuple[float, float, float]:
        return (
            float(self.target_in_plane_spacing_mm[0]),
            float(self.target_in_plane_spacing_mm[1]),
            float(self.target_slice_thickness_mm),
        )


def parse_normalization_spec(job_config: dict[str, Any] | None) -> HeadNormalizationSpec:
    config = job_config if isinstance(job_config, dict) else {}
    target_plane = str(config.get("target_plane") or "axial").strip().lower()
    if target_plane != "axial":
        raise RuntimeError(f"Unsupported head normalization target_plane: {target_plane}")

    raw_spacing = config.get("target_in_plane_spacing_mm", [1.0, 1.0])
    if not isinstance(raw_spacing, (list, tuple)) or len(raw_spacing) != 2:
        raise RuntimeError("target_in_plane_spacing_mm must be a two-item array")
    in_plane_spacing = (_positive_float(raw_spacing[0], "target_in_plane_spacing_mm[0]"),
                        _positive_float(raw_spacing[1], "target_in_plane_spacing_mm[1]"))
    slice_thickness = _positive_float(
        config.get("target_slice_thickness_mm", 5.0),
        "target_slice_thickness_mm",
    )
    return HeadNormalizationSpec(
        target_plane=target_plane,
        target_in_plane_spacing_mm=in_plane_spacing,
        target_slice_thickness_mm=slice_thickness,
        write_normalized_nifti=bool(config.get("write_normalized_nifti", True)),
    )


def _positive_float(value: Any, field_name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{field_name} must be numeric") from exc
    if parsed <= 0.0:
        raise RuntimeError(f"{field_name} must be positive")
    return parsed


def _normalize_landmark_name(name: str) -> str:
    key = str(name).strip().lower().replace("-", "_").replace(" ", "_")
    return HEAD_LANDMARK_ALIASES.get(key, key)


def _landmark_vector(value: Any, field_name: str) -> np.ndarray:
    if isinstance(value, dict):
        raw = [value.get("x"), value.get("y"), value.get("z")]
    else:
        raw = value
    if not isinstance(raw, (list, tuple)) or len(raw) != 3:
        raise RuntimeError(f"{field_name} must be a 3-item RAS-mm coordinate")
    try:
        vector = np.asarray([float(raw[0]), float(raw[1]), float(raw[2])], dtype=np.float64)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{field_name} must contain numeric coordinates") from exc
    if not np.all(np.isfinite(vector)):
        raise RuntimeError(f"{field_name} must contain finite coordinates")
    return vector


def normalize_head_landmarks(raw_landmarks: Any) -> dict[str, list[float]]:
    """Normalize supported head landmark payloads to RAS-mm coordinates."""
    if not isinstance(raw_landmarks, dict):
        return {}
    normalized: dict[str, list[float]] = {}
    for raw_name, raw_value in raw_landmarks.items():
        name = _normalize_landmark_name(str(raw_name))
        if name not in HEAD_ANATOMIC_LANDMARKS:
            continue
        normalized[name] = [
            float(value)
            for value in _landmark_vector(raw_value, f"head_landmarks.{name}").tolist()
        ]
    return normalized


def _missing_anatomic_landmarks(landmarks: dict[str, list[float]]) -> list[str]:
    return [name for name in HEAD_ANATOMIC_LANDMARKS if name not in landmarks]


def _unit_vector(vector: np.ndarray, field_name: str) -> np.ndarray:
    norm = float(np.linalg.norm(vector))
    if norm <= 1e-6:
        raise RuntimeError(f"{field_name} has near-zero length")
    return np.asarray(vector, dtype=np.float64) / norm


def _image_world_corners(image: nib.Nifti1Image) -> np.ndarray:
    shape = tuple(int(value) for value in image.shape[:3])
    corners = np.asarray(
        [
            [x, y, z]
            for x in (0, max(shape[0] - 1, 0))
            for y in (0, max(shape[1] - 1, 0))
            for z in (0, max(shape[2] - 1, 0))
        ],
        dtype=np.float64,
    )
    hom = np.c_[corners, np.ones(corners.shape[0], dtype=np.float64)]
    return (np.asarray(image.affine, dtype=np.float64) @ hom.T).T[:, :3]


def _canonical_mask_world_points(
    mask_path: Path,
    *,
    max_points: int = 250_000,
) -> tuple[np.ndarray, tuple[int, int, int]]:
    mask_image = nib.as_closest_canonical(nib.load(str(mask_path)))
    mask = np.asarray(mask_image.get_fdata(), dtype=np.float32) > 0
    coords = np.argwhere(mask)
    if coords.size == 0:
        raise RuntimeError("brain mask is empty")
    if coords.shape[0] > max_points:
        stride = int(np.ceil(coords.shape[0] / max_points))
        coords = coords[::stride]
    hom = np.c_[coords.astype(np.float64), np.ones(coords.shape[0], dtype=np.float64)]
    points = (np.asarray(mask_image.affine, dtype=np.float64) @ hom.T).T[:, :3]
    return points, tuple(int(value) for value in mask.shape[:3])


def _optional_canonical_mask_world_points(
    mask_path: Path,
    *,
    max_points: int = 80_000,
) -> np.ndarray | None:
    if not mask_path.exists():
        return None
    mask_image = nib.as_closest_canonical(nib.load(str(mask_path)))
    mask = np.asarray(mask_image.get_fdata(), dtype=np.float32) > 0
    coords = np.argwhere(mask)
    if coords.shape[0] < 10:
        return None
    if coords.shape[0] > max_points:
        stride = int(np.ceil(coords.shape[0] / max_points))
        coords = coords[::stride]
    hom = np.c_[coords.astype(np.float64), np.ones(coords.shape[0], dtype=np.float64)]
    return (np.asarray(mask_image.affine, dtype=np.float64) @ hom.T).T[:, :3]


def _brain_axes_in_source_axial_plane(
    canonical_image: nib.Nifti1Image,
    brain_points: np.ndarray,
    centroid: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, float, np.ndarray]:
    """Keep the acquisition plane normal and estimate only in-plane brain axes."""
    source_x = _unit_vector(
        np.asarray(canonical_image.affine[:3, 0], dtype=np.float64),
        "source axial x axis",
    )
    source_y = _unit_vector(
        np.asarray(canonical_image.affine[:3, 1], dtype=np.float64),
        "source axial y axis",
    )
    z_axis = _unit_vector(
        np.asarray(canonical_image.affine[:3, 2], dtype=np.float64),
        "source axial plane normal",
    )
    if float(np.dot(z_axis, np.asarray([0.0, 0.0, 1.0], dtype=np.float64))) < 0:
        z_axis *= -1.0

    plane_x = _unit_vector(
        source_x - (np.dot(source_x, z_axis) * z_axis),
        "source x axis projected into axial plane",
    )
    plane_y = _unit_vector(np.cross(z_axis, plane_x), "source axial plane y axis")
    if float(np.dot(plane_y, source_y)) < 0:
        plane_x *= -1.0
        plane_y *= -1.0

    centered = brain_points - centroid
    projected = np.column_stack([centered @ plane_x, centered @ plane_y])
    covariance = np.cov(projected, rowvar=False)
    eigenvalues, eigenvectors = np.linalg.eigh(covariance)
    order = np.argsort(eigenvalues)[::-1]
    eigenvalues = eigenvalues[order]
    eigenvectors = eigenvectors[:, order]
    if float(eigenvalues[0]) <= 1e-6:
        raise RuntimeError("brain mask has insufficient in-plane variance")

    posterior_reference = -plane_y
    candidates = [
        _unit_vector(
            (plane_x * eigenvectors[0, idx]) + (plane_y * eigenvectors[1, idx]),
            f"brain in-plane PCA axis {idx}",
        )
        for idx in range(2)
    ]
    anterior_posterior_index = max(
        range(2),
        key=lambda idx: float(abs(np.dot(candidates[idx], posterior_reference))),
    )
    y_axis = candidates[anterior_posterior_index]
    if float(np.dot(y_axis, posterior_reference)) < 0:
        y_axis *= -1.0
    x_axis = _unit_vector(np.cross(y_axis, z_axis), "brain in-plane right-left axis")

    signed_rotation_radians = np.arctan2(
        float(np.dot(z_axis, np.cross(posterior_reference, y_axis))),
        float(np.dot(posterior_reference, y_axis)),
    )
    axes = np.column_stack([x_axis, y_axis, z_axis])
    return axes, eigenvalues, float(np.degrees(signed_rotation_radians)), posterior_reference


def _anatomic_frame_from_landmarks(
    landmarks: dict[str, list[float]],
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    left_orbitale = _landmark_vector(landmarks["left_orbitale"], "head_landmarks.left_orbitale")
    right_orbitale = _landmark_vector(landmarks["right_orbitale"], "head_landmarks.right_orbitale")
    left_porion = _landmark_vector(landmarks["left_porion"], "head_landmarks.left_porion")
    right_porion = _landmark_vector(landmarks["right_porion"], "head_landmarks.right_porion")

    x_axis = _unit_vector(right_orbitale - left_orbitale, "left/right orbitale vector")
    mid_orbitale = (left_orbitale + right_orbitale) / 2.0
    mid_porion = (left_porion + right_porion) / 2.0
    om_vector = mid_orbitale - mid_porion
    y_axis = om_vector - (np.dot(om_vector, x_axis) * x_axis)
    y_axis = _unit_vector(y_axis, "orbitomeatal vector projected orthogonal to left-right axis")
    z_axis = _unit_vector(np.cross(x_axis, y_axis), "orbitomeatal normal vector")
    if z_axis[2] < 0:
        y_axis = -y_axis
        z_axis = -z_axis

    axes = np.column_stack([x_axis, y_axis, z_axis])
    origin = (mid_orbitale + mid_porion) / 2.0
    frame = {
        "axis_codes": ["R", "A", "S"],
        "x_axis_source": "right_orbitale - left_orbitale",
        "y_axis_source": "mid_orbitale - mid_porion projected orthogonal to x",
        "z_axis_source": "x cross y, forced toward superior",
        "origin_source": "midpoint between mid_orbitale and mid_porion",
        "origin_ras_mm": [float(value) for value in origin.tolist()],
        "x_axis_ras": [float(value) for value in x_axis.tolist()],
        "y_axis_ras": [float(value) for value in y_axis.tolist()],
        "z_axis_ras": [float(value) for value in z_axis.tolist()],
    }
    return axes, origin, frame


def compute_mask_status(
    mask: np.ndarray | None,
    spacing_xyz: tuple[float, float, float],
) -> dict[str, Any]:
    if mask is None:
        return {
            "status": "missing",
            "present": False,
            "complete": False,
            "voxel_count": 0,
            "volume_cm3": None,
            "bounds": None,
            "touches_scan_bounds": True,
            "touched_bounds": ["missing"],
        }

    mask_bool = np.asarray(mask, dtype=bool)
    if mask_bool.ndim != 3:
        return {
            "status": "invalid_shape",
            "present": False,
            "complete": False,
            "voxel_count": 0,
            "volume_cm3": None,
            "bounds": None,
            "touches_scan_bounds": True,
            "touched_bounds": ["invalid_shape"],
        }

    voxel_count = int(np.count_nonzero(mask_bool))
    if voxel_count == 0:
        return {
            "status": "empty",
            "present": False,
            "complete": False,
            "voxel_count": 0,
            "volume_cm3": 0.0,
            "bounds": None,
            "touches_scan_bounds": False,
            "touched_bounds": [],
        }

    coords = np.argwhere(mask_bool)
    mins = coords.min(axis=0)
    maxs = coords.max(axis=0)
    touched_bounds: list[str] = []
    axis_names = ("x", "y", "z")
    for axis, axis_name in enumerate(axis_names):
        if int(mins[axis]) <= 0:
            touched_bounds.append(f"{axis_name}_min")
        if int(maxs[axis]) >= mask_bool.shape[axis] - 1:
            touched_bounds.append(f"{axis_name}_max")

    voxel_volume_cm3 = float(spacing_xyz[0] * spacing_xyz[1] * spacing_xyz[2]) / 1000.0
    touches_scan_bounds = bool(touched_bounds)
    return {
        "status": "complete" if not touches_scan_bounds else "truncated",
        "present": True,
        "complete": not touches_scan_bounds,
        "voxel_count": voxel_count,
        "volume_cm3": round(voxel_count * voxel_volume_cm3, 3),
        "bounds": {
            "x": {"start": int(mins[0]), "end": int(maxs[0])},
            "y": {"start": int(mins[1]), "end": int(maxs[1])},
            "z": {"start": int(mins[2]), "end": int(maxs[2])},
        },
        "touches_scan_bounds": touches_scan_bounds,
        "touched_bounds": touched_bounds,
    }


def collect_mask_statuses(
    mask_dir: Path,
    mask_names: tuple[str, ...] | list[str],
    spacing_xyz: tuple[float, float, float],
    *,
    reference_shape: tuple[int, int, int] | None = None,
) -> dict[str, Any]:
    statuses: dict[str, Any] = {}
    missing: list[str] = []
    invalid_geometry: list[str] = []
    empty: list[str] = []
    incomplete: list[str] = []

    for mask_name in mask_names:
        mask_path = mask_dir / f"{mask_name}.nii.gz"
        if not mask_path.exists():
            status = compute_mask_status(None, spacing_xyz)
            missing.append(mask_name)
        else:
            try:
                image = nib.load(str(mask_path))
                mask = np.asarray(image.get_fdata(), dtype=np.float32) > 0
            except Exception as exc:
                status = {
                    "status": "read_error",
                    "present": False,
                    "complete": False,
                    "voxel_count": 0,
                    "volume_cm3": None,
                    "bounds": None,
                    "touches_scan_bounds": True,
                    "touched_bounds": ["read_error"],
                    "error": str(exc),
                }
                invalid_geometry.append(mask_name)
            else:
                if reference_shape is not None and tuple(mask.shape) != tuple(reference_shape):
                    status = {
                        "status": "geometry_mismatch",
                        "present": False,
                        "complete": False,
                        "voxel_count": int(np.count_nonzero(mask)),
                        "volume_cm3": None,
                        "bounds": None,
                        "touches_scan_bounds": True,
                        "touched_bounds": ["geometry_mismatch"],
                        "shape": [int(value) for value in mask.shape],
                    }
                    invalid_geometry.append(mask_name)
                else:
                    status = compute_mask_status(mask, spacing_xyz)
                    if status["status"] == "empty":
                        empty.append(mask_name)
                    elif not status["complete"]:
                        incomplete.append(mask_name)
        statuses[mask_name] = status

    return {
        "complete": not (missing or invalid_geometry or empty or incomplete),
        "available_count": len(mask_names) - len(missing) - len(invalid_geometry),
        "expected_count": len(mask_names),
        "missing": missing,
        "invalid_geometry": invalid_geometry,
        "empty": empty,
        "incomplete": incomplete,
        "masks": statuses,
    }


def normalize_nifti_to_axial(
    source_path: Path,
    output_path: Path,
    spec: HeadNormalizationSpec,
    *,
    interpolation_order: int = 1,
) -> dict[str, Any]:
    if spec.target_plane != "axial":
        raise RuntimeError(f"Unsupported head normalization target_plane: {spec.target_plane}")

    image = nib.load(str(source_path))
    source_axis_codes = tuple(str(code) for code in aff2axcodes(image.affine))
    source_spacing = tuple(float(value) for value in image.header.get_zooms()[:3])
    source_shape = tuple(int(value) for value in image.shape[:3])

    canonical = nib.as_closest_canonical(image)
    canonical_axis_codes = tuple(str(code) for code in aff2axcodes(canonical.affine))
    canonical_spacing = tuple(float(value) for value in canonical.header.get_zooms()[:3])
    canonical_shape = tuple(int(value) for value in canonical.shape[:3])

    resampled = resample_to_output(
        canonical,
        voxel_sizes=spec.target_spacing_mm,
        order=int(interpolation_order),
    )
    normalized_shape = tuple(int(value) for value in resampled.shape[:3])
    normalized_spacing = tuple(float(value) for value in resampled.header.get_zooms()[:3])

    artifact_path = None
    if spec.write_normalized_nifti:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        nib.save(resampled, str(output_path))
        artifact_path = output_path

    return {
        "target_plane": spec.target_plane,
        "target_spacing_mm": {
            "x": spec.target_spacing_mm[0],
            "y": spec.target_spacing_mm[1],
            "z": spec.target_spacing_mm[2],
        },
        "source_axis_codes": list(source_axis_codes),
        "source_spacing_mm": {
            "x": source_spacing[0],
            "y": source_spacing[1],
            "z": source_spacing[2],
        },
        "source_shape": [int(value) for value in source_shape],
        "canonical_axis_codes": list(canonical_axis_codes),
        "canonical_spacing_mm": {
            "x": canonical_spacing[0],
            "y": canonical_spacing[1],
            "z": canonical_spacing[2],
        },
        "canonical_shape": [int(value) for value in canonical_shape],
        "normalized_spacing_mm": {
            "x": normalized_spacing[0],
            "y": normalized_spacing[1],
            "z": normalized_spacing[2],
        },
        "normalized_shape": [int(value) for value in normalized_shape],
        "normalized_nifti": str(artifact_path) if artifact_path else None,
    }


def normalize_nifti_to_ras_isotropic(
    source_path: Path,
    output_path: Path,
    *,
    voxel_size_mm: float = 2.0,
    write_normalized_nifti: bool = True,
    interpolation_order: int = 1,
) -> dict[str, Any]:
    """Create a canonical RAS isotropic head CT volume.

    This is geometric canonicalization only. Anatomical alignment to the
    orbitomeatal line or midline requires explicit landmarks or a validated
    landmark detector, so the returned payload reports that status separately.
    """

    voxel_size = _positive_float(voxel_size_mm, "voxel_size_mm")
    image = nib.load(str(source_path))
    source_axis_codes = tuple(str(code) for code in aff2axcodes(image.affine))
    source_spacing = tuple(float(value) for value in image.header.get_zooms()[:3])
    source_shape = tuple(int(value) for value in image.shape[:3])

    canonical = nib.as_closest_canonical(image)
    canonical_axis_codes = tuple(str(code) for code in aff2axcodes(canonical.affine))
    canonical_spacing = tuple(float(value) for value in canonical.header.get_zooms()[:3])
    canonical_shape = tuple(int(value) for value in canonical.shape[:3])
    resampled = resample_to_output(
        canonical,
        voxel_sizes=(voxel_size, voxel_size, voxel_size),
        order=int(interpolation_order),
    )

    artifact_path = None
    if write_normalized_nifti:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        nib.save(resampled, str(output_path))
        artifact_path = output_path

    return {
        "target_orientation": "RAS",
        "target_spacing_mm": {"x": voxel_size, "y": voxel_size, "z": voxel_size},
        "source_axis_codes": list(source_axis_codes),
        "source_spacing_mm": {
            "x": source_spacing[0],
            "y": source_spacing[1],
            "z": source_spacing[2],
        },
        "source_shape": [int(value) for value in source_shape],
        "canonical_axis_codes": list(canonical_axis_codes),
        "canonical_spacing_mm": {
            "x": canonical_spacing[0],
            "y": canonical_spacing[1],
            "z": canonical_spacing[2],
        },
        "canonical_shape": [int(value) for value in canonical_shape],
        "normalized_spacing_mm": {
            "x": float(resampled.header.get_zooms()[0]),
            "y": float(resampled.header.get_zooms()[1]),
            "z": float(resampled.header.get_zooms()[2]),
        },
        "normalized_shape": [int(value) for value in resampled.shape[:3]],
        "normalized_nifti": str(artifact_path) if artifact_path else None,
        "anatomic_alignment": {
            "status": "landmarks_required",
            "orbitomeatal_line_perpendicular": False,
            "midline_perpendicular": False,
            "reason": (
                "No validated orbitomeatal-line or midline landmarks were "
                "available for deterministic anatomic reformatting."
            ),
        },
    }


def normalize_nifti_to_orbitomeatal_isotropic(
    source_path: Path,
    output_path: Path,
    *,
    landmarks: dict[str, Any] | None,
    voxel_size_mm: float = 2.0,
    write_normalized_nifti: bool = True,
    interpolation_order: int = 1,
) -> dict[str, Any]:
    """Create a 2 mm anatomic head CT volume from explicit RAS-mm landmarks.

    Required landmarks are left/right orbitale and left/right porion. The output
    frame makes the left-right axis horizontal, the orbitomeatal line the
    anterior-posterior axis, and the resulting normal the superior axis.
    """

    voxel_size = _positive_float(voxel_size_mm, "voxel_size_mm")
    normalized_landmarks = normalize_head_landmarks(landmarks or {})
    missing = _missing_anatomic_landmarks(normalized_landmarks)
    if missing:
        return {
            "target_orientation": "orbitomeatal_ras",
            "target_spacing_mm": {"x": voxel_size, "y": voxel_size, "z": voxel_size},
            "normalized_nifti": None,
            "anatomic_alignment": {
                "status": "landmarks_required",
                "orbitomeatal_line_perpendicular": False,
                "midline_perpendicular": False,
                "required_landmarks": list(HEAD_ANATOMIC_LANDMARKS),
                "missing_landmarks": missing,
                "reason": (
                    "Explicit RAS-mm orbitale and porion landmarks are required "
                    "for deterministic orbitomeatal and midline reformatting."
                ),
            },
        }

    image = nib.load(str(source_path))
    canonical = nib.as_closest_canonical(image)
    source_axis_codes = tuple(str(code) for code in aff2axcodes(image.affine))
    source_spacing = tuple(float(value) for value in image.header.get_zooms()[:3])
    source_shape = tuple(int(value) for value in image.shape[:3])

    axes, frame_origin, frame = _anatomic_frame_from_landmarks(normalized_landmarks)
    world_corners = _image_world_corners(canonical)
    target_coords = (world_corners - frame_origin) @ axes
    min_coords = np.floor(target_coords.min(axis=0) / voxel_size) * voxel_size
    max_coords = np.ceil(target_coords.max(axis=0) / voxel_size) * voxel_size
    output_shape = tuple(int(max(1, np.ceil((max_coords[axis] - min_coords[axis]) / voxel_size) + 1)) for axis in range(3))
    output_origin = frame_origin + axes @ min_coords
    output_affine = np.eye(4, dtype=np.float64)
    output_affine[:3, 0] = axes[:, 0] * voxel_size
    output_affine[:3, 1] = axes[:, 1] * voxel_size
    output_affine[:3, 2] = axes[:, 2] * voxel_size
    output_affine[:3, 3] = output_origin

    resampled = resample_from_to(
        canonical,
        (output_shape, output_affine),
        order=int(interpolation_order),
    )

    artifact_path = None
    if write_normalized_nifti:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        nib.save(resampled, str(output_path))
        artifact_path = output_path

    return {
        "target_orientation": "orbitomeatal_ras",
        "target_spacing_mm": {"x": voxel_size, "y": voxel_size, "z": voxel_size},
        "source_axis_codes": list(source_axis_codes),
        "source_spacing_mm": {
            "x": source_spacing[0],
            "y": source_spacing[1],
            "z": source_spacing[2],
        },
        "source_shape": [int(value) for value in source_shape],
        "normalized_spacing_mm": {
            "x": float(resampled.header.get_zooms()[0]),
            "y": float(resampled.header.get_zooms()[1]),
            "z": float(resampled.header.get_zooms()[2]),
        },
        "normalized_shape": [int(value) for value in resampled.shape[:3]],
        "normalized_nifti": str(artifact_path) if artifact_path else None,
        "landmarks": normalized_landmarks,
        "anatomic_frame": frame,
        "anatomic_alignment": {
            "status": "complete",
            "orbitomeatal_line_perpendicular": True,
            "midline_perpendicular": True,
            "required_landmarks": list(HEAD_ANATOMIC_LANDMARKS),
            "missing_landmarks": [],
            "reason": (
                "Output was resampled from explicit orbitale and porion "
                "RAS-mm landmarks."
            ),
        },
    }


def normalize_nifti_to_brain_mask_geometry_isotropic(
    source_path: Path,
    brain_mask_path: Path,
    output_path: Path,
    *,
    crop_mask_path: Path | None = None,
    crop_margin_mm: float = 25.0,
    voxel_size_mm: float = 2.0,
    in_plane_spacing_mm: tuple[float, float] | None = None,
    write_normalized_nifti: bool = True,
    interpolation_order: int = 3,
) -> dict[str, Any]:
    """Create a brain-aligned head CT while preserving the acquisition plane."""

    slice_spacing = _positive_float(voxel_size_mm, "voxel_size_mm")
    if not brain_mask_path.exists():
        return {
            "target_orientation": "brain_mask_source_axial_plane_pca",
            "target_spacing_mm": {"x": None, "y": None, "z": slice_spacing},
            "normalized_nifti": None,
            "anatomic_alignment": {
                "status": "brain_mask_required",
                "midline_perpendicular": False,
                "orbitomeatal_line_perpendicular": False,
                "reason": "The total/brain.nii.gz mask is required for mask-based head geometry normalization.",
            },
        }

    image = nib.load(str(source_path))
    canonical = nib.as_closest_canonical(image)
    source_axis_codes = tuple(str(code) for code in aff2axcodes(image.affine))
    source_spacing = tuple(float(value) for value in image.header.get_zooms()[:3])
    source_shape = tuple(int(value) for value in image.shape[:3])
    if in_plane_spacing_mm is None:
        target_spacing = (source_spacing[0], source_spacing[1], slice_spacing)
    else:
        target_spacing = (
            _positive_float(in_plane_spacing_mm[0], "in_plane_spacing_mm[0]"),
            _positive_float(in_plane_spacing_mm[1], "in_plane_spacing_mm[1]"),
            slice_spacing,
        )

    brain_points, brain_mask_shape = _canonical_mask_world_points(brain_mask_path)
    centroid = brain_points.mean(axis=0)
    axes, eigenvalues, in_plane_rotation_degrees, posterior_reference = (
        _brain_axes_in_source_axial_plane(canonical, brain_points, centroid)
    )

    crop_points = None
    crop_source = "source_volume_corners"
    if crop_mask_path is not None:
        crop_points = _optional_canonical_mask_world_points(Path(crop_mask_path), max_points=250_000)
        if crop_points is not None:
            crop_source = str(crop_mask_path)
    if crop_points is None:
        crop_points = brain_points
        crop_source = str(brain_mask_path)

    target_coords = (crop_points - centroid) @ axes
    spacing_vector = np.asarray(target_spacing, dtype=np.float64)
    margin = _positive_float(crop_margin_mm, "crop_margin_mm")
    target_coords_min = target_coords.min(axis=0) - margin
    target_coords_max = target_coords.max(axis=0) + margin
    min_coords = np.floor(target_coords_min / spacing_vector) * spacing_vector
    max_coords = np.ceil(target_coords_max / spacing_vector) * spacing_vector
    output_shape = tuple(
        int(max(1, np.ceil((max_coords[axis] - min_coords[axis]) / spacing_vector[axis]) + 1))
        for axis in range(3)
    )
    output_origin = centroid + axes @ min_coords
    output_affine = np.eye(4, dtype=np.float64)
    output_affine[:3, 0] = axes[:, 0] * target_spacing[0]
    output_affine[:3, 1] = axes[:, 1] * target_spacing[1]
    output_affine[:3, 2] = axes[:, 2] * target_spacing[2]
    output_affine[:3, 3] = output_origin

    resampled = resample_from_to(
        canonical,
        (output_shape, output_affine),
        order=int(interpolation_order),
    )

    artifact_path = None
    if write_normalized_nifti:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        nib.save(resampled, str(output_path))
        artifact_path = output_path

    axis_labels = ["right_left", "anterior_posterior", "superior_inferior"]
    return {
        "target_orientation": "brain_mask_source_axial_plane_pca",
        "target_spacing_mm": {
            "x": target_spacing[0],
            "y": target_spacing[1],
            "z": target_spacing[2],
        },
        "source_axis_codes": list(source_axis_codes),
        "source_spacing_mm": {
            "x": source_spacing[0],
            "y": source_spacing[1],
            "z": source_spacing[2],
        },
        "source_shape": [int(value) for value in source_shape],
        "brain_mask": str(brain_mask_path),
        "brain_mask_shape": [int(value) for value in brain_mask_shape],
        "brain_mask_sampled_points": int(brain_points.shape[0]),
        "normalized_spacing_mm": {
            "x": float(resampled.header.get_zooms()[0]),
            "y": float(resampled.header.get_zooms()[1]),
            "z": float(resampled.header.get_zooms()[2]),
        },
        "normalized_shape": [int(value) for value in resampled.shape[:3]],
        "normalized_nifti": str(artifact_path) if artifact_path else None,
        "brain_geometry_frame": {
            "centroid_ras_mm": [float(value) for value in centroid.tolist()],
            "axes": {
                axis_labels[idx]: [float(value) for value in axes[:, idx].tolist()]
                for idx in range(3)
            },
            "source_axial_normal_ras": [float(value) for value in axes[:, 2].tolist()],
            "source_posterior_reference_ras": [
                float(value) for value in posterior_reference.tolist()
            ],
            "in_plane_rotation_degrees": in_plane_rotation_degrees,
            "crop_source": crop_source,
            "crop_margin_mm": margin,
            "crop_sampled_points": int(crop_points.shape[0]),
            "in_plane_pca_eigenvalues": [float(value) for value in eigenvalues.tolist()],
            "method": (
                "Source acquisition axial-plane normal preserved; total/brain mask world "
                "coordinates projected into that plane; 2D PCA assigned to the nearest source "
                "right-left and anterior-posterior axes; posterior image-row direction forced "
                "for conventional axial DICOM display"
            ),
        },
        "anatomic_alignment": {
            "status": "brain_mask_in_plane",
            "midline_perpendicular": True,
            "orbitomeatal_line_perpendicular": False,
            "orbitomeatal_line_required": False,
            "reason": (
                "The source acquisition axial-plane normal was preserved and only the in-plane "
                "right-left/anterior-posterior rotation was estimated from total/brain. The "
                "orbitomeatal line and brain-structure masks were not used."
            ),
        },
    }
