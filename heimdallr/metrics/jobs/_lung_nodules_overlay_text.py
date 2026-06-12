"""Localized text helpers for the pulmonary nodule overlay."""

from __future__ import annotations

from heimdallr.shared import settings
from heimdallr.shared.i18n import format_decimal, normalize_locale, translate


def resolve_artifact_locale(job_config: dict) -> str:
    """Resolve artifact locale from job config with environment fallback."""
    return normalize_locale(job_config.get("locale") or settings.ARTIFACTS_LOCALE)


def overlay_title(locale: str) -> str:
    """Return the localized burned-in overlay title."""
    return translate("lung_nodules.overlay.title", locale=locale)


def build_component_overlay_text(
    *,
    component_id: int,
    component_index: int,
    component_count: int,
    slice_idx: int,
    probable_viewer_slice_index_one_based: int,
    voxel_count: int,
    volume_cm3: float,
    locale: str,
) -> tuple[str, list[str]]:
    """Build localized title and summary lines for a nodule component overlay."""
    title = translate(
        "lung_nodules.overlay.component_title",
        locale=locale,
        component=component_index,
        total=component_count,
    )
    summary_lines = [
        translate("lung_nodules.overlay.component_id", locale=locale, value=component_id),
        translate("lung_nodules.overlay.nifti_slice", locale=locale, value=slice_idx),
        translate(
            "lung_nodules.overlay.viewer_slice",
            locale=locale,
            value=probable_viewer_slice_index_one_based,
        ),
        translate("lung_nodules.overlay.voxel_count", locale=locale, value=voxel_count),
        translate(
            "lung_nodules.overlay.volume_cm3",
            locale=locale,
            value=format_decimal(volume_cm3, 3, locale=locale),
        ),
    ]
    return title, summary_lines


def series_description(locale: str) -> str:
    """Return the localized DICOM SeriesDescription."""
    return translate("lung_nodules.overlay.series_description", locale=locale)


def derivation_description(locale: str) -> str:
    """Return the localized DICOM DerivationDescription."""
    return translate("lung_nodules.overlay.derivation_description", locale=locale)
