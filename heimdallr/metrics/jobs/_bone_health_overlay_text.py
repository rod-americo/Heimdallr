"""Localized text helpers for the bone-health L1 overlay."""

from __future__ import annotations

from heimdallr.shared import settings
from heimdallr.shared.i18n import format_decimal, format_integer, normalize_locale, translate


def resolve_artifact_locale(job_config: dict) -> str:
    """Resolve artifact locale from job config with environment fallback."""
    return normalize_locale(job_config.get("locale") or settings.ARTIFACTS_LOCALE)


def build_overlay_text(
    *,
    hu_mean: float | None,
    hu_std: float | None,
    roi_voxels: int,
    roi_radius_mm: float,
    classification: str,
    locale: str,
) -> tuple[str, list[str]]:
    """Build localized title and summary lines for the L1 bone-health overlay."""
    title = translate(
        "bone_health.overlay.title",
        locale=locale,
        classification=translate(f"bone_health.overlay.classification.{classification}", locale=locale),
    )
    summary_lines = [
        translate(
            "bone_health.overlay.hu_mean",
            locale=locale,
            value=format_decimal(float(hu_mean), 2, locale=locale) if hu_mean is not None else "-",
        ),
        translate(
            "bone_health.overlay.hu_std",
            locale=locale,
            value=format_decimal(float(hu_std), 2, locale=locale) if hu_std is not None else "-",
        ),
        translate(
            "bone_health.overlay.roi_voxels",
            locale=locale,
            value=format_integer(roi_voxels, locale=locale),
        ),
        translate(
            "bone_health.overlay.roi_radius",
            locale=locale,
            value=format_decimal(float(roi_radius_mm), 2, locale=locale),
        ),
    ]
    return title, summary_lines


def series_description(locale: str) -> str:
    """Return the localized DICOM SeriesDescription."""
    return translate("bone_health.overlay.series_description", locale=locale)


def derivation_description(locale: str, *, hu_mean: float | None, classification: str) -> str:
    """Return the localized DICOM DerivationDescription."""
    return translate(
        "bone_health.overlay.derivation_description",
        locale=locale,
        hu_mean=format_decimal(float(hu_mean), 2, locale=locale) if hu_mean is not None else "-",
        classification=translate(f"bone_health.overlay.classification.{classification}", locale=locale),
    )
