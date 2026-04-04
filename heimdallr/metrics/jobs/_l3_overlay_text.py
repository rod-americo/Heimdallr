"""Localized text helpers for the L3 muscle-area overlay."""

from __future__ import annotations

from heimdallr.shared import settings
from heimdallr.shared.i18n import format_decimal, normalize_locale, translate


def resolve_artifact_locale(job_config: dict) -> str:
    """Resolve artifact locale from job config with environment fallback."""
    return normalize_locale(job_config.get("locale") or settings.ARTIFACTS_LOCALE)


def build_overlay_text(
    *,
    slice_idx: int,
    probable_viewer_slice_index_one_based: int,
    muscle_area_cm2: float,
    height_m: float | None,
    smi_cm2_m2: float | None,
    locale: str,
) -> tuple[str, list[str]]:
    """Build localized title and summary lines for the L3 overlay."""
    title = translate("l3.overlay.title", locale=locale)
    summary_lines = [
        translate(
            "l3.overlay.sma",
            locale=locale,
            value=format_decimal(muscle_area_cm2, 1, locale=locale),
        ),
        translate("l3.overlay.nifti_slice", locale=locale, value=slice_idx),
        translate("l3.overlay.viewer_slice", locale=locale, value=probable_viewer_slice_index_one_based),
    ]
    if height_m is not None:
        summary_lines.append(
            translate(
                "l3.overlay.height",
                locale=locale,
                value=format_decimal(height_m, 2, locale=locale),
            )
        )
    if smi_cm2_m2 is not None:
        summary_lines.append(
            translate(
                "l3.overlay.smi",
                locale=locale,
                value=format_decimal(smi_cm2_m2, 1, locale=locale),
            )
        )
    return title, summary_lines


def build_overlay_panel_titles(*, locale: str) -> tuple[str, str]:
    """Build localized panel titles for the L3 overlay."""
    return (
        translate("l3.overlay.axial_title", locale=locale),
        translate("l3.overlay.sagittal_title", locale=locale),
    )
