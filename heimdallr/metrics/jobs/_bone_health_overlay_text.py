"""Localized text helpers for the bone-health L1 overlay."""

from __future__ import annotations

from typing import TypedDict

from heimdallr.shared import settings
from heimdallr.shared.i18n import format_integer, normalize_locale, translate


class OverlayLine(TypedDict, total=False):
    text: str
    color: str


def resolve_artifact_locale(job_config: dict) -> str:
    """Resolve artifact locale from job config with environment fallback."""
    return normalize_locale(job_config.get("locale") or settings.ARTIFACTS_LOCALE)


def hu_mean_color(hu_mean: float | None) -> str:
    """Return the overlay color for the mean HU value based on the configured threshold bands."""
    if hu_mean is None:
        return "white"
    if hu_mean > 160:
        return "white"
    if hu_mean >= 110:
        return "#ffd166"
    return "#ef4444"


def build_overlay_text(
    *,
    hu_mean: float | None,
    hu_std: float | None,
    locale: str,
) -> tuple[str, list[OverlayLine]]:
    """Build localized title and summary lines for the L1 bone-health overlay."""
    title = translate("bone_health.overlay.title", locale=locale)
    mean_value = format_integer(float(hu_mean), locale=locale) if hu_mean is not None else "-"
    summary_lines: list[OverlayLine] = [
        {
            "text": translate(
                "bone_health.overlay.hu_mean",
                locale=locale,
                value=mean_value,
            ),
            "color": hu_mean_color(hu_mean),
        },
    ]
    return title, summary_lines


def series_description(locale: str) -> str:
    """Return the localized DICOM SeriesDescription."""
    return translate("bone_health.overlay.series_description", locale=locale)


def derivation_description(locale: str, *, hu_mean: float | None) -> str:
    """Return the localized DICOM DerivationDescription."""
    return translate(
        "bone_health.overlay.derivation_description",
        locale=locale,
        hu_mean=format_integer(float(hu_mean), locale=locale) if hu_mean is not None else "-",
    )
