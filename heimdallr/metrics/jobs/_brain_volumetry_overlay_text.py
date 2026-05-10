"""Localized text helpers for the brain-volumetry overlay series."""

from __future__ import annotations

from heimdallr.shared import settings
from heimdallr.shared.i18n import format_integer, normalize_locale, translate


def resolve_artifact_locale(job_config: dict) -> str:
    """Resolve artifact locale from job config with environment fallback."""
    return normalize_locale(job_config.get("locale") or settings.ARTIFACTS_LOCALE)


def build_overlay_text(*, measurement: dict, locale: str) -> list[str]:
    """Build localized summary lines for the brain-volumetry overlay."""
    summary_lines = [translate("brain_volumetry.overlay.title", locale=locale)]
    volume_cm3 = measurement.get("volume_cm3")
    if volume_cm3 is None and measurement.get("analysis_status") == "complete":
        volume_cm3 = measurement.get("observed_volume_cm3")
    if volume_cm3 is not None:
        summary_lines.append(
            translate(
                "brain_volumetry.overlay.brain",
                locale=locale,
                volume=format_integer(volume_cm3, locale=locale),
            )
        )
    elif measurement.get("analysis_status") == "incomplete":
        summary_lines.append(
            translate(
                "brain_volumetry.overlay.incomplete",
                locale=locale,
            )
        )
    return summary_lines


def series_description(locale: str) -> str:
    """Return the localized DICOM SeriesDescription."""
    return translate("brain_volumetry.overlay.series_description", locale=locale)


def derivation_description(locale: str) -> str:
    """Return the localized DICOM DerivationDescription."""
    return translate("brain_volumetry.overlay.derivation_description", locale=locale)
