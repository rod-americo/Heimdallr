"""Localized text helpers for the parenchymal-organ overlay series."""

from __future__ import annotations

from heimdallr.shared import settings
from heimdallr.shared.i18n import format_integer, normalize_locale, translate


def resolve_artifact_locale(job_config: dict) -> str:
    """Resolve artifact locale from job config with environment fallback."""
    return normalize_locale(job_config.get("locale") or settings.ARTIFACTS_LOCALE)


def build_overlay_text(
    *,
    organ_measurements: dict[str, dict],
    locale: str,
) -> list[str]:
    """Build localized summary lines for the parenchymal organ overlay."""
    summary_lines = [translate("parenchymal.overlay.title", locale=locale)]
    for organ_key in ("liver", "spleen", "pancreas", "kidney_right", "kidney_left"):
        measurement = organ_measurements.get(organ_key) or {}
        status = str(measurement.get("analysis_status", "") or "")
        if status == "missing":
            continue

        volume_cm3 = measurement.get("volume_cm3")
        if volume_cm3 is None and status == "complete":
            volume_cm3 = measurement.get("observed_volume_cm3")
        hu_mean = measurement.get("hu_mean")
        if volume_cm3 is None or hu_mean is None:
            continue

        summary_lines.append(
            translate(
                f"parenchymal.overlay.organ.{organ_key}",
                locale=locale,
                volume=format_integer(volume_cm3, locale=locale),
                hu=format_integer(hu_mean, locale=locale),
                attenuation_unit=translate("parenchymal.overlay.attenuation_unit", locale=locale),
            )
        )
    return summary_lines


def series_description(locale: str) -> str:
    """Return the localized DICOM SeriesDescription."""
    return translate("parenchymal.overlay.series_description", locale=locale)


def derivation_description(locale: str) -> str:
    """Return the localized DICOM DerivationDescription."""
    return translate("parenchymal.overlay.derivation_description", locale=locale)
