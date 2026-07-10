"""Localized text helpers for the parenchymal-organ overlay series."""

from __future__ import annotations

from dataclasses import dataclass

from heimdallr.shared import settings
from heimdallr.shared.i18n import format_integer, normalize_locale, translate


VOLUME_ALERT_THRESHOLDS = {
    "liver": ("greater_than", 1800.0),
    "spleen": ("greater_than", 400.0),
    "kidney_right": ("less_than", 100.0),
    "kidney_left": ("less_than", 100.0),
}


@dataclass(frozen=True)
class OverlayTextLine:
    """One overlay line with an optional character span rendered as an alert."""

    text: str
    alert_span: tuple[int, int] | None = None


def resolve_artifact_locale(job_config: dict) -> str:
    """Resolve artifact locale from job config with environment fallback."""
    return normalize_locale(job_config.get("locale") or settings.ARTIFACTS_LOCALE)


def build_overlay_text(
    *,
    organ_measurements: dict[str, dict],
    locale: str,
    hepatic_steatosis: dict | None = None,
) -> list[str]:
    """Build localized summary lines for the parenchymal organ overlay."""
    return [
        line.text
        for line in build_overlay_lines(
            organ_measurements=organ_measurements,
            locale=locale,
            hepatic_steatosis=hepatic_steatosis,
        )
    ]


def _volume_is_alert(organ_key: str, volume_cm3: float) -> bool:
    threshold = VOLUME_ALERT_THRESHOLDS.get(organ_key)
    if threshold is None:
        return False
    comparison, boundary = threshold
    if comparison == "greater_than":
        return float(volume_cm3) > boundary
    return float(volume_cm3) < boundary


def _steatosis_line(assessment: dict | None, *, locale: str) -> OverlayTextLine | None:
    if not assessment:
        return None
    status = str(assessment.get("status") or "")
    if status == "kvp_out_of_range":
        message_id = "parenchymal.overlay.steatosis.kvp_out_of_range"
        kwargs = {}
    elif status == "normal":
        message_id = "parenchymal.overlay.steatosis.normal"
        kwargs = {}
    elif status == "estimated" and assessment.get("estimated_percent") is not None:
        message_id = "parenchymal.overlay.steatosis.estimated"
        kwargs = {"percent": format_integer(assessment["estimated_percent"], locale=locale)}
    else:
        return None
    return OverlayTextLine(translate(message_id, locale=locale, **kwargs))


def build_overlay_lines(
    *,
    organ_measurements: dict[str, dict],
    locale: str,
    hepatic_steatosis: dict | None = None,
) -> list[OverlayTextLine]:
    """Build localized overlay lines and preserve volume alert spans."""
    summary_lines = [OverlayTextLine(translate("parenchymal.overlay.title", locale=locale))]
    for organ_key in ("liver", "spleen", "pancreas", "kidney_right", "kidney_left"):
        measurement = organ_measurements.get(organ_key) or {}
        status = str(measurement.get("analysis_status", "") or "")
        if status == "missing":
            continue

        volume_cm3 = measurement.get("volume_cm3")
        if volume_cm3 is None and status == "complete":
            volume_cm3 = measurement.get("observed_volume_cm3")
        hu_mean = measurement.get("hu_mean")
        if volume_cm3 is None:
            continue

        if hu_mean is None:
            volume_text = format_integer(volume_cm3, locale=locale)
            rendered = translate(
                f"parenchymal.overlay.organ.{organ_key}.volume_only",
                locale=locale,
                volume=volume_text,
            )
            alert_start = rendered.find(volume_text) if _volume_is_alert(organ_key, volume_cm3) else -1
            summary_lines.append(
                OverlayTextLine(
                    rendered,
                    (alert_start, alert_start + len(volume_text)) if alert_start >= 0 else None,
                )
            )
        else:
            volume_text = format_integer(volume_cm3, locale=locale)
            rendered = translate(
                f"parenchymal.overlay.organ.{organ_key}",
                locale=locale,
                volume=volume_text,
                hu=format_integer(hu_mean, locale=locale),
                attenuation_unit=translate("parenchymal.overlay.attenuation_unit", locale=locale),
            )
            alert_start = rendered.find(volume_text) if _volume_is_alert(organ_key, volume_cm3) else -1
            summary_lines.append(
                OverlayTextLine(
                    rendered,
                    (alert_start, alert_start + len(volume_text)) if alert_start >= 0 else None,
                )
            )

        if organ_key == "liver":
            steatosis_line = _steatosis_line(hepatic_steatosis, locale=locale)
            if steatosis_line is not None:
                summary_lines.append(steatosis_line)
    return summary_lines


def series_description(locale: str) -> str:
    """Return the localized DICOM SeriesDescription."""
    return translate("parenchymal.overlay.series_description", locale=locale)


def derivation_description(locale: str) -> str:
    """Return the localized DICOM DerivationDescription."""
    return translate("parenchymal.overlay.derivation_description", locale=locale)
