"""Overlay text helpers for experimental hepatic-segment overlays."""

from __future__ import annotations

from heimdallr.shared import settings
from heimdallr.shared.i18n import format_integer, normalize_locale, translate


def resolve_artifact_locale(job_config: dict) -> str:
    """Resolve artifact locale from job config with environment fallback."""
    return normalize_locale(job_config.get("locale") or settings.ARTIFACTS_LOCALE)


def build_overlay_text(
    *,
    segment_measurements: dict[str, dict],
    locale: str,
) -> list[str]:
    """Build compact summary lines for hepatic-segment overlays."""
    lines = [translate("liver_segments.overlay.title", locale=locale)]
    for segment_key in (
        "liver_segment_1",
        "liver_segment_2",
        "liver_segment_3",
        "liver_segment_4",
        "liver_segment_5",
        "liver_segment_6",
        "liver_segment_7",
        "liver_segment_8",
    ):
        measurement = segment_measurements.get(segment_key) or {}
        volume_cm3 = measurement.get("volume_cm3")
        if volume_cm3 is None:
            continue
        lines.append(
            translate(
                f"liver_segments.overlay.{segment_key}",
                locale=locale,
                volume=format_integer(volume_cm3, locale=locale),
            )
        )
    return lines
