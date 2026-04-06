"""Localized text helpers for the vertebral fracture overlay."""

from __future__ import annotations

from collections.abc import Sequence

from heimdallr.shared import settings
from heimdallr.shared.i18n import format_decimal, normalize_locale, translate


def resolve_artifact_locale(job_config: dict) -> str:
    """Resolve artifact locale from job config with environment fallback."""
    return normalize_locale(job_config.get("locale") or settings.ARTIFACTS_LOCALE)


def _translate_value(prefix: str, value: str | None, *, locale: str) -> str:
    key = str(value or "").strip().lower() or "none"
    return translate(f"{prefix}.{key}", locale=locale)


def translate_status(value: str | None, *, locale: str) -> str:
    return _translate_value("vertebral_fracture.overlay.status_value", value, locale=locale)


def translate_label(value: str | None, *, locale: str) -> str:
    return _translate_value("vertebral_fracture.overlay.label_value", value, locale=locale)


def translate_pattern(value: str | None, *, locale: str) -> str:
    return _translate_value("vertebral_fracture.overlay.pattern_value", value, locale=locale)


def translate_severity(value: str | None, *, locale: str) -> str:
    return _translate_value("vertebral_fracture.overlay.severity_value", value, locale=locale)


def build_panel_lines(summary: dict, *, locale: str) -> list[str]:
    """Build localized summary lines for a vertebra panel."""
    lines = [
        translate(
            "vertebral_fracture.overlay.panel.status",
            locale=locale,
            value=translate_status(summary.get("status"), locale=locale),
        ),
        translate(
            "vertebral_fracture.overlay.panel.label",
            locale=locale,
            value=translate_label(summary.get("screen_label"), locale=locale),
        ),
        translate(
            "vertebral_fracture.overlay.panel.pattern",
            locale=locale,
            value=translate_pattern(summary.get("suspected_pattern"), locale=locale),
        ),
        translate(
            "vertebral_fracture.overlay.panel.severity",
            locale=locale,
            value=translate_severity(summary.get("severity"), locale=locale),
        ),
    ]

    ratios = summary.get("ratios", {}) if isinstance(summary.get("ratios"), dict) else {}
    height_loss_percent = ratios.get("height_loss_ratio_percent")
    if height_loss_percent is not None:
        lines.insert(
            2,
            translate(
                "vertebral_fracture.overlay.panel.height_loss",
                locale=locale,
                value=format_decimal(float(height_loss_percent), 1, locale=locale),
            ),
        )

    return lines


def build_panel_title(vertebra: str, *, locale: str) -> str:
    """Build a localized panel title."""
    return translate("vertebral_fracture.overlay.panel.title", locale=locale, vertebra=vertebra)


def build_pathology_label(vertebra: str, summary: dict, *, locale: str) -> str:
    """Build a concise per-vertebra annotation label for the sagittal overlay."""
    ratios = summary.get("ratios", {}) if isinstance(summary.get("ratios"), dict) else {}
    height_loss_percent = ratios.get("height_loss_ratio_percent")
    if height_loss_percent is not None:
        height_loss_text = format_decimal(float(height_loss_percent), 1, locale=locale)
        return translate(
            "vertebral_fracture.overlay.annotation",
            locale=locale,
            vertebra=vertebra,
            grade=translate_label(summary.get("screen_label"), locale=locale),
            value=height_loss_text,
        )
    return translate(
        "vertebral_fracture.overlay.annotation_no_loss",
        locale=locale,
        vertebra=vertebra,
        grade=translate_label(summary.get("screen_label"), locale=locale),
    )


def build_overlay_title(*, locale: str) -> str:
    """Return the localized figure title."""
    return translate("vertebral_fracture.overlay.title", locale=locale)


def series_description(locale: str) -> str:
    """Return the localized DICOM SeriesDescription."""
    return translate("vertebral_fracture.overlay.series_description", locale=locale)


def derivation_description(locale: str, *, vertebrae: Sequence[str]) -> str:
    """Return the localized DICOM DerivationDescription."""
    vertebrae_text = ", ".join(str(item) for item in vertebrae if str(item).strip())
    return translate(
        "vertebral_fracture.overlay.derivation_description",
        locale=locale,
        vertebrae=vertebrae_text or "-",
    )
